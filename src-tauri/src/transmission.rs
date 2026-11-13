use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::persistence::DatabaseManager;
use crate::types::{
    CheckpointRecord, CoreRequest, CoreResponse, SidecarCommand, SidecarEvent,
};

/// [T-201] 비행 중(In-Flight) 요청 추적기 - 중복 제출 방지
#[derive(Clone, Default)]
pub struct InFlightTracker {
    /// 현재 실행 중인 request_id 집합
    active_requests: Arc<Mutex<HashSet<String>>>,
    /// 세션별 활성 작업 키 (session_id -> active request_id)
    active_sessions: Arc<Mutex<HashMap<String, String>>>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// [T-201] 요청 실행 등록 시도 (이미 실행 중이면 Err(CoreResponse) 반환)
    pub fn try_acquire(&self, request: &CoreRequest) -> Result<(), CoreResponse> {
        let mut req_set = self.active_requests.lock().unwrap();
        let mut sess_map = self.active_sessions.lock().unwrap();

        // 1. 동일 request_id 중복 검사
        if req_set.contains(&request.request_id) {
            return Err(CoreResponse::rejected(
                Some(request.request_id.clone()),
                "T-201: 동일한 요청 식별자(request_id)의 작업이 이미 진행 중입니다.",
            ));
        }

        // 2. 동일 세션 내 동시 중복 제출 검사
        if let Some(existing_req_id) = sess_map.get(&request.session_id) {
            return Err(CoreResponse::rejected(
                Some(request.request_id.clone()),
                format!(
                    "T-201: 해당 세션({})에서 이미 활성 요청({})이 실행 중입니다. 중복 제출이 제한됩니다.",
                    request.session_id, existing_req_id
                ),
            ));
        }

        req_set.insert(request.request_id.clone());
        sess_map.insert(request.session_id.clone(), request.request_id.clone());
        Ok(())
    }

    /// 요청 종료 시 추적기에서 해제
    pub fn release(&self, request_id: &str, session_id: &str) {
        let mut req_set = self.active_requests.lock().unwrap();
        let mut sess_map = self.active_sessions.lock().unwrap();

        req_set.remove(request_id);
        if let Some(curr) = sess_map.get(session_id) {
            if curr == request_id {
                sess_map.remove(session_id);
            }
        }
    }
}

/// Node.js Sidecar 통신 파이프라인 관리자
#[derive(Clone)]
pub struct SidecarBridge {
    db: DatabaseManager,
    inflight: InFlightTracker,
    /// Sidecar stdin 전송 채널 (비동기 송신자)
    stdin_sender: Arc<tokio::sync::mpsc::Sender<String>>,
    /// ACK 대기 맵 (request_id -> oneshot::Sender<SidecarEvent>)
    pending_acks: Arc<Mutex<HashMap<String, oneshot::Sender<SidecarEvent>>>>,
    /// 최종 RESULT 대기 맵 (request_id -> oneshot::Sender<SidecarEvent>)
    pending_results: Arc<Mutex<HashMap<String, oneshot::Sender<SidecarEvent>>>>,
}

impl SidecarBridge {
    /// 브릿지 인스턴스 생성 및 I/O 채널 초기화
    pub fn new(
        db: DatabaseManager,
        inflight: InFlightTracker,
        stdin_sender: tokio::sync::mpsc::Sender<String>,
    ) -> Self {
        Self {
            db,
            inflight,
            stdin_sender: Arc::new(stdin_sender),
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            pending_results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// [Transmission] Sidecar로 NDJSON 커맨드를 전송하고 ACK 및 최종 결과를 처리한다.
    pub async fn dispatch_request(
        &self,
        request: CoreRequest,
        ack_timeout_duration: Duration,
        result_timeout_duration: Duration,
    ) -> CoreResponse {
        let request_id = request.request_id.clone();
        let session_id = request.session_id.clone();

        // 1. [T-201] 중복 제출 방지 락 획득
        if let Err(rejection) = self.inflight.try_acquire(&request) {
            let _ = self.db.update_request_status(&request_id, "REJECTED");
            let _ = self.db.record_history(&request_id, "REJECTED_T201", &rejection.message);
            return rejection;
        }

        // ACK 및 Result 수신용 채널 등록
        let (ack_tx, ack_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        {
            let mut acks = self.pending_acks.lock().unwrap();
            acks.insert(request_id.clone(), ack_tx);
            let mut results = self.pending_results.lock().unwrap();
            results.insert(request_id.clone(), result_tx);
        }

        // 2. NDJSON 커맨드 메시지 직렬화
        let cmd = SidecarCommand {
            r#type: "DISPATCH_WORKFLOW".to_string(),
            request_id: request_id.clone(),
            session_id: session_id.clone(),
            prompt: request.raw_prompt.clone(),
            timestamp: Utc::now().to_rfc3339(),
            metadata: request.metadata.clone(),
        };

        let ndjson_line = match serde_json::to_string(&cmd) {
            Ok(json) => json + "\n",
            Err(e) => {
                self.cleanup(&request_id, &session_id);
                let _ = self.db.update_request_status(&request_id, "FAILED");
                return CoreResponse::technical_error(
                    Some(request_id),
                    format!("O-103: 커맨드 JSON 직렬화 실패: {}", e),
                );
            }
        };

        // Sidecar stdin 파이프로 전송
        if let Err(e) = self.stdin_sender.send(ndjson_line).await {
            self.cleanup(&request_id, &session_id);
            let _ = self.db.update_request_status(&request_id, "FAILED");
            return CoreResponse::technical_error(
                Some(request_id),
                format!("O-103: Sidecar stdin 스트림 전송 불가: {}", e),
            );
        }

        // 3. [T-203 & R-101] 수신 확인(ACK) 타임아웃 검증
        match timeout(ack_timeout_duration, ack_rx).await {
            Ok(Ok(SidecarEvent::Ack { .. })) => {
                let _ = self.db.update_request_status(&request_id, "ACKED");
                let _ = self.db.record_history(&request_id, "ACK_RECEIVED", "ACK verified");
            }
            Ok(Ok(SidecarEvent::Error { error_code, message, .. })) => {
                self.cleanup(&request_id, &session_id);
                let _ = self.db.update_request_status(&request_id, "FAILED");
                return CoreResponse::technical_error(
                    Some(request_id),
                    format!("O-103: Sidecar 수신 검증 단계 에러 [{}]: {}", error_code, message),
                );
            }
            Ok(Err(_)) => {
                self.cleanup(&request_id, &session_id);
                let _ = self.db.update_request_status(&request_id, "FAILED");
                return CoreResponse::technical_error(
                    Some(request_id),
                    "O-103: [T-203] Sidecar ACK 내부 통신 채널이 비정상 종료되었습니다.",
                );
            }
            Err(_) => {
                // [T-203] ACK 타임아웃
                self.cleanup(&request_id, &session_id);
                let _ = self.db.update_request_status(&request_id, "TIMEOUT_ACK");
                let _ = self.db.record_history(
                    &request_id,
                    "TIMEOUT_ACK",
                    &format!("ACK 응답이 {}초 내에 도착하지 않았습니다.", ack_timeout_duration.as_secs()),
                );
                return CoreResponse::technical_error(
                    Some(request_id),
                    format!(
                        "O-103: [T-203] Sidecar 응답 미도착: 수신 확인(ACK) 제한 시간({}초) 초과",
                        ack_timeout_duration.as_secs()
                    ),
                );
            }
            _ => {}
        }

        // 4. [T-203] 워크플로우 완료(RESULT) 대기 및 타임아웃 처리
        let final_response = match timeout(result_timeout_duration, result_rx).await {
            Ok(Ok(SidecarEvent::Result { output, status, .. })) => {
                let _ = self.db.update_request_status(&request_id, "COMPLETED");
                let _ = self.db.record_history(
                    &request_id,
                    "COMPLETED",
                    &serde_json::to_string(&output).unwrap_or_default(),
                );
                // [O-101] 정상 완료 응답 생성
                CoreResponse::success(
                    request_id.clone(),
                    format!("O-101: 에이전트 워크플로우 정상 완료 (상태: {})", status),
                    Some(output),
                )
            }
            Ok(Ok(SidecarEvent::Error { error_code, message, .. })) => {
                let _ = self.db.update_request_status(&request_id, "FAILED");
                let _ = self.db.record_history(
                    &request_id,
                    "EXECUTION_ERROR",
                    &format!("[{}] {}", error_code, message),
                );
                CoreResponse::technical_error(
                    Some(request_id.clone()),
                    format!("O-103: Sidecar 워크플로우 실행 실패 [{}]: {}", error_code, message),
                )
            }
            Ok(Err(_)) => {
                let _ = self.db.update_request_status(&request_id, "FAILED");
                CoreResponse::technical_error(
                    Some(request_id.clone()),
                    "O-103: [T-203] Sidecar 결과 수신 채널이 닫혔습니다.",
                )
            }
            Err(_) => {
                // [T-203] 최종 결과 타임아웃
                let _ = self.db.update_request_status(&request_id, "TIMEOUT_EXEC");
                let _ = self.db.record_history(
                    &request_id,
                    "TIMEOUT_EXECUTION",
                    &format!("실행 제한 시간({}초) 초과", result_timeout_duration.as_secs()),
                );
                CoreResponse::technical_error(
                    Some(request_id.clone()),
                    format!(
                        "O-103: [T-203] 워크플로우 실행 시간 초과: {}초 내에 완료되지 못했습니다.",
                        result_timeout_duration.as_secs()
                    ),
                )
            }
        };

        // 완료 후 추적기에서 해제
        self.cleanup(&request_id, &session_id);
        final_response
    }

    /// Sidecar stdout으로부터 읽어들인 한 줄(NDJSON)을 파싱하여 적절한 대기 채널 및 DB로 디스패치
    pub fn handle_incoming_ndjson(&self, raw_line: &str) {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            return;
        }

        let event: SidecarEvent = match serde_json::from_str(trimmed) {
            Ok(ev) => ev,
            Err(e) => {
                eprintln!("[SidecarBridge] 잘못된 NDJSON 수신: '{}', error: {}", trimmed, e);
                return;
            }
        };

        match &event {
            SidecarEvent::Ack { request_id, .. } => {
                let mut acks = self.pending_acks.lock().unwrap();
                if let Some(sender) = acks.remove(request_id) {
                    let _ = sender.send(event);
                }
            }
            SidecarEvent::Progress {
                request_id,
                node,
                state,
                ..
            } => {
                let payload_str = serde_json::to_string(state).unwrap_or_default();
                let _ = self.db.record_history(
                    request_id,
                    &format!("PROGRESS:{}", node),
                    &payload_str,
                );
            }
            SidecarEvent::Checkpoint {
                request_id,
                thread_id,
                checkpoint_id,
                node,
                state,
                timestamp,
            } => {
                let state_str = serde_json::to_string(state).unwrap_or_default();
                let record = CheckpointRecord {
                    checkpoint_id: checkpoint_id.clone(),
                    request_id: request_id.clone(),
                    thread_id: thread_id.clone(),
                    node_name: node.clone(),
                    state_data: state_str.clone(),
                    created_at: timestamp.clone(),
                };
                let _ = self.db.save_checkpoint(&record);
                let _ = self.db.record_history(
                    request_id,
                    &format!("CHECKPOINT:{}", node),
                    &state_str,
                );
            }
            SidecarEvent::Result { request_id, .. } => {
                let mut results = self.pending_results.lock().unwrap();
                if let Some(sender) = results.remove(request_id) {
                    let _ = sender.send(event);
                }
            }
            SidecarEvent::Error { request_id, .. } => {
                // ACK 대기 중이었거나 Result 대기 중이었던 경우 모두 해소
                {
                    let mut acks = self.pending_acks.lock().unwrap();
                    if let Some(sender) = acks.remove(request_id) {
                        let _ = sender.send(event.clone());
                    }
                }
                {
                    let mut results = self.pending_results.lock().unwrap();
                    if let Some(sender) = results.remove(request_id) {
                        let _ = sender.send(event);
                    }
                }
            }
        }
    }

    fn cleanup(&self, request_id: &str, session_id: &str) {
        self.inflight.release(request_id, session_id);
        self.pending_acks.lock().unwrap().remove(request_id);
        self.pending_results.lock().unwrap().remove(request_id);
    }
}

