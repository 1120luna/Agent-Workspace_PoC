use chrono::Utc;
use serde::{Deserialize, Serialize};

/// [I-001] 프론트엔드(UI)로부터 수신되는 원시 입력 페이로드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeInputPayload {
    /// 사용자 프롬프트 텍스트 (줄바꿈/공백 포함)
    pub prompt: String,
    /// 세션/대화 컨텍스트 식별자 (선택 사항)
    pub session_id: Option<String>,
    /// 클라이언트 메타데이터 (선택 사항)
    pub metadata: Option<serde_json::Value>,
}

/// [D-002] 줄바꿈과 공백이 원문 그대로 보존된 Core 내부 요청 객체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreRequest {
    /// 요청 고유 식별자 (UUID v4)
    pub request_id: String,
    /// 원본 보존된 프롬프트 (D-002: 줄바꿈, 공백 등 무변형 원문)
    pub raw_prompt: String,
    /// 세션 식별자
    pub session_id: String,
    /// 요청 상태 (RECEIVED, ACKED, RUNNING, COMPLETED, FAILED, REJECTED)
    pub status: String,
    /// ISO 8601 생성 타임스탬프
    pub created_at: String,
    /// 부가 메타데이터
    pub metadata: serde_json::Value,
}

/// [O-001] 백엔드 응답 상태 코드 분류
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseCode {
    /// [O-101] 정상 수신 및 완료 (Success / ACK Received / Execution Done)
    #[serde(rename = "O-101")]
    Success,
    /// [O-102] 입력 거부 (Input Rejected: 빈 값, 유효성 실패, 중복 요청 등)
    #[serde(rename = "O-102")]
    InputRejected,
    /// [O-103] 기술적 오류 (Technical Error: Sidecar 통신 실패, 타임아웃, DB 오류 등)
    #[serde(rename = "O-103")]
    TechnicalError,
}

impl ResponseCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResponseCode::Success => "O-101",
            ResponseCode::InputRejected => "O-102",
            ResponseCode::TechnicalError => "O-103",
        }
    }
}

/// [O-002] 프론트엔드로 전달되는 표준 Core 응답 객체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreResponse {
    /// [O-101, O-102, O-103] 분류 코드
    pub code: ResponseCode,
    /// 상태 라벨 (SUCCESS, REJECTED, ERROR)
    pub status: String,
    /// 연결된 요청 식별자
    pub request_id: Option<String>,
    /// 상태 및 결과 메시지
    pub message: String,
    /// 결과 데이터 페이로드
    pub data: Option<serde_json::Value>,
    /// 응답 생성 타임스탬프
    pub timestamp: String,
}

impl CoreResponse {
    /// [O-101] 정상 응답 생성
    pub fn success(request_id: String, message: impl Into<String>, data: Option<serde_json::Value>) -> Self {
        Self {
            code: ResponseCode::Success,
            status: "SUCCESS".to_string(),
            request_id: Some(request_id),
            message: message.into(),
            data,
            timestamp: Utc::now().to_rfc3339(),
        }
    }

    /// [O-102] 입력 거부 응답 생성
    pub fn rejected(request_id: Option<String>, message: impl Into<String>) -> Self {
        Self {
            code: ResponseCode::InputRejected,
            status: "REJECTED".to_string(),
            request_id,
            message: message.into(),
            data: None,
            timestamp: Utc::now().to_rfc3339(),
        }
    }

    /// [O-103] 기술적 오류 응답 생성
    pub fn technical_error(request_id: Option<String>, message: impl Into<String>) -> Self {
        Self {
            code: ResponseCode::TechnicalError,
            status: "ERROR".to_string(),
            request_id,
            message: message.into(),
            data: None,
            timestamp: Utc::now().to_rfc3339(),
        }
    }
}

/// Tauri Core -> Node.js Sidecar로 전달되는 NDJSON 커맨드 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarCommand {
    pub r#type: String, // "DISPATCH_WORKFLOW", "CANCEL" 등
    pub request_id: String,
    pub session_id: String,
    pub prompt: String,
    pub timestamp: String,
    pub metadata: serde_json::Value,
}

/// Node.js Sidecar -> Tauri Core로 수신되는 NDJSON 이벤트 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SidecarEvent {
    /// [R-101] Sidecar 수신 검증 완료 즉시 ACK 반환
    #[serde(rename = "ACK")]
    Ack {
        request_id: String,
        timestamp: String,
    },
    /// LangGraph 노드 실행 진행 상황
    #[serde(rename = "PROGRESS")]
    Progress {
        request_id: String,
        node: String,
        state: serde_json::Value,
        timestamp: String,
    },
    /// 체크포인트 저장 통보
    #[serde(rename = "CHECKPOINT")]
    Checkpoint {
        request_id: String,
        thread_id: String,
        checkpoint_id: String,
        node: String,
        state: serde_json::Value,
        timestamp: String,
    },
    /// [O-101] 워크플로우 정상 완료 결과
    #[serde(rename = "RESULT")]
    Result {
        request_id: String,
        status: String,
        output: serde_json::Value,
        timestamp: String,
    },
    /// [O-103] Sidecar 실행 중 발생한 오류
    #[serde(rename = "ERROR")]
    Error {
        request_id: String,
        error_code: String,
        message: String,
        timestamp: String,
    },
}

/// DB에 영속화될 요청 이력 레코드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestRecord {
    pub request_id: String,
    pub session_id: String,
    pub raw_prompt: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub metadata: String,
}

/// DB에 영속화될 실행 이벤트 이력 레코드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub id: i64,
    pub request_id: String,
    pub event_type: String,
    pub payload: String,
    pub created_at: String,
}

/// DB에 영속화될 체크포인트 레코드
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub checkpoint_id: String,
    pub request_id: String,
    pub thread_id: String,
    pub node_name: String,
    pub state_data: String,
    pub created_at: String,
}

