// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod persistence;
mod reception;
mod transmission;
mod types;

use std::path::PathBuf;
use std::time::Duration;

use tauri::{AppHandle, Emitter, Manager, State};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use persistence::DatabaseManager;
use reception::ReceptionEngine;
use transmission::{InFlightTracker, SidecarBridge};
use types::{
    CheckpointRecord, CoreResponse, FeInputPayload, HistoryRecord, RequestRecord,
};

/// Tauri 애플리케이션 전역 상태 관리자
pub struct AppState {
    pub db: DatabaseManager,
    pub bridge: SidecarBridge,
    pub inflight: InFlightTracker,
}

/// [Tauri Command 1] 프론트엔드 입력값 수신 및 에이전트 워크플로우 디스패치
#[tauri::command]
async fn submit_prompt(
    payload: FeInputPayload,
    state: State<'_, AppState>,
    app_handle: AppHandle,
) -> Result<CoreResponse, CoreResponse> {
    // 1. [R-100] 빈 값/공백 검증 및 [D-002] 원문 보존 객체 생성
    let core_request = match ReceptionEngine::process_input(payload) {
        Ok(req) => req,
        Err(rejection) => {
            // [O-102] 입력 거부 응답 반환 및 UI 채널 통지
            let _ = app_handle.emit("agent-output", &rejection);
            return Err(rejection);
        }
    };

    let request_id = core_request.request_id.clone();

    // 2. [Persistence] 초기 요청 데이터 SQLite 영속화
    if let Err(e) = state.db.save_request(&core_request) {
        let err_response = CoreResponse::technical_error(
            Some(request_id),
            format!("O-103: SQLite 데이터베이스 요청 저장 실패: {}", e),
        );
        let _ = app_handle.emit("agent-output", &err_response);
        return Err(err_response);
    }

    // 3. [Transmission] Sidecar 통신 파이프라인 (T-201 중복 방지 & T-203 타임아웃 적용)
    // ACK 타임아웃 5초, 워크플로우 실행 타임아웃 120초
    let response = state
        .bridge
        .dispatch_request(
            core_request,
            Duration::from_secs(5),
            Duration::from_secs(120),
        )
        .await;

    // 4. [O-101 / O-102 / O-103] 처리 결과를 UI 채널로 전달
    let _ = app_handle.emit("agent-output", &response);

    if response.code == types::ResponseCode::Success {
        Ok(response)
    } else {
        Err(response)
    }
}

/// [Tauri Command 2] 특정 요청 상태 조회
#[tauri::command]
async fn get_request_status(
    request_id: String,
    state: State<'_, AppState>,
) -> Result<Option<RequestRecord>, String> {
    state.db.get_request(&request_id).map_err(|e| e.to_string())
}

/// [Tauri Command 3] 특정 요청의 이벤트 실행 이력 조회
#[tauri::command]
async fn get_request_history(
    request_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<HistoryRecord>, String> {
    state.db.get_history(&request_id).map_err(|e| e.to_string())
}

/// [Tauri Command 4] 특정 요청의 체크포인트 목록 조회
#[tauri::command]
async fn get_request_checkpoints(
    request_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<CheckpointRecord>, String> {
    state.db.get_checkpoints(&request_id).map_err(|e| e.to_string())
}

/// Sidecar 장기 실행 백그라운드 프로세스 기동
fn spawn_sidecar_process(
    app_dir: PathBuf,
    stdin_rx: mpsc::Receiver<String>,
    bridge: SidecarBridge,
    app_handle: AppHandle,
) {
    tauri::async_runtime::spawn(async move {
        let mut rx = stdin_rx;

        // Sidecar 실행 경로 (절대 경로로 직접 지정)
        let sidecar_dir = PathBuf::from(r"C:\Users\LG\Desktop\git blog\1120luna.github.io\Agent-Workspace_PoC\sidecar");
        let sidecar_dist = sidecar_dir.join("dist").join("index.js");

        let mut cmd = if sidecar_dist.exists() {
            let mut c = Command::new("node");
            c.arg(&sidecar_dist);
            c
        } else {
            eprintln!("[Tauri Rust Core] 빌드된 sidecar dist가 없습니다: {:?}", sidecar_dist);
            Command::new("node")
        };

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .current_dir(sidecar_dir);

        eprintln!("[Tauri Rust Core] Node.js Sidecar 프로세스 시작 중...");

        let mut child: Child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[Tauri Rust Core] Sidecar 프로세스 스폰 실패: {}", e);
                return;
            }
        };

        let mut stdin = child.stdin.take().expect("Failed to open sidecar stdin");
        let stdout = child.stdout.take().expect("Failed to open sidecar stdout");
        let stderr = child.stderr.take().expect("Failed to open sidecar stderr");

        // 1. Sidecar stdin 쓰기 태스크
        tauri::async_runtime::spawn(async move {
            while let Some(line) = rx.recv().await {
                if let Err(e) = stdin.write_all(line.as_bytes()).await {
                    eprintln!("[Sidecar Stdin Task] 라인 쓰기 오류: {}", e);
                    break;
                }
                let _ = stdin.flush().await;
            }
        });

        // 2. Sidecar stdout 읽기 (NDJSON 스트림 파서) 태스크
        let bridge_clone = bridge.clone();
        let app_handle_clone = app_handle.clone();
        tauri::async_runtime::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // 브릿지로 NDJSON 이벤트 전달 (ACK 매칭, Checkpoint 저장 등)
                bridge_clone.handle_incoming_ndjson(trimmed);

                // 프론트엔드로 실시간 이벤트 스트리밍
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    let _ = app_handle_clone.emit("sidecar-stream", &json_val);
                }
            }
        });

        // 3. Sidecar stderr 로깅 태스크
        tauri::async_runtime::spawn(async move {
            let mut err_reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = err_reader.next_line().await {
                eprintln!("[Sidecar STDERR] {}", line);
            }
        });

        // 프로세스 종료 감시
        let status = child.wait().await;
        eprintln!("[Tauri Rust Core] Sidecar 프로세스 종료됨: {:?}", status);
    });
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            let app_handle = app.handle().clone();

            // DB 파일 경로 설정 (앱 데이터 디렉터리 내 agent_workspace.db)
            let app_data_dir = app
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("./"));
            std::fs::create_dir_all(&app_data_dir).ok();
            let db_path = app_data_dir.join("agent_workspace.db");

            eprintln!("[Tauri Core] SQLite DB 경로: {:?}", db_path);
            let db = DatabaseManager::new(db_path).expect("Failed to initialize SQLite Database");

            let inflight = InFlightTracker::new();
            let (stdin_tx, stdin_rx) = mpsc::channel::<String>(100);

            let bridge = SidecarBridge::new(db.clone(), inflight.clone(), stdin_tx);

            // Sidecar 장기 실행 프로세스 백그라운드 구동
            spawn_sidecar_process(app_data_dir, stdin_rx, bridge.clone(), app_handle.clone());

            // Tauri 상태 등록
            app.manage(AppState {
                db,
                bridge,
                inflight,
            });

            eprintln!("[Tauri Core] Agent Workspace Core 백엔드 준비 완료.");
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            submit_prompt,
            get_request_status,
            get_request_history,
            get_request_checkpoints
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

