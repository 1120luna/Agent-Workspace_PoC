use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{params, Connection, Result};

use crate::types::{CheckpointRecord, CoreRequest, HistoryRecord, RequestRecord};

/// [Persistence Layer] SQLite 기반 데이터 영속성 관리자
#[derive(Clone)]
pub struct DatabaseManager {
    conn: Arc<Mutex<Connection>>,
}

impl DatabaseManager {
    /// 지정된 파일 경로의 SQLite 데이터베이스를 열고 스키마를 초기화한다.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let manager = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        manager.init_schema()?;
        Ok(manager)
    }

    /// 인메모리 SQLite 인스턴스를 생성한다 (단위 테스트 및 임시 실행용).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let manager = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        manager.init_schema()?;
        Ok(manager)
    }

    /// 필수 테이블 및 인덱스 스키마를 생성한다.
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // 1. 요청 테이블 (D-002 원문 보존 포함)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS requests (
                request_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                raw_prompt TEXT NOT NULL,
                status TEXT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
            [],
        )?;

        // 2. 실행 이력 (NDJSON 이벤트 로깅)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS execution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (request_id) REFERENCES requests(request_id)
            );",
            [],
        )?;

        // 3. LangGraph 체크포인트 메타데이터
        conn.execute(
            "CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                node_name TEXT NOT NULL,
                state_data TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (request_id) REFERENCES requests(request_id)
            );",
            [],
        )?;

        // 성능 최적화 인덱스
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_session ON requests(session_id);",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_request_id ON execution_history(request_id);",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_checkpoints_request_id ON checkpoints(request_id);",
            [],
        )?;

        Ok(())
    }

    /// [D-002] 원문이 보존된 요청 레코드를 저장한다.
    pub fn save_request(&self, req: &CoreRequest) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let metadata_str = serde_json::to_string(&req.metadata).unwrap_or_else(|_| "{}".to_string());
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO requests (request_id, session_id, raw_prompt, status, metadata, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                req.request_id,
                req.session_id,
                req.raw_prompt,
                req.status,
                metadata_str,
                req.created_at,
                now
            ],
        )?;

        Ok(())
    }

    /// 요청 상태(RECEIVED, ACKED, RUNNING, COMPLETED, FAILED 등)를 갱신한다.
    pub fn update_request_status(&self, request_id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE requests SET status = ?1, updated_at = ?2 WHERE request_id = ?3",
            params![status, now, request_id],
        )?;

        Ok(())
    }

    /// [T-201 보조] 동일 세션 내에서 현재 실행 중(RECEIVED, ACKED, RUNNING)인 동일 프롬프트 요청 존재 여부 확인
    pub fn has_duplicate_active_request(&self, session_id: &str, raw_prompt: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM requests 
             WHERE session_id = ?1 AND raw_prompt = ?2 AND status IN ('RECEIVED', 'ACKED', 'RUNNING')",
            params![session_id, raw_prompt],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    /// 실행 이벤트 이력을 기록한다 (ACK, PROGRESS, RESULT, ERROR 등).
    pub fn record_history(&self, request_id: &str, event_type: &str, payload: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO execution_history (request_id, event_type, payload, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![request_id, event_type, payload, now],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// LangGraph 워크플로우 체크포인트 상태를 저장한다.
    pub fn save_checkpoint(&self, checkpoint: &CheckpointRecord) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO checkpoints (checkpoint_id, request_id, thread_id, node_name, state_data, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                checkpoint.checkpoint_id,
                checkpoint.request_id,
                checkpoint.thread_id,
                checkpoint.node_name,
                checkpoint.state_data,
                checkpoint.created_at
            ],
        )?;

        Ok(())
    }

    /// 특정 요청 식별자의 상세 정보를 조회한다.
    pub fn get_request(&self, request_id: &str) -> Result<Option<RequestRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT request_id, session_id, raw_prompt, status, metadata, created_at, updated_at
             FROM requests WHERE request_id = ?1",
        )?;

        let mut rows = stmt.query(params![request_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(RequestRecord {
                request_id: row.get(0)?,
                session_id: row.get(1)?,
                raw_prompt: row.get(2)?,
                status: row.get(3)?,
                metadata: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    /// 특정 요청에 대한 실행 이력 목록을 시간순으로 조회한다.
    pub fn get_history(&self, request_id: &str) -> Result<Vec<HistoryRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, request_id, event_type, payload, created_at
             FROM execution_history WHERE request_id = ?1 ORDER BY id ASC",
        )?;

        let rows = stmt.query_map(params![request_id], |row| {
            Ok(HistoryRecord {
                id: row.get(0)?,
                request_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;

        let mut history = Vec::new();
        for item in rows {
            history.push(item?);
        }
        Ok(history)
    }

    /// 특정 요청의 체크포인트 목록을 조회한다.
    pub fn get_checkpoints(&self, request_id: &str) -> Result<Vec<CheckpointRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT checkpoint_id, request_id, thread_id, node_name, state_data, created_at
             FROM checkpoints WHERE request_id = ?1 ORDER BY created_at ASC",
        )?;

        let rows = stmt.query_map(params![request_id], |row| {
            Ok(CheckpointRecord {
                checkpoint_id: row.get(0)?,
                request_id: row.get(1)?,
                thread_id: row.get(2)?,
                node_name: row.get(3)?,
                state_data: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;

        let mut checkpoints = Vec::new();
        for item in rows {
            checkpoints.push(item?);
        }
        Ok(checkpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_and_retrieve_request() {
        let db = DatabaseManager::in_memory().unwrap();
        let raw_prompt = "Line 1\nLine 2\tTabbed\r\nLine 3";
        let req = CoreRequest {
            request_id: "req-001".to_string(),
            raw_prompt: raw_prompt.to_string(),
            session_id: "sess-001".to_string(),
            status: "RECEIVED".to_string(),
            created_at: Utc::now().to_rfc3339(),
            metadata: serde_json::json!({"tag": "test"}),
        };

        db.save_request(&req).unwrap();

        let retrieved = db.get_request("req-001").unwrap().expect("Request not found");
        assert_eq!(retrieved.raw_prompt, raw_prompt);
        assert_eq!(retrieved.status, "RECEIVED");

        // 상태 업데이트
        db.update_request_status("req-001", "RUNNING").unwrap();
        let updated = db.get_request("req-001").unwrap().unwrap();
        assert_eq!(updated.status, "RUNNING");
    }

    #[test]
    fn test_duplicate_active_check() {
        let db = DatabaseManager::in_memory().unwrap();
        let prompt = "Execute agent workflow";
        let req = CoreRequest {
            request_id: "req-100".to_string(),
            raw_prompt: prompt.to_string(),
            session_id: "session-abc".to_string(),
            status: "RUNNING".to_string(),
            created_at: Utc::now().to_rfc3339(),
            metadata: serde_json::json!({}),
        };
        db.save_request(&req).unwrap();

        // 동일 세션 & 동일 프롬프트 중복 검사
        let is_dup = db.has_duplicate_active_request("session-abc", prompt).unwrap();
        assert!(is_dup);

        // 다른 세션
        let is_dup_diff_sess = db.has_duplicate_active_request("session-xyz", prompt).unwrap();
        assert!(!is_dup_diff_sess);

        // 완료 상태로 변경 후에는 중복으로 판단하지 않음
        db.update_request_status("req-100", "COMPLETED").unwrap();
        let is_dup_after_done = db.has_duplicate_active_request("session-abc", prompt).unwrap();
        assert!(!is_dup_after_done);
    }
}

