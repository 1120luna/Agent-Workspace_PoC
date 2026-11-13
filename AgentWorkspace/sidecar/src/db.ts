import path from "path";
import fs from "fs";

export interface CheckpointData {
  checkpointId: string;
  requestId: string;
  threadId: string;
  nodeName: string;
  stateData: string;
  createdAt: string;
}

/**
 * SQLite 체크포인트 저장소
 * better-sqlite3 지원 환경에서는 SQLite DB 파일을 사용하고,
 * 네이티브 바인딩 부재 시 인메모리 맵으로 안전하게 폴백한다.
 */
export class SidecarDatabase {
  private db: any = null;
  private fallbackStore: Map<string, CheckpointData> = new Map();
  private dbPath: string;

  constructor(customPath?: string) {
    const dataDir = process.env.AGENT_DATA_DIR || path.join(process.cwd(), "data");
    if (!fs.existsSync(dataDir)) {
      try {
        fs.mkdirSync(dataDir, { recursive: true });
      } catch (err) {
        // 무시
      }
    }
    this.dbPath = customPath || path.join(dataDir, "sidecar_checkpoints.db");
    this.init();
  }

  private init() {
    try {
      // better-sqlite3 동적 임포트 시도
      const Database = require("better-sqlite3");
      this.db = new Database(this.dbPath);
      this.db.pragma("journal_mode = WAL");
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS checkpoints (
          checkpoint_id TEXT PRIMARY KEY,
          request_id TEXT NOT NULL,
          thread_id TEXT NOT NULL,
          node_name TEXT NOT NULL,
          state_data TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chk_req ON checkpoints(request_id);
        CREATE INDEX IF NOT EXISTS idx_chk_thread ON checkpoints(thread_id);
      `);
    } catch (e) {
      // 네이티브 바이너리가 없거나 로드 실패 시 메모리 폴백
      this.db = null;
    }
  }

  public saveCheckpoint(
    checkpointId: string,
    requestId: string,
    threadId: string,
    nodeName: string,
    state: any
  ): CheckpointData {
    const record: CheckpointData = {
      checkpointId,
      requestId,
      threadId,
      nodeName,
      stateData: JSON.stringify(state),
      createdAt: new Date().toISOString(),
    };

    if (this.db) {
      try {
        const stmt = this.db.prepare(`
          INSERT OR REPLACE INTO checkpoints (checkpoint_id, request_id, thread_id, node_name, state_data, created_at)
          VALUES (?, ?, ?, ?, ?, ?)
        `);
        stmt.run(
          record.checkpointId,
          record.requestId,
          record.threadId,
          record.nodeName,
          record.stateData,
          record.createdAt
        );
      } catch (err) {
        this.fallbackStore.set(checkpointId, record);
      }
    } else {
      this.fallbackStore.set(checkpointId, record);
    }

    return record;
  }

  public getCheckpoints(requestId: string): CheckpointData[] {
    if (this.db) {
      try {
        const stmt = this.db.prepare(`
          SELECT checkpoint_id AS checkpointId, request_id AS requestId, thread_id AS threadId,
                 node_name AS nodeName, state_data AS stateData, created_at AS createdAt
          FROM checkpoints WHERE request_id = ? ORDER BY created_at ASC
        `);
        return stmt.all(requestId) as CheckpointData[];
      } catch (err) {
        // 폴백
      }
    }

    return Array.from(this.fallbackStore.values()).filter(
      (c) => c.requestId === requestId
    );
  }
}

