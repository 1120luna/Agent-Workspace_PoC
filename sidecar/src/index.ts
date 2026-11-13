import * as readline from "readline";
import { z } from "zod";
import { SidecarDatabase } from "./db";
import { WorkflowRunner } from "./workflow";

/**
 * Zod 기반 Host -> Sidecar 커맨드 페이로드 유효성 검증 스키마
 */
const CommandPayloadSchema = z.object({
  type: z.literal("DISPATCH_WORKFLOW"),
  request_id: z.string().min(1, "request_id는 비어있을 수 없습니다."),
  session_id: z.string().min(1, "session_id는 비어있을 수 없습니다."),
  prompt: z.string().min(1, "prompt는 비어있을 수 없습니다."),
  timestamp: z.string().optional(),
  metadata: z.record(z.any()).optional().default({}),
});

type CommandPayload = z.infer<typeof CommandPayloadSchema>;

/**
 * 표준 NDJSON 출력 헬퍼 (stdout으로 단일 라인 JSON 전송)
 */
function emitNdjson(payload: Record<string, any>) {
  const line = JSON.stringify(payload);
  process.stdout.write(line + "\n");
}

// 초기화
const db = new SidecarDatabase();
const workflowRunner = new WorkflowRunner(db);

// stdin 인터페이스 연결
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

// stderr 로거 (디버깅용, NDJSON 오염 방지)
function logDebug(message: string) {
  process.stderr.write(`[Sidecar] ${new Date().toISOString()} ${message}\n`);
}

logDebug("Sidecar 런타임이 성공적으로 기동되었습니다. stdin 입력을 수신 대기합니다.");

// NDJSON 라인 리스너
rl.on("line", async (line: string) => {
  const trimmed = line.trim();
  if (!trimmed) return;

  let rawJson: any;
  try {
    rawJson = JSON.parse(trimmed);
  } catch (err: any) {
    emitNdjson({
      type: "ERROR",
      request_id: "unknown",
      error_code: "JSON_PARSE_ERROR",
      message: `NDJSON 파싱 오류: ${err.message}`,
      timestamp: new Date().toISOString(),
    });
    return;
  }

  // 1. Zod 스키마 검증
  const validationResult = CommandPayloadSchema.safeParse(rawJson);
  if (!validationResult.success) {
    const errorMessages = validationResult.error.errors
      .map((e) => `${e.path.join(".")}: ${e.message}`)
      .join(", ");

    emitNdjson({
      type: "ERROR",
      request_id: rawJson.request_id || "unknown",
      error_code: "ZOD_VALIDATION_ERROR",
      message: `요청 페이로드 유효성 검증 실패: ${errorMessages}`,
      timestamp: new Date().toISOString(),
    });
    return;
  }

  const command = validationResult.data;

  // 2. [R-101] 정상 수신 확인(ACK) 즉시 반환
  emitNdjson({
    type: "ACK",
    request_id: command.request_id,
    timestamp: new Date().toISOString(),
  });

  logDebug(`[R-101 ACK 발송 완료] request_id: ${command.request_id}`);

  // 3. 비동기 LangGraph.js 에이전트 워크플로우 트리거
  try {
    const finalState = await workflowRunner.execute(
      command.request_id,
      command.session_id,
      command.prompt,
      command.metadata,
      {
        onProgress: (nodeName, state) => {
          emitNdjson({
            type: "PROGRESS",
            request_id: command.request_id,
            node: nodeName,
            state,
            timestamp: new Date().toISOString(),
          });
        },
        onCheckpoint: (nodeName, checkpointId, state) => {
          emitNdjson({
            type: "CHECKPOINT",
            request_id: command.request_id,
            thread_id: command.session_id,
            checkpoint_id: checkpointId,
            node: nodeName,
            state,
            timestamp: new Date().toISOString(),
          });
        },
      }
    );

    // 4. [O-101] 최종 완료 결과 이벤트 방출
    emitNdjson({
      type: "RESULT",
      request_id: command.request_id,
      status: finalState.status,
      output: {
        finalAnswer: finalState.finalAnswer,
        plan: finalState.plan,
        executionResults: finalState.executionResults,
        currentStep: finalState.currentStep,
      },
      timestamp: new Date().toISOString(),
    });

    logDebug(`[RESULT 발송 완료] request_id: ${command.request_id}`);
  } catch (err: any) {
    logDebug(`[워크플로우 오류] request_id: ${command.request_id}, err: ${err.message}`);
    // [O-103] 실행 단계 기술적 오류 이벤트 방출
    emitNdjson({
      type: "ERROR",
      request_id: command.request_id,
      error_code: "WORKFLOW_EXECUTION_ERROR",
      message: err.message || "워크플로우 실행 중 예외가 발생했습니다.",
      timestamp: new Date().toISOString(),
    });
  }
});

process.on("SIGINT", () => {
  logDebug("SIGINT 수신, 정상 종료합니다.");
  process.exit(0);
});

process.on("SIGTERM", () => {
  logDebug("SIGTERM 수신, 정상 종료합니다.");
  process.exit(0);
});

