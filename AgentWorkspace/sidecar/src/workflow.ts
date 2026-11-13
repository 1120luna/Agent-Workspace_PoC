import { StateGraph, END, START, Annotation } from "@langchain/langgraph";
import { SidecarDatabase } from "./db";

/**
 * 에이전트 워크플로우 상태 스키마 정의
 */
export interface AgentState {
  requestId: string;
  sessionId: string;
  prompt: string;
  metadata: Record<string, any>;
  plan: string[];
  executionResults: Record<string, any>;
  currentStep: string;
  finalAnswer: string;
  status: "INITIALIZED" | "PLANNING" | "EXECUTING" | "SYNTHESIZING" | "COMPLETED" | "FAILED";
  error?: string;
}

// LangGraph State Annotation
export const AgentStateAnnotation = Annotation.Root({
  requestId: Annotation<string>({
    reducer: (_, next) => next,
    default: () => "",
  }),
  sessionId: Annotation<string>({
    reducer: (_, next) => next,
    default: () => "",
  }),
  prompt: Annotation<string>({
    reducer: (_, next) => next,
    default: () => "",
  }),
  metadata: Annotation<Record<string, any>>({
    reducer: (curr, next) => ({ ...curr, ...next }),
    default: () => ({}),
  }),
  plan: Annotation<string[]>({
    reducer: (_, next) => next,
    default: () => [],
  }),
  executionResults: Annotation<Record<string, any>>({
    reducer: (curr, next) => ({ ...curr, ...next }),
    default: () => ({}),
  }),
  currentStep: Annotation<string>({
    reducer: (_, next) => next,
    default: () => "START",
  }),
  finalAnswer: Annotation<string>({
    reducer: (_, next) => next,
    default: () => "",
  }),
  status: Annotation<"INITIALIZED" | "PLANNING" | "EXECUTING" | "SYNTHESIZING" | "COMPLETED" | "FAILED">({
    reducer: (_, next) => next,
    default: () => "INITIALIZED",
  }),
  error: Annotation<string | undefined>({
    reducer: (_, next) => next,
    default: () => undefined,
  }),
});

export type AgentStateType = typeof AgentStateAnnotation.State;

export interface WorkflowHooks {
  onProgress?: (nodeName: string, state: Partial<AgentStateType>) => void;
  onCheckpoint?: (nodeName: string, checkpointId: string, state: Partial<AgentStateType>) => void;
}

/**
 * LangGraph.js 기반 워크플로우 팩토리
 */
export class WorkflowRunner {
  private db: SidecarDatabase;

  constructor(db: SidecarDatabase) {
    this.db = db;
  }

  /**
   * 컴파일된 StateGraph 인스턴스 빌드
   */
  public buildGraph(hooks?: WorkflowHooks) {
    const graph = new StateGraph(AgentStateAnnotation)
      // 1. 노드 1: 인풋 분석 (Input Analyzer)
      .addNode("input_analyzer", async (state: AgentStateType) => {
        const nextState: Partial<AgentStateType> = {
          currentStep: "input_analyzer",
          status: "PLANNING",
        };

        const checkpointId = `chk_${state.requestId}_step1`;
        this.db.saveCheckpoint(checkpointId, state.requestId, state.sessionId, "input_analyzer", {
          ...state,
          ...nextState,
        });

        hooks?.onProgress?.("input_analyzer", nextState);
        hooks?.onCheckpoint?.("input_analyzer", checkpointId, nextState);
        return nextState;
      })
      // 2. 노드 2: 계획 수립 (Planner)
      .addNode("plan_generator", async (state: AgentStateType) => {
        // 프롬프트 기반 가상 작업 계획 수립
        const plan = [
          `1. 사용자 프롬프트 의미 해석: "${state.prompt.slice(0, 50)}..."`,
          "2. 컨텍스트 검색 및 필요한 도구 파라미터 매핑",
          "3. 목표 작업 추론 및 단계별 실행",
          "4. 최종 산출물 합성 및 검증",
        ];

        const nextState: Partial<AgentStateType> = {
          plan,
          currentStep: "plan_generator",
          status: "EXECUTING",
        };

        const checkpointId = `chk_${state.requestId}_step2`;
        this.db.saveCheckpoint(checkpointId, state.requestId, state.sessionId, "plan_generator", {
          ...state,
          ...nextState,
        });

        hooks?.onProgress?.("plan_generator", nextState);
        hooks?.onCheckpoint?.("plan_generator", checkpointId, nextState);
        return nextState;
      })
      // 3. 노드 3: 단계별 실행 (Executor)
      .addNode("action_executor", async (state: AgentStateType) => {
        const executionResults: Record<string, any> = {
          executionTimeMs: 120,
          stepsCompleted: state.plan.length,
          artifactsGenerated: ["agent-task-plan.json"],
          contextSize: state.prompt.length,
        };

        const nextState: Partial<AgentStateType> = {
          executionResults,
          currentStep: "action_executor",
          status: "SYNTHESIZING",
        };

        const checkpointId = `chk_${state.requestId}_step3`;
        this.db.saveCheckpoint(checkpointId, state.requestId, state.sessionId, "action_executor", {
          ...state,
          ...nextState,
        });

        hooks?.onProgress?.("action_executor", nextState);
        hooks?.onCheckpoint?.("action_executor", checkpointId, nextState);
        return nextState;
      })
      // 4. 노드 4: 결과 합성 (Synthesizer)
      .addNode("synthesizer", async (state: AgentStateType) => {
        const finalAnswer = `[Agent Workspace Core Output]\n요청하신 "${state.prompt}" 에 대한 에이전트 실행이 성공적으로 완료되었습니다.\n총 ${state.plan.length}개 단계의 계획이 이행되었으며, 결과가 영속화되었습니다.`;

        const nextState: Partial<AgentStateType> = {
          finalAnswer,
          currentStep: "synthesizer",
          status: "COMPLETED",
        };

        const checkpointId = `chk_${state.requestId}_step4`;
        this.db.saveCheckpoint(checkpointId, state.requestId, state.sessionId, "synthesizer", {
          ...state,
          ...nextState,
        });

        hooks?.onProgress?.("synthesizer", nextState);
        hooks?.onCheckpoint?.("synthesizer", checkpointId, nextState);
        return nextState;
      });

    // 엣지 연결 (순차적 파이프라인)
    graph.addEdge(START, "input_analyzer" as any);
    graph.addEdge("input_analyzer" as any, "plan_generator" as any);
    graph.addEdge("plan_generator" as any, "action_executor" as any);
    graph.addEdge("action_executor" as any, "synthesizer" as any);
    graph.addEdge("synthesizer" as any, END);

    return graph.compile();
  }

  /**
   * 워크플로우 실행
   */
  public async execute(
    requestId: string,
    sessionId: string,
    prompt: string,
    metadata: Record<string, any>,
    hooks?: WorkflowHooks
  ): Promise<AgentStateType> {
    const app = this.buildGraph(hooks);

    const initialState: AgentStateType = {
      requestId,
      sessionId,
      prompt,
      metadata,
      plan: [],
      executionResults: {},
      currentStep: "INITIALIZED",
      finalAnswer: "",
      status: "INITIALIZED",
      error: undefined,
    };

    const result = await app.invoke(initialState);
    return result as AgentStateType;
  }
}

