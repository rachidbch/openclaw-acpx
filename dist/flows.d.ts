import { C as PermissionPolicy, E as PromptInput, S as PermissionMode, b as McpServer$1, t as SessionAgentOptions, x as NonInteractivePermissionPolicy, y as AuthPolicy } from "./session-options-RpasATRo.js";
import "./client-B5pDFmTo.js";
import { SetSessionConfigOptionResponse } from "@agentclientprotocol/sdk";
import net from "node:net";
//#region src/flows/types.d.ts
type MaybePromise<T> = T | Promise<T>;
type FlowRunDefinition<TInput = unknown> = {
  title?: string | ((context: {
    input: TInput;
    flowName: string;
    flowPath?: string;
  }) => MaybePromise<string | undefined>);
};
type FlowNodeContext<TInput = unknown> = {
  input: TInput;
  outputs: Record<string, unknown>;
  results: Record<string, FlowNodeResult>;
  state: FlowRunState;
  services: Record<string, unknown>;
  /** Supplied by FlowRunner; aborts when this attempt times out or is cancelled. */
  signal?: AbortSignal;
  /** Supplied to function actions; cancellation retires the owned command before the attempt ends. */
  runShell?: (execution: FlowShellExecution) => Promise<FlowShellResult>;
};
type FlowNodeCommon = {
  timeoutMs?: number;
  heartbeatMs?: number;
  statusDetail?: string;
};
type FlowEdge = {
  from: string;
  to: string;
} | {
  from: string;
  switch: {
    on: string;
    cases: Record<string, string>;
  };
};
type AcpNodeDefinition = FlowNodeCommon & {
  nodeType: "acp";
  profile?: string;
  cwd?: string | ((context: FlowNodeContext) => MaybePromise<string | undefined>);
  session?: {
    handle?: string;
    isolated?: boolean;
  };
  prompt: (context: FlowNodeContext) => MaybePromise<PromptInput | string>;
  parse?: (text: string, context: FlowNodeContext) => MaybePromise<unknown>;
};
type ComputeNodeDefinition = FlowNodeCommon & {
  nodeType: "compute";
  run: (context: FlowNodeContext) => MaybePromise<unknown>;
};
type FunctionActionNodeDefinition = FlowNodeCommon & {
  nodeType: "action";
  run: (context: FlowNodeContext) => MaybePromise<unknown>;
};
type ShellActionExecution = {
  command: string;
  args?: string[];
  cwd?: string;
  env?: Record<string, string>;
  stdin?: string;
  shell?: boolean | string;
  allowNonZeroExit?: boolean;
  timeoutMs?: number;
  /** Per-stream UTF-8 capture limit. Omitted is unlimited; zero permits empty output only. */
  maxBufferBytes?: number;
};
type ShellActionResult = {
  command: string;
  args: string[];
  cwd: string;
  stdout: string;
  stderr: string;
  combinedOutput: string;
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  durationMs: number;
};
type FlowShellExecution = Omit<ShellActionExecution, "allowNonZeroExit">;
type FlowShellResult = ShellActionResult & {
  /** The command's own deadline expired; an enclosing attempt cancellation rejects instead. */
  timedOut: boolean;
};
type ShellActionNodeDefinition = FlowNodeCommon & {
  nodeType: "action";
  exec: (context: FlowNodeContext) => MaybePromise<ShellActionExecution>;
  parse?: (result: ShellActionResult, context: FlowNodeContext) => MaybePromise<unknown>;
};
type ActionNodeDefinition = FunctionActionNodeDefinition | ShellActionNodeDefinition;
type CheckpointNodeDefinition = FlowNodeCommon & {
  nodeType: "checkpoint";
  summary?: string;
  run?: (context: FlowNodeContext) => MaybePromise<unknown>;
};
type FlowNodeDefinition = AcpNodeDefinition | ComputeNodeDefinition | ActionNodeDefinition | CheckpointNodeDefinition;
type FlowPermissionRequirements = {
  requiredMode: PermissionMode;
  requireExplicitGrant?: boolean;
  reason?: string;
};
type FlowDefinition = {
  name: string;
  run?: FlowRunDefinition;
  permissions?: FlowPermissionRequirements;
  startAt: string;
  nodes: Record<string, FlowNodeDefinition>;
  edges: FlowEdge[];
};
type FlowNodeOutcome = "ok" | "timed_out" | "failed" | "cancelled";
type FlowNodeResult = {
  attemptId: string;
  nodeId: string;
  nodeType: FlowNodeDefinition["nodeType"];
  outcome: FlowNodeOutcome;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  output?: unknown;
  error?: string;
};
type FlowArtifactRef = {
  path: string;
  mediaType: string;
  bytes: number;
  sha256: string;
};
type FlowConversationTrace = {
  sessionId: string;
  messageStart: number;
  messageEnd: number;
  eventStartSeq: number;
  eventEndSeq: number;
};
type FlowActionReceipt = {
  actionType: "shell" | "function";
  command?: string;
  args?: string[];
  cwd?: string;
  exitCode?: number | null;
  signal?: NodeJS.Signals | null;
  durationMs?: number;
};
type FlowStepTrace = {
  sessionId?: string;
  promptArtifact?: FlowArtifactRef;
  rawResponseArtifact?: FlowArtifactRef;
  outputArtifact?: FlowArtifactRef;
  outputInline?: unknown;
  stdoutArtifact?: FlowArtifactRef;
  stderrArtifact?: FlowArtifactRef;
  conversation?: FlowConversationTrace;
  action?: FlowActionReceipt;
};
type FlowStepRecord = {
  attemptId: string;
  nodeId: string;
  nodeType: FlowNodeDefinition["nodeType"];
  outcome: FlowNodeOutcome;
  startedAt: string;
  finishedAt: string;
  promptText: string | null;
  rawText: string | null;
  output: unknown;
  error?: string;
  session: FlowSessionBinding | null;
  agent: {
    agentName: string;
    agentCommand: string;
    cwd: string;
  } | null;
  trace?: FlowStepTrace;
};
type FlowSessionBinding = {
  key: string;
  handle: string;
  bundleId: string;
  name: string;
  profile?: string;
  agentName: string;
  agentCommand: string;
  agentArgv?: string[];
  cwd: string;
  acpxRecordId: string;
  acpSessionId: string;
  agentSessionId?: string;
};
type FlowRunState = {
  runId: string;
  flowName: string;
  runTitle?: string;
  flowPath?: string;
  startedAt: string;
  finishedAt?: string;
  updatedAt: string;
  status: "running" | "waiting" | "completed" | "failed" | "timed_out";
  input: unknown;
  outputs: Record<string, unknown>;
  results: Record<string, FlowNodeResult>;
  steps: FlowStepRecord[];
  sessionBindings: Record<string, FlowSessionBinding>;
  currentNode?: string;
  currentAttemptId?: string;
  currentNodeType?: FlowNodeDefinition["nodeType"];
  currentNodeStartedAt?: string;
  lastHeartbeatAt?: string;
  statusDetail?: string;
  waitingOn?: string;
  error?: string;
};
type FlowRunResult = {
  runDir: string;
  state: FlowRunState;
};
type ResolvedFlowAgent = {
  agentName: string;
  agentCommand: string;
  agentArgv?: string[];
  cwd: string;
};
type FlowRunnerOptions = {
  resolveAgent: (profile?: string) => ResolvedFlowAgent;
  permissionMode: PermissionMode;
  mcpServers?: McpServer$1[];
  nonInteractivePermissions?: NonInteractivePermissionPolicy;
  permissionPolicy?: PermissionPolicy;
  authCredentials?: Record<string, string>;
  authPolicy?: AuthPolicy;
  fs?: boolean;
  timeoutMs?: number;
  defaultNodeTimeoutMs?: number;
  ttlMs?: number;
  verbose?: boolean;
  suppressSdkConsoleErrors?: boolean;
  sessionOptions?: SessionAgentOptions;
  services?: Record<string, unknown>;
  outputRoot?: string;
};
//#endregion
//#region src/flows/definition.d.ts
export declare function defineFlow<TFlow extends FlowDefinition>(definition: TFlow): TFlow;
export declare function acp(definition: Omit<AcpNodeDefinition, "nodeType">): AcpNodeDefinition;
export declare function compute(definition: Omit<ComputeNodeDefinition, "nodeType">): ComputeNodeDefinition;
export declare function action(definition: Omit<FunctionActionNodeDefinition, "nodeType">): FunctionActionNodeDefinition;
export declare function action(definition: Omit<ShellActionNodeDefinition, "nodeType">): ShellActionNodeDefinition;
export declare function shell(definition: Omit<ShellActionNodeDefinition, "nodeType">): ShellActionNodeDefinition;
export declare function checkpoint(definition?: Omit<CheckpointNodeDefinition, "nodeType">): CheckpointNodeDefinition;
//#endregion
//#region src/flows/runtime.d.ts
export declare class FlowRunner {
  private readonly resolveAgent;
  private readonly defaultCwd;
  private readonly connectionOptions;
  private readonly defaultNodeTimeoutMs;
  private readonly suppressSdkConsoleErrors?;
  private readonly sessionOptions?;
  private readonly services;
  private readonly store;
  private readonly pendingPersistentSessionClients;
  private readonly attempts;
  private readonly pendingClientReleases;
  private readonly runInterruptions;
  private readonly shellOwners;
  constructor(options: FlowRunnerOptions);
  run(flow: FlowDefinition, input: unknown, options?: {
    flowPath?: string;
  }): Promise<FlowRunResult>;
  private runWithOwnership;
  private registerShellOwner;
  private cancelShellOwners;
  private releaseShellOwners;
  private executeFlowRun;
  private throwIfRunInterrupted;
  private executeFlowStep;
  private makeAttemptContext;
  private writeNodeStartedSnapshot;
  private maybeCompleteCheckpointStep;
  private resolveNextNode;
  private completeFlowRun;
  private recordFlowStepOutcome;
  private executeNode;
  private executeCallbackNode;
  private executeShellNode;
  private shellControl;
  private runCallbackShell;
  private executeAcpNode;
  private prepareAcpPrompt;
  private writeAcpPromptHeartbeat;
  private executeIsolatedAcpPrompt;
  private initializeIsolatedSessionBundle;
  private executePersistentAcpPrompt;
  private appendAcpPromptPreparedTrace;
  private finishAcpPrompt;
  private publishAcpCapture;
  private appendAcpResponseParsedTrace;
  private parseAcpOutput;
  private startHeartbeat;
  private ensureSessionBinding;
  private createPromptEventCapture;
  private runPersistentPrompt;
  private closePendingPersistentSessionClients;
  private runIsolatedPrompt;
}
//#endregion
//#region src/flows/decision.d.ts
type DecisionAcpOptions = Omit<AcpNodeDefinition, "nodeType" | "prompt" | "parse">;
type DecisionDefinition<TChoice extends string> = DecisionAcpOptions & {
  question: string | ((context: FlowNodeContext) => string | Promise<string>);
  choices: readonly TChoice[];
  field?: string;
};
export declare function decision<TChoice extends string>(definition: DecisionDefinition<TChoice>): AcpNodeDefinition;
export declare function decisionEdge<TChoice extends string>(args: {
  from: string;
  choices: readonly TChoice[];
  field?: string;
  cases: Record<TChoice, string>;
}): FlowEdge;
//#endregion
//#region src/flows/store.d.ts
export declare function flowRunsBaseDir(homeDir?: string): string;
//#endregion
//#region src/flows/json.d.ts
type JsonObjectParseMode = "strict" | "fenced" | "compat";
export declare function parseJsonObject(text: string, options?: {
  mode?: JsonObjectParseMode;
}): unknown;
export declare function parseStrictJsonObject(text: string): unknown;
export declare function extractJsonObject(text: string): unknown;
//#endregion
export type { AcpNodeDefinition, ActionNodeDefinition, CheckpointNodeDefinition, ComputeNodeDefinition, DecisionDefinition, FlowDefinition, FlowEdge, FlowNodeCommon, FlowNodeContext, FlowNodeDefinition, FlowPermissionRequirements, FlowRunDefinition, FlowRunResult, FlowRunState, FlowRunnerOptions, FlowSessionBinding, FlowShellExecution, FlowShellResult, FlowStepRecord, FunctionActionNodeDefinition, JsonObjectParseMode, ResolvedFlowAgent, ShellActionExecution, ShellActionNodeDefinition, ShellActionResult };
//# sourceMappingURL=flows.d.ts.map