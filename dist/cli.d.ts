import { T as SessionRecord } from "./session-options-BNVRsaod.js";
import { Command } from "commander";
//#region src/cli/flags.d.ts
export declare function parseTtlSeconds(value: string): number;
export declare function parseAllowedTools(value: string): string[];
export declare function parseMaxTurns(value: string): number;
//#endregion
//#region src/cli/output/render.d.ts
type SessionConnectionStatus = "connected" | "starting" | "needs reconnect";
export declare function formatPromptSessionBannerLine(record: SessionRecord, currentCwd: string, connectionStatus?: SessionConnectionStatus): string;
//#endregion
export {};
//# sourceMappingURL=cli.d.ts.map