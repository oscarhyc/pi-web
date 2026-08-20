/**
 * SessionTransport — pluggable abstraction over "where the agent actually runs".
 *
 * The fork introduces this so the existing in-process AgentSessionWrapper can
 * be replaced by a remote-backed one without touching every callsite in
 * rpc-manager.ts. Two concrete implementations live in this folder:
 *
 *   - in-process.ts : uses the SDK's createAgentSessionFromServices directly
 *                     in the same Node process (default behaviour, preserved
 *                     for users who don't set PI_WEB_AGENT_URL).
 *   - remote.ts     : holds a RemoteAgentTransport connection per session and
 *                     translates every command to an RPC over TCP/Unix socket.
 *
 * The factory picks one based on environment variables at startup.
 */

import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { AgentSessionLike } from "../pi-types";
import type { AgentEvent, RpcSessionStartOptions } from "../rpc-manager";

export interface SessionTransport {
	/** Resolve a transport for a given cwd/sessionId. */
	acquire(opts: {
		cwd: string;
		sessionId?: string | null;
		toolNames?: string[];
		initialModel?: RpcSessionStartOptions["initialModel"];
		thinkingLevel?: ThinkingLevel;
	}): Promise<TransportHandle>;
}

export interface TransportHandle {
	sessionId: string;
	sessionFile: string;
	cwd: string;

	/** Subscribe to the full event stream. */
	onEvent(listener: (event: AgentEvent) => void): () => void;

	/** Send a command; resolve when the agent acknowledges / returns. */
	send(command: Record<string, unknown>): Promise<unknown>;

	/** Best-effort snapshot for /api/agent/[id] ?state=... and idle checks. */
	snapshot(): Promise<{
		sessionId: string;
		sessionFile: string;
		isStreaming: boolean;
		isPromptRunning?: boolean;
		isBashRunning?: boolean;
		isCompacting?: boolean;
		model?: { id: string; provider: string } | null;
		thinkingLevel?: ThinkingLevel;
		autoCompactionEnabled?: boolean;
		autoRetryEnabled?: boolean;
		messageCount?: number;
		pendingMessageCount?: number;
	}>;

	/** Tear down. */
	dispose(): Promise<void>;
}

export function selectTransportFromEnv(): { kind: "in-process" } | { kind: "remote"; url: string; token: string } {
	const url = process.env.PI_WEB_AGENT_URL;
	const token = process.env.PI_WEB_AGENT_TOKEN;
	if (url && token) return { kind: "remote", url, token };
	if (url && !token) {
		throw new Error("PI_WEB_AGENT_URL is set but PI_WEB_AGENT_TOKEN is missing");
	}
	return { kind: "in-process" };
}
