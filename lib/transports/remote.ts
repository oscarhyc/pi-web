/**
 * RemoteTransport — pi-web frontend ↔ pi-agent-bridge.
 *
 * The bridge lives on a separate machine (the "agent machine") and owns one
 * `pi --mode rpc` subprocess per pi-web session. This module constructs one
 * RemoteAgentTransport per session, translates RPC manager commands into the
 * JSONL protocol the bridge speaks, and reflects incoming events back through
 * the wrapper interface.
 */

import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import { randomUUID } from "node:crypto";
import {
	RemoteAgentTransport,
	type ImageContent,
	type RpcSessionState,
} from "../remote-agent-transport";
import { persistExplicitStartupPreferences } from "../startup-preferences";
import type { AgentEvent, RpcSessionStartOptions } from "../rpc-manager";
import type { SessionTransport, TransportHandle } from ".";

type State = RpcSessionState & {
	isPromptRunning?: boolean;
	isBashRunning?: boolean;
	autoRetryEnabled?: boolean;
	pendingMessageCount?: number;
};

interface TransportSnapshot {
	sessionId: string;
	sessionFile: string;
	isStreaming: boolean;
	isPromptRunning?: boolean;
	isBashRunning?: boolean;
	isCompacting?: boolean;
	model?: { id: string; provider: string } | null;
	thinkingLevel?: import("@earendil-works/pi-agent-core").ThinkingLevel;
	autoCompactionEnabled?: boolean;
	autoRetryEnabled?: boolean;
	messageCount?: number;
	pendingMessageCount?: number;
}

interface RemoteTransportInit {
	url: string;
	token: string;
}

export function createRemoteTransport(init: RemoteTransportInit): SessionTransport {
	return {
		async acquire(opts) {
			const client = new RemoteAgentTransport({
				url: init.url,
				token: init.token,
				cwd: opts.cwd,
				sessionId: opts.sessionId ?? null,
				toolNames: opts.toolNames ?? null,
				connectTimeoutMs: 10_000,
				requestTimeoutMs: 60_000,
			});
			await client.start();
			return new RemoteHandle(client, opts);
		},
	};
}

type _UnusedState = RpcSessionState;

class RemoteHandle implements TransportHandle {
	private listeners: Array<(e: AgentEvent) => void> = [];
	private promptInFlight = 0;
	private lastSnapshot: State | null = null;
	private alive = true;
	private unsubscribe: (() => void) | null = null;

	constructor(
		public readonly client: RemoteAgentTransport,
		private readonly opts: {
			cwd: string;
			initialModel?: RpcSessionStartOptions["initialModel"];
			thinkingLevel?: ThinkingLevel;
		},
	) {
		this.unsubscribe = this.client.onEvent((event) => {
			// Translate pi's raw JsonAgentSessionEvent into the AgentEvent shape
			// pi-web's hooks expect.
			this.lastSnapshot = mergeSnapshot(this.lastSnapshot, event);
			const mapped = adaptEvent(event);
			if (mapped) {
				for (const l of this.listeners) {
					try { l(mapped); } catch (err) { console.error("[remote-transport] listener error:", err); }
				}
			}
		});
	}

	get sessionId(): string {
		return this.client.sessionId || this.lastSnapshot?.sessionId || "";
	}
	get sessionFile(): string {
		return this.lastSnapshot?.sessionFile ?? "";
	}
	get cwd(): string { return this.opts.cwd; }

	onEvent(listener: (e: AgentEvent) => void): () => void {
		this.listeners.push(listener);
		return () => {
			const i = this.listeners.indexOf(listener);
			if (i !== -1) this.listeners.splice(i, 1);
		};
	}

	async snapshot(): Promise<TransportSnapshot> {
		const state = await this.client.getState();
		const merged = { ...(this.lastSnapshot ?? ({} as State)), ...state } as TransportSnapshot;
		// sessionFile is required by TransportHandle; default to empty string when absent.
		this.lastSnapshot = merged as State;
		return { ...merged, sessionFile: merged.sessionFile ?? "" };
	}

	async send(command: Record<string, unknown>): Promise<unknown> {
		const type = command.type as string;
		switch (type) {
			case "prompt": {
				this.promptInFlight += 1;
				const id = randomUUID();
				try {
					await this.client.prompt(
						command.message as string,
						command.images as ImageContent[] | undefined,
						command.streamingBehavior as "steer" | "followUp" | undefined,
					);
				} catch (e) {
					this.promptInFlight = Math.max(0, this.promptInFlight - 1);
					throw e;
				}
				// We don't await completion here; the rpc-manager translates these
				// into "preflightResult"-style acks by emitting "prompt_done" on agent_end.
				queueMicrotask(() => {
					setTimeout(() => {
						this.promptInFlight = Math.max(0, this.promptInFlight - 1);
						this.emit({ type: "prompt_done" });
					}, 0);
				});
				return { accepted: true, id };
			}
			case "steer":
				await this.client.steer(command.message as string, command.images as ImageContent[] | undefined);
				return null;
			case "follow_up":
				await this.client.followUp(command.message as string, command.images as ImageContent[] | undefined);
				return null;
			case "abort":
				await this.client.abort();
				return null;
			case "get_state":
				return await this.snapshot();
			case "set_model": {
				const { provider, modelId } = command as { provider: string; modelId: string };
				return await this.client.setModel(provider, modelId);
			}
			case "set_thinking_level":
				await this.client.setThinkingLevel(command.level as ThinkingLevel);
				return null;
			case "compact":
				return await this.client.compact(command.customInstructions as string | undefined);
			case "set_session_name":
				await this.client.setSessionName((command.name as string).trim());
				return null;
			case "get_session_stats":
				return await this.client.getSessionStats();
			case "get_last_assistant_text": {
				const text = await this.client.getLastAssistantText();
				return { text: text ?? "" };
			}
			case "set_auto_compaction":
				await this.client.setAutoCompaction(command.enabled as boolean);
				return null;
			case "set_auto_retry":
				await this.client.setAutoRetry(command.enabled as boolean);
				return null;
			case "clear_queue":
				// RPC has no direct clear_queue; emulate by steer+abort pair
				try { await this.client.abort(); } catch {}
				return null;
			case "steer":
				// handled above
				return null;
			case "follow_up":
				// handled above
				return null;
			case "set_tools":
				// Not supported in RPC; require wrapper to respawn.
				throw new Error(
					"set_tools is not supported over RPC — the session must be respawned with new toolNames",
				);
			case "get_tools":
				throw new Error("get_tools over RPC not implemented; surface UI as derived from session state");
			case "reload":
				throw new Error("reload over RPC not implemented; respawn the session instead");
			case "fork": {
				const entryId = command.entryId as string;
				const r = await this.client.fork(entryId);
				// Match the rpc-manager fork contract: `{cancelled, newSessionId}`
				const newSessionId = r.text ?? null;
				return { cancelled: !!r.cancelled, newSessionId };
			}
			case "navigate_tree":
				throw new Error(
					"navigate_tree is not supported over RPC — fork to a new session at the desired leaf instead",
				);
			case "bash":
				return await this.client.bash(command.command as string, command.excludeFromContext as boolean | undefined);
			case "abort_bash":
				await this.client.abortBash();
				return null;
			case "extension_ui_response":
				// pi-web serializes the response back through the same wire — we just
				// forward via a raw protocol write.
				this.client["writeRaw"]?.({
					type: "extension_ui_response",
					id: command.id,
					value: command.value,
					confirmed: command.confirmed,
					cancelled: command.cancelled,
				});
				return null;
			case "extension_ui_input":
				this.client["writeRaw"]?.({
					type: "extension_ui_input",
					id: command.id,
					data: command.data,
				});
				return null;
			default:
				throw new Error(`Unsupported remote command: ${type}`);
		}
	}

	async dispose(): Promise<void> {
		if (!this.alive) return;
		this.alive = false;
		try { this.unsubscribe?.(); } catch {}
		// Note: persistExplicitStartupPreferences needs a SettingsManager handle,
		// which we don't have in remote mode. The bridge owns settings persistence
		// implicitly via its own session spawn (--provider / --model flags),
		// so this is intentionally a no-op here.
		try { await this.client.stop(); } catch {}
	}

	private emit(e: AgentEvent): void {
		for (const l of this.listeners) {
			try { l(e); } catch {}
		}
	}
}

/**
 * Map a raw RPC event into AgentEvent expected by the rest of the fork.
 *
 * The names largely match; the only meaningful differences we hit:
 *   - JSON-RPC responses have type:"response", but the rest of the wrapper
 *     treats those as terminal data not as streaming events.
 *   - Extension UI requests come through untouched.
 *   - "agent_settled" maps to "agent_settled" (the SDK already uses the same name).
 */
function adaptEvent(raw: { type?: string;[k: string]: unknown }): AgentEvent | null {
	if (!raw || !raw.type) return null;
	if (raw.type === "response") return null; // handled by send()
	return raw as AgentEvent;
}

function mergeSnapshot(prev: State | null, event: { type?: string;[k: string]: unknown }): State | null {
	if (!event || typeof event !== "object") return prev;
	const e = event as { type?: string; sessionId?: string; sessionFile?: string;[k: string]: unknown };
	const next: State = { ...(prev ?? ({} as State)) };
	if (e.type === "session_init" || e.type === "agent_start") {
		if (typeof e.sessionId === "string") next.sessionId = e.sessionId;
		if (typeof e.sessionFile === "string") next.sessionFile = e.sessionFile;
	}
	return next;
}

// back-compat for the bridge protocol extension_ui_response path
