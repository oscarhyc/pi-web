/**
 * RemoteAgentTransport — pi-web frontend ↔ pi-agent-bridge (over TCP/Unix socket).
 *
 * This is a faithful port of the SDK's RpcClient (lib/modes/rpc/rpc-client.js)
 * to a network transport. It speaks the same JSONL-over-LF framing as
 * `pi --mode rpc`, but instead of spawning the agent as a stdio subprocess it
 * connects to a bridge process that owns the subprocess.
 *
 * Wire shape (one JSON object per line, terminated by '\n'):
 *   S→C  {"type":"hello","protocol":1,...}
 *   C→S  {"type":"auth","token":"..."}
 *   S→C  {"type":"auth_ok"|"auth_required"|"auth_error"}
 *   C→S  {"type":"init","cwd":"/abs","sessionId":"<id>"|null,"toolNames":string[]|null}
 *   S→C  {"type":"init_ok","sessionId":"...","sessionFile":"...","pid":n}
 *
 *   C→S  <agent RPC command, see pi-coding-agent's RpcCommand>
 *   S→C  <agent RPC response or JsonAgentSessionEvent>
 *
 *   Bridge-only commands surfaced as ordinary RPC-shaped responses:
 *     {"type":"bridge.list_sessions"}
 *     {"type":"bridge.read_file","path":"...","encoding":"utf8"|null,"maxBytes":n}
 *     {"type":"bridge.stat","path":"..."}
 *     {"type":"bridge.readdir","path":"..."}
 *     {"type":"bridge.tail_session","sessionFile":"...","maxBytes":n}
 *     {"type":"bridge.export_session","sessionFile":"...","format":"html"}
 *     {"type":"bridge.ping"}
 *
 * Reconnect behaviour: by default this client is connection-owning. Each
 * instance keeps one socket for its lifetime; if the socket dies the client
 * rejects pending requests and surfaces `close`. Callers are expected to
 * construct a new instance per session / per retry attempt (the in-app
 * lifecycle is session-scoped; idle sessions are torn down after 10 minutes
 * anyway — see lib/rpc-manager.ts).
 */
import { createConnection, type Socket } from "node:net";
import { StringDecoder } from "node:string_decoder";
import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { ImageContent } from "@earendil-works/pi-ai";
import type { SessionStats, SessionEntry, SessionTreeNode } from "@earendil-works/pi-coding-agent";
// BashResult and CompactionResult aren't exported from the SDK entry; pull them
// from the dist/core path which the SDK package ships.
type BashResult = { output: string; exitCode?: number; cancelled?: boolean; truncated?: boolean; fullOutputPath?: string };
type CompactionResult = unknown;

// ============================================================================
// Public types
// ============================================================================

export interface RemoteAgentTransportOptions {
	/** "tcp://host:port", "unix:/path/to/socket", or a parsed object */
	url: string | { kind: "tcp"; host: string; port: number } | { kind: "unix"; path: string };
	token: string;
	cwd: string;
	sessionId?: string | null;
	toolNames?: string[] | null;
	connectTimeoutMs?: number;
	requestTimeoutMs?: number;
}

export type RpcEvent = { type: string; [k: string]: unknown };

export type RpcEventListener = (event: RpcEvent) => void;

export interface RpcSessionState {
	sessionId: string;
	sessionFile?: string;
	model?: { id: string; provider: string } | null;
	thinkingLevel: ThinkingLevel;
	isStreaming: boolean;
	isCompacting: boolean;
	autoCompactionEnabled?: boolean;
	messageCount?: number;
	pendingMessageCount?: number;
}

export interface SessionListingEntry {
	sessionFile: string;
	sessionId: string | null;
	cwd: string;
	startedAt: string | null;
	lastModifiedMs: number;
	sizeBytes: number;
	entryEstimate: number;
}

export interface BridgePing {
	pong: true;
	uptime: number;
	pid: number;
}

export class RemoteAgentError extends Error {
	constructor(message: string, public readonly cause?: unknown) {
		super(message);
		this.name = "RemoteAgentError";
	}
}

// ============================================================================
// URL parsing
// ============================================================================
function parseUrl(url: RemoteAgentTransportOptions["url"]):
	| { kind: "tcp"; host: string; port: number }
	| { kind: "unix"; path: string } {
	if (typeof url !== "string") return url;
	const s = url.trim();
	if (s.startsWith("unix:")) return { kind: "unix", path: s.slice("unix:".length).trim() };
	if (s.startsWith("tcp://")) {
		const rest = s.slice("tcp://".length);
		const lastColon = rest.lastIndexOf(":");
		if (lastColon === -1) {
			throw new RemoteAgentError(`Invalid tcp URL: ${s} (expected tcp://host:port)`);
		}
		const host = rest.slice(0, lastColon);
		const port = Number(rest.slice(lastColon + 1));
		if (!Number.isInteger(port) || port < 1 || port > 65535) {
			throw new RemoteAgentError(`Invalid port in URL: ${s}`);
		}
		return { kind: "tcp", host, port };
	}
	throw new RemoteAgentError(`Unsupported agent URL scheme: ${s}`);
}

// ============================================================================
// JSONL framing (mirrors pi-coding-agent/dist/modes/rpc/jsonl.js)
// ============================================================================
function serializeJsonLine(value: unknown): string {
	return JSON.stringify(value) + "\n";
}

function attachLineReader(stream: NodeJS.ReadableStream, onLine: (line: string) => void): () => void {
	const decoder = new StringDecoder("utf8");
	let buffer = "";
	const emit = (line: string) => onLine(line.endsWith("\r") ? line.slice(0, -1) : line);
	const onData = (chunk: Buffer | string) => {
		buffer += typeof chunk === "string" ? chunk : decoder.write(chunk);
		for (;;) {
			const i = buffer.indexOf("\n");
			if (i === -1) return;
			emit(buffer.slice(0, i));
			buffer = buffer.slice(i + 1);
		}
	};
	const onEnd = () => {
		buffer += decoder.end();
		if (buffer.length > 0) emit(buffer);
	};
	stream.on("data", onData);
	stream.on("end", onEnd);
	return () => {
		stream.off("data", onData);
		stream.off("end", onEnd);
	};
}

// ============================================================================
// Rpc command union (subset that pi-web actually issues — keeps the file small)
// See pi-coding-agent/dist/modes/rpc/rpc-types.d.ts for the canonical types.
// ============================================================================
type RpcCommandBody =
	| { type: "prompt"; message: string; images?: ImageContent[]; streamingBehavior?: "steer" | "followUp" }
	| { type: "steer"; message: string; images?: ImageContent[] }
	| { type: "follow_up"; message: string; images?: ImageContent[] }
	| { type: "abort" }
	| { type: "new_session"; parentSession?: string }
	| { type: "get_state" }
	| { type: "set_model"; provider: string; modelId: string }
	| { type: "set_thinking_level"; level: ThinkingLevel }
	| { type: "compact"; customInstructions?: string }
	| { type: "set_auto_compaction"; enabled: boolean }
	| { type: "set_auto_retry"; enabled: boolean }
	| { type: "bash"; command: string; excludeFromContext?: boolean }
	| { type: "abort_bash" }
	| { type: "get_session_stats" }
	| { type: "get_last_assistant_text" }
	| { type: "set_session_name"; name: string }
	| { type: "switch_session"; sessionPath: string }
	| { type: "fork"; entryId: string }
	| { type: "get_available_models" }
	| { type: "get_available_thinking_levels" }
	| { type: "get_commands" };

type RpcResponse =
	| { id?: string; type: "response"; command: string; success: true; data?: unknown }
	| { id?: string; type: "response"; command: string; success: false; error: string };

// ============================================================================
// Client
// ============================================================================
export class RemoteAgentTransport {
	private socket: Socket | null = null;
	private connected = false;
	private authed = false;
	private initialized = false;
	private events: RpcEventListener[] = [];
	private pending = new Map<string, { resolve: (r: RpcResponse) => void; reject: (e: Error) => void; command: string }>();
	private stderr = "";
	private bridgeError: Error | null = null;
	private nextRequestId = 1;
	private hostInfo: { kind: "tcp"; host: string; port: number } | { kind: "unix"; path: string };
	private stopReader: (() => void) | null = null;
	private boundSessionId: string | null = null;
	private boundCwd: string;

	constructor(private readonly opts: RemoteAgentTransportOptions) {
		this.hostInfo = parseUrl(opts.url);
		this.boundCwd = opts.cwd;
	}

	get cwd(): string { return this.boundCwd; }
	get sessionId(): string | null { return this.boundSessionId; }
	get isConnected(): boolean { return this.connected && this.initialized; }

	async start(): Promise<void> {
		if (this.socket) throw new RemoteAgentError("Client already started");
		await this.connect();
		await this.authenticate();
		await this.initialize();
	}

	async stop(): Promise<void> {
		if (!this.socket) return;
		this.stopReader?.();
		this.stopReader = null;
		try { this.socket.end(); } catch {}
		await new Promise<void>((resolve) => {
			const t = setTimeout(() => { try { this.socket?.destroy(); } catch {}; resolve(); }, 1500);
			this.socket?.once("close", () => { clearTimeout(t); resolve(); });
			this.socket?.once("error", () => { clearTimeout(t); resolve(); });
		});
		this.socket = null;
		this.connected = false;
		this.authed = false;
		this.initialized = false;
		this.rejectAllPending(new RemoteAgentError("client_stopped"));
	}

	onEvent(listener: RpcEventListener): () => void {
		this.events.push(listener);
		return () => {
			const i = this.events.indexOf(listener);
			if (i !== -1) this.events.splice(i, 1);
		};
	}

	getStderr(): string { return this.stderr; }

	// ----- typed command surface ----------------------------------------------
	async prompt(message: string, images?: ImageContent[], streamingBehavior?: "steer" | "followUp"): Promise<void> {
		await this.send({ type: "prompt", message, images, streamingBehavior });
	}
	async steer(message: string, images?: ImageContent[]): Promise<void> {
		await this.send({ type: "steer", message, images });
	}
	async followUp(message: string, images?: ImageContent[]): Promise<void> {
		await this.send({ type: "follow_up", message, images });
	}
	async abort(): Promise<void> { await this.send({ type: "abort" }); }

	async getState(): Promise<RpcSessionState> {
		const resp = await this.send({ type: "get_state" });
		return this.unwrapData<RpcSessionState>(resp, "get_state");
	}
	async setModel(provider: string, modelId: string): Promise<{ id: string; provider: string }> {
		const resp = await this.send({ type: "set_model", provider, modelId });
		const data = this.unwrapData<{ id: string; provider: string }>(resp, "set_model");
		this.boundSessionId = data.id || this.boundSessionId; // sessionId is also reported as model.id in some shapes
		return data;
	}
	async setThinkingLevel(level: ThinkingLevel): Promise<void> {
		await this.send({ type: "set_thinking_level", level });
	}
	async compact(customInstructions?: string): Promise<CompactionResult> {
		const resp = await this.send({ type: "compact", customInstructions });
		return this.unwrapData<CompactionResult>(resp, "compact");
	}
	async setAutoCompaction(enabled: boolean): Promise<void> {
		await this.send({ type: "set_auto_compaction", enabled });
	}
	async setAutoRetry(enabled: boolean): Promise<void> {
		await this.send({ type: "set_auto_retry", enabled });
	}
	async bash(command: string, excludeFromContext?: boolean): Promise<BashResult> {
		const resp = await this.send({ type: "bash", command, excludeFromContext });
		return this.unwrapData<BashResult>(resp, "bash");
	}
	async abortBash(): Promise<void> { await this.send({ type: "abort_bash" }); }
	async getSessionStats(): Promise<SessionStats> {
		const resp = await this.send({ type: "get_session_stats" });
		return this.unwrapData<SessionStats>(resp, "get_session_stats");
	}
	async getLastAssistantText(): Promise<string | null> {
		const resp = await this.send({ type: "get_last_assistant_text" });
		const data = this.unwrapData<{ text: string | null }>(resp, "get_last_assistant_text");
		return data?.text ?? null;
	}
	async setSessionName(name: string): Promise<void> {
		await this.send({ type: "set_session_name", name });
	}
	async switchSession(sessionPath: string): Promise<{ cancelled: boolean }> {
		const resp = await this.send({ type: "switch_session", sessionPath });
		const data = this.unwrapData<{ cancelled: boolean }>(resp, "switch_session");
		this.boundSessionId = null; // we'll re-learn via getState
		return data;
	}
	async fork(entryId: string): Promise<{ text: string; cancelled: boolean; newSessionFile?: string }> {
		const resp = await this.send({ type: "fork", entryId });
		return this.unwrapData<{ text: string; cancelled: boolean; newSessionFile?: string }>(resp, "fork");
	}
	async getAvailableModels(): Promise<Array<{ provider: string; id: string }>> {
		const resp = await this.send({ type: "get_available_models" });
		const data = this.unwrapData<{ models: Array<{ provider: string; id: string }> }>(resp, "get_available_models");
		return data.models;
	}
	async getAvailableThinkingLevels(): Promise<ThinkingLevel[]> {
		const resp = await this.send({ type: "get_available_thinking_levels" });
		const data = this.unwrapData<{ levels: ThinkingLevel[] }>(resp, "get_available_thinking_levels");
		return data.levels;
	}
	async getCommands(): Promise<Array<{ name: string; description?: string; source: string }>> {
		const resp = await this.send({ type: "get_commands" });
		const data = this.unwrapData<{ commands: Array<{ name: string; description?: string; source: string }> }>(
			resp,
			"get_commands",
		);
		return data.commands;
	}

	// ----- bridge-level commands ----------------------------------------------
	async bridgePing(): Promise<BridgePing> {
		return this.bridgeRpc<BridgePing>("bridge.ping", {});
	}
	async bridgeListSessions(): Promise<SessionListingEntry[]> {
		const data = await this.bridgeRpc<{ sessions: SessionListingEntry[] }>("bridge.list_sessions", {});
		return data.sessions;
	}
	async bridgeReadFile(path: string, encoding: "utf8" | null = "utf8", maxBytes = 1024 * 1024):
		Promise<{ bytes: number; truncated: boolean; text: string | null; base64?: string }> {
		return this.bridgeRpc("bridge.read_file", { path, encoding, maxBytes });
	}
	async bridgeStat(path: string): Promise<{ exists: boolean; isFile?: boolean; isDirectory?: boolean; size?: number; mtimeMs?: number }> {
		return this.bridgeRpc("bridge.stat", { path });
	}
	async bridgeReaddir(path: string): Promise<Array<{ name: string; isDir: boolean }>> {
		const data = await this.bridgeRpc<{ entries: Array<{ name: string; isDir: boolean }> }>("bridge.readdir", { path });
		return data.entries;
	}
	async bridgeTailSession(sessionFile: string, maxBytes = 4 * 1024 * 1024): Promise<{ path: string; text: string; bytes: number }> {
		return this.bridgeRpc("bridge.tail_session", { sessionFile, maxBytes });
	}
	async bridgeExportSession(sessionFile: string): Promise<{ html: string; path: string }> {
		return this.bridgeRpc("bridge.export_session", { sessionFile, format: "html" });
	}

	// ============================================================================
	// Internal
	// ============================================================================

	private async connect(): Promise<void> {
		const timeoutMs = this.opts.connectTimeoutMs ?? 10_000;
		await new Promise<void>((resolve, reject) => {
			const sock = this.hostInfo.kind === "tcp"
				? createConnection({ host: this.hostInfo.host, port: this.hostInfo.port })
				: createConnection(this.hostInfo.path);
			this.socket = sock;

			const onTimeout = () => {
				sock.destroy();
				reject(new RemoteAgentError(`connect timeout after ${timeoutMs}ms`));
			};
			const timer = setTimeout(onTimeout, timeoutMs);

			const onConnect = () => {
				clearTimeout(timer);
				sock.off("error", onError);
				this.connected = true;
				this.stopReader = attachLineReader(sock, (line) => this.handleLine(line));
				sock.on("error", (e) => {
					this.stderr += `${e.message}\n`;
					if (this.bridgeError) return;
					this.bridgeError = new RemoteAgentError(`socket error: ${e.message}`);
					this.rejectAllPending(this.bridgeError);
				});
				sock.on("close", () => this.handleClose());
				resolve();
			};
			const onError = (e: Error) => {
				clearTimeout(timer);
				sock.destroy();
				reject(new RemoteAgentError(`connect failed: ${e.message}`));
			};
			sock.once("connect", onConnect);
			sock.once("error", onError);
		});
	}

	private async authenticate(): Promise<void> {
		const hello = await this.waitForLineOfType("hello");
		const serverProto = (hello as { protocol?: number }).protocol;
		if (serverProto !== 1) {
			throw new RemoteAgentError(`unsupported bridge protocol ${serverProto}`);
		}
		this.writeRaw({ type: "auth", token: this.opts.token });
		const ack = await this.waitForLineMatching((m) => m.type === "auth_ok" || m.type === "auth_error", 5_000);
		if (ack.type !== "auth_ok") {
			throw new RemoteAgentError(`auth failed: ${(ack as { error?: string }).error || "unauthorized"}`);
		}
		this.authed = true;
	}

	private async initialize(): Promise<void> {
		const initMsg = {
			type: "init" as const,
			cwd: this.boundCwd,
			sessionId: this.opts.sessionId ?? null,
			toolNames: this.opts.toolNames ?? null,
		};
		this.writeRaw(initMsg);
		const ack = await this.waitForLineMatching((m) => m.type === "init_ok" || m.type === "init_error", 10_000);
		if (ack.type !== "init_ok") {
			throw new RemoteAgentError(`init failed: ${(ack as { error?: string }).error || "unknown"}`);
		}
		const sid = (ack as { sessionId?: string | null }).sessionId;
		if (sid) this.boundSessionId = sid;
		this.initialized = true;

		// Probe sessionId/state for clients that didn't get it from init
		try {
			const state = await this.getState();
			this.boundSessionId = state.sessionId || this.boundSessionId;
		} catch { /* tolerate */ }
	}

	private handleLine(line: string): void {
		if (!line) return;
		let obj: unknown;
		try { obj = JSON.parse(line); } catch { return; }
		const m = obj as { type?: string; id?: string };
		if (m.type === "response") {
			const r = obj as RpcResponse;
			if (!r.id) return;
			const p = this.pending.get(r.id);
			if (!p) return;
			this.pending.delete(r.id);
			if (r.success) p.resolve(r);
			else p.reject(new RemoteAgentError(`${p.command}: ${r.error}`));
			return;
		}
		if (m.type === "agent_dead" || m.type === "close") {
			const err = new RemoteAgentError(
				(m as { error?: string }).error ?? (m.type === "close" ? "bridge_closed" : "agent_died"),
			);
			this.bridgeError = err;
			this.rejectAllPending(err);
			return;
		}
		// All other lines (events, extension_ui_request, etc.) are broadcast to listeners
		for (const l of this.events) {
			try { l(obj as RpcEvent); } catch {}
		}
	}

	private handleClose(): void {
		this.connected = false;
		this.authed = false;
		this.initialized = false;
		const err = this.bridgeError ?? new RemoteAgentError("socket_closed");
		this.rejectAllPending(err);
	}

	private rejectAllPending(err: Error): void {
		for (const p of this.pending.values()) p.reject(err);
		this.pending.clear();
	}

	private writeRaw(obj: unknown): void {
		if (!this.socket || this.socket.destroyed || !this.socket.writable) {
			throw new RemoteAgentError("socket not writable");
		}
		this.socket.write(serializeJsonLine(obj));
	}

	private async send(body: RpcCommandBody): Promise<RpcResponse> {
		if (!this.isConnected) throw new RemoteAgentError("not_connected");
		const id = `req_${this.nextRequestId++}`;
		const timeoutMs = this.opts.requestTimeoutMs ?? 30_000;
		const full = { ...body, id } as object;
		return new Promise<RpcResponse>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.pending.delete(id);
				reject(new RemoteAgentError(`timeout for ${body.type}`));
			}, timeoutMs);
			this.pending.set(id, {
				resolve: (r) => { clearTimeout(timer); resolve(r); },
				reject: (e) => { clearTimeout(timer); reject(e); },
				command: body.type,
			});
			try {
				this.writeRaw(full);
			} catch (e) {
				clearTimeout(timer);
				this.pending.delete(id);
				reject(e instanceof Error ? e : new RemoteAgentError(String(e)));
			}
		});
	}

	private async bridgeRpc<T>(command: string, payload: object): Promise<T> {
		// Bridge-only commands ride the same RPC channel; we synthesize a
		// response object in the standard {type:"response", success, data|error} shape.
		// We bypass the public send() to talk directly through the wire because
		// the command names start with "bridge." and the regular path will work
		// as-is for response correlation; the agent won't see them (bridge intercepts).
		const id = `req_${this.nextRequestId++}`;
		const timeoutMs = this.opts.requestTimeoutMs ?? 30_000;
		const resp = await new Promise<RpcResponse>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.pending.delete(id);
				reject(new RemoteAgentError(`timeout for ${command}`));
			}, timeoutMs);
			this.pending.set(id, {
				resolve: (r) => { clearTimeout(timer); resolve(r); },
				reject: (e) => { clearTimeout(timer); reject(e); },
				command,
			});
			try {
				this.writeRaw({ id, type: command, ...payload });
			} catch (e) {
				clearTimeout(timer);
				this.pending.delete(id);
				reject(e instanceof Error ? e : new RemoteAgentError(String(e)));
			}
		});
		return this.unwrapData<T>(resp, command);
	}

	private unwrapData<T>(resp: RpcResponse, command: string): T {
		if (!resp.success) {
			const err = "error" in resp ? (resp as { error: string }).error : "unknown_error";
			throw new RemoteAgentError(`${command}: ${err}`);
		}
		const r = resp as Extract<RpcResponse, { success: true; data?: unknown }>;
		return (r.data as T);
	}

	private waitForLineOfType(type: string, timeoutMs = 5_000): Promise<Record<string, unknown>> {
		return this.waitForLineMatching((m) => m.type === type, timeoutMs);
	}

	private waitForLineMatching(pred: (m: { type?: string }) => boolean, timeoutMs: number):
		Promise<{ type?: string;[k: string]: unknown }> {
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				this.events.push(pushListener);
				reject(new RemoteAgentError(`timeout waiting for line matching predicate (${timeoutMs}ms)`));
			}, timeoutMs);
			const pushListener = (m: { type?: string;[k: string]: unknown }) => {
				if (pred(m)) {
					clearTimeout(timer);
					const i = this.events.indexOf(pushListener);
					if (i !== -1) this.events.splice(i, 1);
					resolve(m);
				}
			};
			this.events.push(pushListener);
		});
	}
}

// ============================================================================
// Re-export event/result types for convenience
// ============================================================================
export type { AgentMessage, ThinkingLevel, ImageContent, SessionStats, BashResult, SessionEntry, SessionTreeNode };
export type RemoteRpcEvent = RpcEvent;
