/**
 * RemoteAgentSessionLike — minimal AgentSessionLike implementation that
 * forwards everything to RemoteAgentTransport.
 *
 * This is the single piece that lets AgentSessionWrapper (lib/rpc-manager.ts)
 * continue to be used unmodified when the transport is remote. The wrapper
 * only ever calls into the AgentSessionLike interface; if every method routes
 * to RemoteAgentTransport correctly, the rest of pi-web is unaware that the
 * agent is now on another machine.
 *
 * Implementation strategy:
 *   - Real calls go through the transport (.prompt / .abort / .setModel / ...).
 *   - Things that don't make sense over RPC (bindExtensions, theme switching,
 *     UI dialogs, file watcher) return best-effort defaults so AgentSessionWrapper
 *     doesn't break.
 *   - sessionManager/sessionId/sessionFile track what the remote agent reports.
 */

import type {
	AgentSessionEvent,
	BashOperations,
} from "@earendil-works/pi-coding-agent";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type {
	AgentSessionLike,
	ContextUsage,
	ModelLike,
	NavigateTreeResult,
	SessionStatsInfo,
	ToolInfo,
} from "../pi-types";
import { RemoteAgentTransport, type ImageContent } from "../remote-agent-transport";

// Minimal stand-ins for the SDK's internal extension runner / resource loader.
// AgentSessionWrapper only checks for the existence of these on the
// `AgentSessionLike`; in remote mode they are always empty.
const noopRunner = {
	getRegisteredCommands: () => [] as Array<{ invocationName: string; description?: string; sourceInfo: { path: string; source: string; scope: string; origin: string } }>,
	emit: async (_e: { type: string }) => undefined,
	setUIContext: (_ui?: unknown, _mode?: "rpc") => undefined,
};
const noopResource = {
	getSkills: () => ({ skills: [] as Array<{ name: string; description?: string; sourceInfo: { path: string; source: string; scope: string; origin: string } }> }),
};

export function createRemoteAgentSessionLike(cwd: string, sessionId: string | null, client: RemoteAgentTransport): AgentSessionLike {
	let cachedSessionId = sessionId ?? "";
	let cachedSessionFile: string | undefined;
	let cachedIsStreaming = false;
	let cachedIsCompacting = false;
	let cachedModel: ModelLike | undefined;
	let cachedAutoCompaction = true;
	let cachedAutoRetry = false;
	let cachedThinkingLevel = "off";
	let cachedSystemPrompt = "";
	let cachedSteering: string[] = [];
	let cachedFollowUp: string[] = [];

	const eventSubs = new Set<(e: AgentSessionEvent) => void>();
	client.onEvent((event) => {
		// Translate raw SDK json-event shape into AgentSessionEvent fields used
		// by AgentSessionWrapper. The SDK already produces AgentSessionEvent-shaped
		// values internally; our transport just relays them.
		const e = event as { type?: string;[k: string]: unknown };
		if (e.type === "agent_start") cachedIsStreaming = true;
		if (e.type === "agent_end" || e.type === "agent_settled") cachedIsStreaming = false;
		if (e.type === "compaction_start" || e.type === "auto_compaction_start") cachedIsCompacting = true;
		if (e.type === "compaction_end" || e.type === "auto_compaction_end") cachedIsCompacting = false;
		if (e.type === "session_init" && typeof e.sessionId === "string") {
			cachedSessionId = e.sessionId;
		}
		if (e.type === "session_init" && typeof e.sessionFile === "string") {
			cachedSessionFile = e.sessionFile;
		}
		if (e.type === "model_select" && e.model) {
			const m = e.model as { id?: string; provider?: string };
			if (m.id && m.provider) cachedModel = { id: m.id, provider: m.provider };
		}
		if (e.type === "thinking_level_change" && typeof e.level === "string") {
			cachedThinkingLevel = e.level as string;
		}
		for (const s of eventSubs) {
			try { s(e as unknown as AgentSessionEvent); } catch (err) { console.error("[remote] event sub error:", err); }
		}
	});

	const makeWriteStreamProxy = () => new PassThroughNoop();

	return {
		get sessionId() { return cachedSessionId; },
		get sessionFile() { return cachedSessionFile; },
		get isStreaming() { return cachedIsStreaming; },
		get isCompacting() { return cachedIsCompacting; },
		get autoCompactionEnabled() { return cachedAutoCompaction; },
		get autoRetryEnabled() { return cachedAutoRetry; },
		get model() { return cachedModel; },
		modelRuntime: {
			getModel: () => cachedModel,
			refresh: async () => undefined,
		},
		sessionManager: makeRemoteSessionManager(cwd, () => cachedSessionFile),
		settingsManager: makeRemoteSettingsManager(),
		agent: {
			get state() {
				return {
					systemPrompt: cachedSystemPrompt,
					thinkingLevel: cachedThinkingLevel,
					streamingMessage: undefined,
				};
			},
		},
		extensionRunner: noopRunner,
		promptTemplates: [],
		resourceLoader: noopResource,
		bindExtensions: undefined,
		dispose() { /* called by AgentSessionWrapper.destroy() */ },
		async reload() { /* no-op over RPC; respawn lifecycle instead */ },
		subscribe(listener) {
			eventSubs.add(listener);
			return () => { eventSubs.delete(listener); };
		},
		async prompt(text, options) {
			await client.prompt(text, options?.images as ImageContent[] | undefined, options?.streamingBehavior);
		},
		async abort() { await client.abort(); },
		async executeBash(command, _onChunk, options) {
			return await client.bash(command, options?.excludeFromContext);
		},
		abortBash() {
			// fire-and-forget; bridge handles abort
			void client.abortBash();
		},
		get isBashRunning() { return false; },
		async setModel(model) {
			await client.setModel(model.provider, model.id);
			cachedModel = { id: model.id, provider: model.provider };
		},
		async navigateTree(): Promise<NavigateTreeResult> {
			// Not supported over RPC; warn and bail
			console.warn("[remote] navigateTree not supported — caller should fork instead");
			return { cancelled: true, aborted: true };
		},
		setThinkingLevel(level) { cachedThinkingLevel = level; },
		async compact(customInstructions) { return await client.compact(customInstructions); },
		setSessionName(name) {
			void client.setSessionName(name);
		},
		getSessionStats() {
			// Lazy fetch would require an awaited call; for now return empty stats
			// The exact shape matches the SDK's SessionStatsInfo minus sessionName.
			return {
				sessionId: cachedSessionId,
				sessionFile: cachedSessionFile,
				userMessages: 0,
				assistantMessages: 0,
				toolCalls: 0,
				toolResults: 0,
				totalMessages: 0,
				tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				cost: 0,
			};
		},
		getLastAssistantText() { return ""; },
		setAutoCompactionEnabled(enabled) {
			cachedAutoCompaction = enabled;
			void client.setAutoCompaction(enabled);
		},
		setAutoRetryEnabled(enabled) {
			cachedAutoRetry = enabled;
			void client.setAutoRetry(enabled);
		},
		async steer(text, images) {
			await client.steer(text, images as ImageContent[] | undefined);
		},
		async followUp(text, images) {
			await client.followUp(text, images as ImageContent[] | undefined);
		},
		get pendingMessageCount() { return cachedSteering.length + cachedFollowUp.length; },
		getSteeringMessages() { return [...cachedSteering]; },
		getFollowUpMessages() { return [...cachedFollowUp]; },
		clearQueue() { const s = [...cachedSteering], f = [...cachedFollowUp]; cachedSteering = []; cachedFollowUp = []; return { steering: s, followUp: f }; },
		getAllTools() { return []; },
		getActiveToolNames() { return []; },
		setActiveToolsByName(_names) { /* no-op; respawn the session to change tools */ },
		abortCompaction() { /* best-effort: send abort to agent */ void client.abort(); },
		getContextUsage(): ContextUsage | undefined { return undefined; },
	};
}

function makeRemoteSessionManager(cwd: string, getFile: () => string | undefined) {
	const shim = {
		getCwd: () => cwd,
		getSessionFile: () => getFile(),
		getSessionId: () => getFile() ?? "",
		getCwdAndFile: () => cwd,
		newSession: () => undefined,
		open: () => makeRemoteSessionManager(cwd, getFile),
		getHeader: () => undefined,
		getEntries: () => [],
		getBranch: () => [],
		appendSessionInfo: () => undefined,
		getLeafId: () => null,
		getTree: () => [],
		isPersisted: () => !!getFile(),
		getSessionName: () => undefined,
		setSessionName: () => undefined,
		appendMessage: () => undefined,
		markFlushed: () => undefined,
		flushed: true,
	} as unknown;
	return shim;
}

function makeRemoteSettingsManager() {
	const shim = {
		getShellPath: () => "/bin/sh",
		getProjectTrusted: () => true,
		setProjectTrusted: () => undefined,
	} as unknown;
	return shim;
}

// Tiny internal helper — a no-op stream for executeBash's optional onChunk.
class PassThroughNoop {
	write(_chunk: string | Uint8Array): boolean { return true; }
	end(): void {}
	readonly destroyed = false;
	destroy(): void {}
}
