/**
 * InProcessTransport — default behaviour preserved.
 *
 * Bridges the existing AgentSessionWrapper (lib/rpc-manager.ts) into the
 * SessionTransport shape. This is what every user of pi-web gets today;
 * the remote transport is opt-in via PI_WEB_AGENT_URL.
 */

import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import { AgentSessionWrapper, type AgentEvent, type RpcSessionStartOptions } from "../rpc-manager";
import type { SessionTransport, TransportHandle } from ".";

export function createInProcessTransport(): SessionTransport {
	return {
		async acquire(opts) {
			const { createAgentSessionFromServices, createAgentSessionServices, getAgentDir } = await import(
				"@earendil-works/pi-coding-agent"
			);
			const agentDir = getAgentDir();
			const services = await createAgentSessionServices({ cwd: opts.cwd, agentDir });
			const session = await createAgentSessionFromServices({
				...services,
				sessionManager: opts.sessionId
					? undefined // SessionManager loaded inside SDK via cwd; --session is not a constructor arg today
					: undefined,
				// The wrapper still gets initialModel/thinkingLevel via rpc-manager.startRpcSession options.
			});
			// Wrap. We re-use the existing AgentSessionWrapper from rpc-manager.ts unchanged.
			// To keep this file self-contained without modifying rpc-manager.ts, we const-cast
			// through AgentSessionLike. Wrapper constructor signature is `(inner: AgentSessionLike)`.
			const wrapper = new AgentSessionWrapper(session as unknown as ConstructorParameters<typeof AgentSessionWrapper>[0]);
			wrapper.start();
			if (opts.initialModel || opts.thinkingLevel) {
				// Apply startup preferences immediately via the existing RPC contract.
				const cmds: Record<string, unknown>[] = [];
				if (opts.thinkingLevel) cmds.push({ type: "set_thinking_level", level: opts.thinkingLevel });
				// set_model is applied by ChatWindow upstream; we don't second-guess it here.
				for (const c of cmds) { try { await wrapper.send(c); } catch {} }
			}
			return handleFromWrapper(wrapper);
		},
	};
}

function handleFromWrapper(wrapper: AgentSessionWrapper): TransportHandle {
	return {
		get sessionId() { return wrapper.sessionId; },
		get sessionFile() { return wrapper.sessionFile; },
		get cwd() { return wrapper.cwd; },
		onEvent(listener) { return wrapper.onEvent(listener); },
		send(command) { return wrapper.send(command); },
		async snapshot() {
			const state = (await wrapper.send({ type: "get_state" })) as {
				sessionId: string; sessionFile: string; isStreaming: boolean;
				isPromptRunning: boolean; isBashRunning: boolean; isCompacting: boolean;
				autoCompactionEnabled: boolean; autoRetryEnabled: boolean;
				pendingMessageCount: number;
				model?: { id: string; provider: string };
				thinkingLevel: ThinkingLevel;
			};
			return state;
		},
		async dispose() { wrapper.destroy(); },
	};
}
