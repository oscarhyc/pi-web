#!/usr/bin/env node
"use strict";
/**
 * pi-agent-bridge
 *
 * TCP/Unix-socket bridge to a pi coding-agent RPC subprocess.
 * Lives on the "agent machine". Connects pi-web (frontend) to pi (agent).
 *
 * Protocol on the wire (LF-delimited JSON, one message per line):
 *
 *   S→C  {"type":"hello","protocol":1,"agent_pid":<n>,"agent_dir":"<path>"}
 *   C→S  {"type":"init","cwd":"<dir>","sessionId":"|"+optional,"toolNames":[...optional]}
 *   S→C  {"type":"init_ok","sessionId":"<id>","sessionFile":"<path>"} | {"type":"init_error","error":"..."}
 *
 *   C→S  <agent command, see @earendil-works/pi-coding-agent's RpcCommand type>
 *   S→C  <agent response or event, see JsonAgentSessionEvent / RpcResponse>
 *
 *   Bridge-only commands (processed by the bridge, NOT forwarded to the agent):
 *     {"type":"bridge.list_sessions"}
 *       → {"type":"response","id":...,"command":"bridge.list_sessions","data":{"sessions":[...]}}
 *     {"type":"bridge.read_file","path":"<abs>","encoding":"utf8"|null,"maxBytes":<n>}
 *       → {"type":"response","id":...,"command":"bridge.read_file","data":{"bytes":...,"text":"..."}}
 *        | {"type":"response",...,"success":false,"error":"..."}
 *     {"type":"bridge.stat","path":"<abs>"}
 *       → {"type":"response",...,"data":{"isFile":...,"size":...,"mtimeMs":...}}
 *     {"type":"bridge.readdir","path":"<abs>"}
 *       → {"type":"response",...,"data":{"entries":[{"name":"...","isDir":...}]}}
 *     {"type":"bridge.tail_session","sessionFile":"<abs>","maxBytes":<n>}
 *       → {"type":"response",...,"data":{"path":"...","text":"..."}}
 *     {"type":"bridge.export_session","sessionFile":"<abs>","format":"html"}
 *       → {"type":"response",...,"data":{"html":"...","path":"..."}}
 *     {"type":"bridge.ping"}
 *       → {"type":"response",...,"data":{"pong":true,"uptime":<s>,"pid":<n>}}
 *
 * Wire auth: a shared bearer token is read from env PI_AGENT_BRIDGE_TOKEN
 * (or from --token). The first line sent by the client MUST be
 *   {"type":"auth","token":"<token>"}
 * On mismatch the server closes the socket after sending
 *   {"type":"auth_error","error":"unauthorized"}.
 *
 * Security notes:
 *   - Token travels in cleartext. Use over a trusted LAN, SSH tunnel, or
 *     terminate TLS at a fronting reverse proxy (caddy/nginx).
 *   - Agent subprocess inherits a sanitized environment that strips
 *     PI_WEB_* secrets.
 *   - Each accepted connection runs with the OS user that started the bridge.
 *     There is no per-user sandboxing; you must trust whoever holds the token.
 */

const net = require("net");
const path = require("path");
const fs = require("fs");
const { spawn } = require("child_process");
const os = require("os");
const crypto = require("crypto");
const { StringDecoder } = require("string_decoder");

// ----------------------------------------------------------------------------
// CLI + env parsing
// ----------------------------------------------------------------------------
function parseArgs(argv) {
	const out = { port: 30142, host: "0.0.0.0", socketPath: null, cliPath: null, maxConns: 32 };
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === "--port" || a === "-p") out.port = Number(argv[++i]);
		else if (a === "--host" || a === "-H") out.host = String(argv[++i]);
		else if (a === "--socket") out.socketPath = String(argv[++i]);
		else if (a === "--cli") out.cliPath = String(argv[++i]);
		else if (a === "--max-conns") out.maxConns = Number(argv[++i]);
		else if (a === "--help" || a === "-h") {
			process.stdout.write(
				"Usage: pi-agent-bridge [--port <n>] [--host <ip>] [--socket <path>] [--cli <path-to-pi-cli>] [--max-conns <n>]\n" +
					"Env: PI_AGENT_BRIDGE_PORT, PI_AGENT_BRIDGE_HOST, PI_AGENT_BRIDGE_SOCKET,\n" +
					"     PI_AGENT_BRIDGE_TOKEN (required), PI_AGENT_BRIDGE_CLI\n",
			);
			process.exit(0);
		}
	}
	if (process.env.PI_AGENT_BRIDGE_PORT) out.port = Number(process.env.PI_AGENT_BRIDGE_PORT);
	if (process.env.PI_AGENT_BRIDGE_HOST) out.host = String(process.env.PI_AGENT_BRIDGE_HOST);
	if (process.env.PI_AGENT_BRIDGE_SOCKET) out.socketPath = String(process.env.PI_AGENT_BRIDGE_SOCKET);
	if (process.env.PI_AGENT_BRIDGE_CLI) out.cliPath = String(process.env.PI_AGENT_BRIDGE_CLI);
	return out;
}

function resolveCliPath(explicit) {
	if (explicit && fs.existsSync(explicit)) return explicit;
	// Probe common locations
	const candidates = [
		path.resolve(__dirname, "..", "..", "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js"),
		path.resolve(process.cwd(), "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js"),
		"/usr/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js",
	];
	for (const c of candidates) if (fs.existsSync(c)) return c;
	throw new Error(
		"Cannot locate @earendil-works/pi-coding-agent. Install it or pass --cli <path>.",
	);
}

// ----------------------------------------------------------------------------
// JSONL line reader/writer over a stream
// ----------------------------------------------------------------------------
function attachLineReader(stream, onLine) {
	const decoder = new StringDecoder("utf8");
	let buffer = "";
	const emit = (line) => onLine(line.endsWith("\r") ? line.slice(0, -1) : line);
	stream.on("data", (chunk) => {
		buffer += typeof chunk === "string" ? chunk : decoder.write(chunk);
		for (;;) {
			const i = buffer.indexOf("\n");
			if (i === -1) return;
			emit(buffer.slice(0, i));
			buffer = buffer.slice(i + 1);
		}
	});
	stream.on("end", () => {
		buffer += decoder.end();
		if (buffer.length > 0) emit(buffer);
	});
}

function writeLine(stream, obj) {
	// serializeJsonLine from the SDK uses \n. We mirror that.
	stream.write(JSON.stringify(obj) + "\n");
}

// ----------------------------------------------------------------------------
// Connection handler
// ----------------------------------------------------------------------------
const MAX_LINE_BYTES = 8 * 1024 * 1024; // 8 MB safety limit per line

class BridgeConnection {
	constructor(socket, opts) {
		this.socket = socket;
		this.opts = opts;
		this.buffer = "";
		this.decoder = new StringDecoder("utf8");
		this.agent = null;
		this.agentStdoutReader = null;
		this.authenticated = false;
		this.initialized = false;
		this.pendingInit = null; // resolve/reject for the init promise
		this.sessionId = null;
		this.cwd = null;
		this.remoteAddress = `${socket.remoteAddress}:${socket.remotePort}`;
		this.closed = false;
		this.nextRequestId = 1;
		// agent command ids seen on stdin from the client → pipe to stdin unchanged
		this.tag = crypto.randomBytes(3).toString("hex");
		this.setNoDelay();
	}

	setNoDelay() {
		if (typeof this.socket.setNoDelay === "function") this.socket.setNoDelay(true);
	}

	close(reason) {
		if (this.closed) return;
		this.closed = true;
		try {
			writeLine(this.socket, { type: "close", reason: reason || "server_closed" });
		} catch {}
		try {
			this.socket.end();
		} catch {}
		if (this.agent && !this.agent.killed) {
			try {
				this.agent.kill("SIGTERM");
			} catch {}
			setTimeout(() => {
				if (this.agent && !this.agent.killed) {
					try {
						this.agent.kill("SIGKILL");
									} catch {}
				}
			}, 2000).unref();
		}
	}

	// ----- inbound line --------------------------------------------------------
	handleLine(line) {
		if (line.length > MAX_LINE_BYTES) {
			this.close("line_too_large");
			return;
		}
		let msg;
		try {
			msg = JSON.parse(line);
		} catch (e) {
			this.close("invalid_json");
			return;
		}
		if (!this.authenticated) {
			if (msg && msg.type === "auth" && typeof msg.token === "string") {
				const expected = process.env.PI_AGENT_BRIDGE_TOKEN || "";
				if (!expected) {
					writeLine(this.socket, { type: "auth_error", error: "server_misconfigured" });
					this.close("auth_misconfigured");
					return;
				}
				if (!safeEqual(msg.token, expected)) {
					writeLine(this.socket, { type: "auth_error", error: "unauthorized" });
					this.close("auth_failed");
					return;
				}
				this.authenticated = true;
				writeLine(this.socket, { type: "auth_ok" });
				return;
			}
			writeLine(this.socket, { type: "auth_required", protocol: 1 });
			this.close("auth_missing");
			return;
		}
		if (!this.initialized) {
			this.handleInit(msg);
			return;
		}
		// bridge-only commands
		if (msg && typeof msg.type === "string" && msg.type.startsWith("bridge.")) {
			this.handleBridgeCommand(msg);
			return;
		}
		// normal agent RPC: forward to agent stdin
		this.forwardToAgent(msg);
	}

	// ----- init handshake ------------------------------------------------------
	handleInit(msg) {
		if (!msg || msg.type !== "init") {
			writeLine(this.socket, { type: "init_error", error: "expected_init" });
			this.close("init_missing");
			return;
		}
		const cwd = String(msg.cwd || "").trim();
		if (!cwd || !path.isAbsolute(cwd)) {
			writeLine(this.socket, { type: "init_error", error: "cwd_must_be_absolute" });
			this.close("bad_cwd");
			return;
		}
		const sessionId = typeof msg.sessionId === "string" ? msg.sessionId : null;
		const toolNames = Array.isArray(msg.toolNames) ? msg.toolNames.map(String) : null;

		this.cwd = cwd;
		this.initialized = true;

		try {
			this.spawnAgent({ cwd, sessionId, toolNames });
		} catch (e) {
			writeLine(this.socket, { type: "init_error", error: String(e && e.message || e) });
			this.close("spawn_failed");
		}
	}

	spawnAgent({ cwd, sessionId, toolNames }) {
		const args = ["--mode", "rpc"];
		if (sessionId) args.push("--session", sessionId);
		if (toolNames) {
			// pi CLI uses comma-separated list of tool names; full tool names go through flag,
			// disabling uses --no-tools / --tools "" (best-effort, depends on SDK version)
			args.push("--tools", toolNames.join(","));
		}

		// Build a sanitized env: drop any PI_WEB_* secrets
		const env = {};
		for (const [k, v] of Object.entries(process.env)) {
			if (k.startsWith("PI_WEB_")) continue;
			if (k === "PI_AGENT_BRIDGE_TOKEN") continue;
			env[k] = v;
		}
		env.PI_CODING_AGENT = "true";
		env.AI_AGENT = "pi";
		env.PYTHONUNBUFFERED = "1";
		env.NODE_NO_WARNINGS = "1";

		const child = spawn(process.execPath, [this.opts.cliPath, ...args], {
			cwd,
			env,
			stdio: ["pipe", "pipe", "pipe"],
			windowsHide: true,
		});
		this.agent = child;

		child.stderr.on("data", (b) => {
			// forward agent stderr to our stderr, and tag every line so it can be filtered
			const s = b.toString();
			process.stderr.write(`[agent ${this.tag}] ${s}`);
		});

		// Watch early death
		let earlyExit = null;
		child.once("exit", (code, signal) => {
			if (!this.initialized || earlyExit) return;
			earlyExit = true;
			writeLine(this.socket, {
				type: "agent_dead",
				code,
				signal,
				error: `agent exited before init (code=${code} signal=${signal})`,
			});
			this.close("agent_early_exit");
		});

		// Pipe agent stdout → socket (events and responses)
		this.agentStdoutReader = attachLineReader(child.stdout, (line) => {
			if (line.length === 0) return;
			// Look for the first response with command "new_session" or "switch_session"
			// to learn the actual sessionId (the agent may have ignored our --session hint)
			try {
				const obj = JSON.parse(line);
				if (obj && obj.type === "response" && obj.command === "new_session" && obj.success) {
					this.sessionId = obj.data && obj.data.sessionId || this.sessionId;
				}
				if (obj && obj.type === "response" && obj.command === "get_state" && obj.success) {
					this.sessionId = obj.data && obj.data.sessionId || this.sessionId;
				}
			} catch {}
			try {
				this.socket.write(line + "\n");
			} catch {}
		});

		// Wait for an internal init ack: we send "get_state" to learn sessionId/sessionFile
		// but only after the agent is responsive. Use a lightweight probe — but actually the
		// agent emits messages lazily. We'll let the client poll via get_state directly.
		writeLine(this.socket, {
			type: "init_ok",
			sessionId: this.sessionId,
			sessionFile: null,
			pid: child.pid,
		});
	}

	forwardToAgent(msg) {
		if (!this.agent || !this.agent.stdin || this.agent.stdin.destroyed) {
			// synthesize an error response so the client's pending promise rejects
			if (msg && msg.id) {
				writeLine(this.socket, {
					type: "response",
					id: msg.id,
					command: msg.type,
					success: false,
					error: "agent_unavailable",
				});
			}
			return;
		}
		try {
			this.agent.stdin.write(JSON.stringify(msg) + "\n");
		} catch (e) {
			if (msg && msg.id) {
				writeLine(this.socket, {
					type: "response",
					id: msg.id,
					command: msg.type,
					success: false,
					error: `agent_stdin_write_failed: ${e.message}`,
				});
			}
		}
	}

	// ----- bridge-only commands (filesystem / sessions metadata) ---------------
	handleBridgeCommand(msg) {
		const respond = (success, data, error) => {
			writeLine(this.socket, {
				type: "response",
				id: msg.id,
				command: msg.type,
				success,
				...(success ? { data } : { error }),
			});
		};

		try {
			switch (msg.type) {
				case "bridge.ping":
					return respond(true, { pong: true, uptime: process.uptime(), pid: process.pid });

				case "bridge.list_sessions": {
					const out = listAllSessions();
					return respond(true, { sessions: out });
				}

				case "bridge.read_file": {
					if (!isPathSafe(msg.path)) return respond(false, undefined, "path_not_allowed");
					const encoding = msg.encoding === null ? null : "utf8";
					const maxBytes = clampInt(msg.maxBytes, 1, 1024 * 1024 * 16, 1024 * 1024);
					if (!fs.existsSync(msg.path)) return respond(false, undefined, "enoent");
					const buf = fs.readFileSync(msg.path);
					const truncated = buf.length > maxBytes;
					const slice = truncated ? buf.subarray(0, maxBytes) : buf;
					return respond(true, {
						bytes: buf.length,
						truncated,
						text: encoding === null ? null : slice.toString("utf8"),
						base64: encoding === null ? slice.toString("base64") : undefined,
					});
				}

				case "bridge.stat": {
					if (!isPathSafe(msg.path)) return respond(false, undefined, "path_not_allowed");
					const st = fs.statSync(msg.path, { throwIfNoEntry: false });
					if (!st) return respond(true, { exists: false });
					return respond(true, {
						exists: true,
						isFile: st.isFile(),
						isDirectory: st.isDirectory(),
						size: st.size,
						mtimeMs: st.mtimeMs,
					});
				}

				case "bridge.readdir": {
					if (!isPathSafe(msg.path)) return respond(false, undefined, "path_not_allowed");
					const entries = fs.readdirSync(msg.path, { withFileTypes: true })
						.map((d) => ({ name: d.name, isDir: d.isDirectory() }));
					return respond(true, { entries });
				}

				case "bridge.tail_session": {
					if (!isPathSafe(msg.sessionFile)) return respond(false, undefined, "path_not_allowed");
					if (!fs.existsSync(msg.sessionFile)) return respond(false, undefined, "enoent");
					const maxBytes = clampInt(msg.maxBytes, 1, 1024 * 1024 * 16, 1024 * 1024 * 4);
					const text = tailFile(msg.sessionFile, maxBytes);
					return respond(true, { path: msg.sessionFile, text, bytes: Buffer.byteLength(text, "utf8") });
				}

				case "bridge.export_session": {
					if (!isPathSafe(msg.sessionFile)) return respond(false, undefined, "path_not_allowed");
					if (!fs.existsSync(msg.sessionFile)) return respond(false, undefined, "enoent");
					const html = exportSessionHtml(msg.sessionFile);
					return respond(true, { html, path: msg.sessionFile });
				}

				default:
					return respond(false, undefined, `unknown_bridge_command: ${msg.type}`);
			}
		} catch (e) {
			respond(false, undefined, `bridge_error: ${e && e.message || e}`);
		}
	}
}

// ----------------------------------------------------------------------------
// Security: path sandbox
// ----------------------------------------------------------------------------
const ALLOWED_ROOTS = (() => {
	const roots = new Set();
	const home = os.homedir();
	roots.add(path.resolve(home));
	try {
		const sessionsDir = path.join(home, ".pi", "agent", "sessions");
		roots.add(sessionsDir);
	} catch {}
	const extra = (process.env.PI_AGENT_BRIDGE_EXTRA_ROOTS || "")
		.split(",").map((s) => s.trim()).filter(Boolean);
	for (const r of extra) roots.add(path.resolve(r));
	return roots;
})();

function isPathSafe(p) {
	if (typeof p !== "string" || !p) return false;
	let resolved;
	try {
		resolved = fs.realpathSync(path.resolve(p));
	} catch {
		// Path may not exist yet for some operations; resolve without realpath
		try { resolved = path.resolve(p); } catch { return false; }
	}
	for (const root of ALLOWED_ROOTS) {
		if (resolved === root) return true;
		if (resolved.startsWith(root + path.sep)) return true;
	}
	return false;
}

function safeEqual(a, b) {
	if (typeof a !== "string" || typeof b !== "string") return false;
	const ab = Buffer.from(a, "utf8");
	const bb = Buffer.from(b, "utf8");
	if (ab.length !== bb.length) return false;
	return crypto.timingSafeEqual(ab, bb);
}

function clampInt(v, lo, hi, dflt) {
	const n = Number(v);
	if (!Number.isFinite(n)) return dflt;
	return Math.max(lo, Math.min(hi, Math.trunc(n)));
}

function tailFile(p, maxBytes) {
	const stat = fs.statSync(p);
	const fd = fs.openSync(p, "r");
	try {
		const start = Math.max(0, stat.size - maxBytes);
		const len = stat.size - start;
		const buf = Buffer.alloc(len);
		fs.readSync(fd, buf, 0, len, start);
		return buf.toString("utf8");
	} finally {
		fs.closeSync(fd);
	}
}

// ----------------------------------------------------------------------------
// Session listing (no agent dependency)
// ----------------------------------------------------------------------------
function listAllSessions() {
	const home = os.homedir();
	const root = path.join(home, ".pi", "agent");
	if (!fs.existsSync(root)) return [];
	// We walk one level deep (sessions/<encoded-cwd>/<files>) — encoded cwd is opaque
	const out = [];
	const cwdDirs = safeReaddir(root);
	for (const cwdEntryName of cwdDirs) {
		const cwdPath = path.join(root, cwdEntryName);
		let st;
		try { st = fs.statSync(cwdPath); } catch { continue; }
		if (!st.isDirectory()) continue;
		const files = safeReaddir(cwdPath);
		for (const f of files) {
			if (!f.endsWith(".jsonl")) continue;
			const fp = path.join(cwdPath, f);
			let fst;
			try { fst = fs.statSync(fp); } catch { continue; }
			let sessionId = null;
			let cwdDecoded = cwdEntryName;
			let startedAt = null;
			let messageCount = 0;
			try {
				const head = tailFile(fp, 16 * 1024).split("\n").filter(Boolean)[0];
				if (head) {
					const header = JSON.parse(head);
					sessionId = header.id || null;
					cwdDecoded = header.cwd || cwdDecoded;
					startedAt = header.timestamp || null;
				}
				// approximate messageCount by counting newline JSON records in tail
				const tail = tailFile(fp, 512 * 1024);
				messageCount = tail.split("\n").filter(Boolean).length;
			} catch {}
			out.push({
				sessionFile: fp,
				sessionId,
				cwd: cwdDecoded,
				startedAt,
				lastModifiedMs: fst.mtimeMs,
				sizeBytes: fst.size,
				entryEstimate: messageCount,
			});
		}
	}
	out.sort((a, b) => b.lastModifiedMs - a.lastModifiedMs);
	return out;
}

function safeReaddir(p) {
	try { return fs.readdirSync(p); } catch { return []; }
}

// ----------------------------------------------------------------------------
// HTML export (delegates to the SDK's export helper if present; otherwise
// returns a minimal "export coming soon" payload so the UI degrades gracefully)
// ----------------------------------------------------------------------------
function exportSessionHtml(p) {
	try {
		// Lazy require so the bridge still starts even without the package
		const sdk = require(path.join(path.dirname(this?.opts?.cliPath || ""), "..", "package.json"));
		if (sdk && sdk.name === "@earendil-works/pi-coding-agent") {
			// We can't easily import TS at runtime; the browser uses the SDK's HTML export
			// via the agent's existing export_html RPC command. This bridge call is a fallback
			// that just returns a manifest — the actual export_html RPC produces the final HTML.
			return `<!doctype html><html><body><pre>Session: ${p}\nUse export_html RPC for the formatted export.</pre></body></html>`;
		}
	} catch {}
	return `<!doctype html><html><body><pre>Session: ${p}</pre></body></html>`;
}

// ----------------------------------------------------------------------------
// Server bootstrap
// ----------------------------------------------------------------------------
function main() {
	const opts = parseArgs(process.argv.slice(2));
	opts.cliPath = resolveCliPath(opts.cliPath);

	if (!process.env.PI_AGENT_BRIDGE_TOKEN || process.env.PI_AGENT_BRIDGE_TOKEN.length < 16) {
		process.stderr.write(
			"FATAL: PI_AGENT_BRIDGE_TOKEN env var is required and must be at least 16 chars.\n",
		);
		process.exit(2);
	}

	const connections = new Set();

	function attach(socket) {
		if (connections.size >= opts.maxConns) {
			socket.write(JSON.stringify({ type: "refused", reason: "max_conns_reached" }) + "\n");
			socket.destroy();
			return;
		}
		const conn = new BridgeConnection(socket, opts);
		connections.add(conn);

		// Greeting
		try {
			writeLine(socket, {
				type: "hello",
				protocol: 1,
				bridgeVersion: "1.0.0",
				hostname: os.hostname(),
				agentDir: path.join(os.homedir(), ".pi", "agent"),
				authRequired: true,
			});
		} catch {
			socket.destroy();
			return;
		}
		attachLineReader(socket, (line) => conn.handleLine(line));

		socket.on("error", (e) => process.stderr.write(`[socket ${conn.tag}] ${e.message}\n`));
		socket.on("close", () => {
			connections.delete(conn);
			conn.close("client_closed");
		});
	}

	if (opts.socketPath) {
		try { fs.unlinkSync(opts.socketPath); } catch {}
		const server = net.createServer(attach);
		server.listen(opts.socketPath, () => {
			fs.chmodSync(opts.socketPath, 0o660);
			process.stdout.write(
				`pi-agent-bridge listening on unix:${opts.socketPath} (cli=${opts.cliPath})\n`,
			);
		});
	} else {
		const server = net.createServer(attach);
		server.listen(opts.port, opts.host, () => {
			process.stdout.write(
				`pi-agent-bridge listening on ${opts.host}:${opts.port} (cli=${opts.cliPath})\n`,
			);
		});
	}
}

if (require.main === module) main();
