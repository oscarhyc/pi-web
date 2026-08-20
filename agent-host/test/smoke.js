#!/usr/bin/env node
"use strict";
/**
 * pi-agent-bridge smoke test.
 *
 * Spawns the bridge on a localhost TCP port, then runs through the wire
 * protocol in a separate Node process (or in-process if invoked directly).
 * Exits non-zero on any failure.
 *
 * Usage:
 *   PI_AGENT_BRIDGE_TOKEN=test-token-at-least-sixteen-bytes node bin/pi-agent-bridge.js &
 *   PI_AGENT_BRIDGE_TOKEN=test-token-at-least-sixteen-bytes \
 *     PI_AGENT_BRIDGE_PORT=39393 \
 *     node agent-host/test/smoke.js
 */

const net = require("net");
const { spawn } = require("child_process");
const path = require("path");

const TOKEN = process.env.PI_AGENT_BRIDGE_TOKEN;
const PORT = Number(process.env.PI_AGENT_BRIDGE_PORT ?? 39393);

if (!TOKEN || TOKEN.length < 16) {
	process.stderr.write("PI_AGENT_BRIDGE_TOKEN must be set (>=16 chars)\n");
	process.exit(2);
}

function lineReader(stream) {
	const decoder = new (require("string_decoder").StringDecoder)("utf8");
	let buf = "";
	const listeners = [];
	stream.on("data", (chunk) => {
		buf += typeof chunk === "string" ? chunk : decoder.write(chunk);
		let i;
		while ((i = buf.indexOf("\n")) !== -1) {
			listeners.forEach((cb) => cb(buf.slice(0, i)));
			buf = buf.slice(i + 1);
		}
	});
	return {
		onLine: (cb) => { listeners.push(cb); },
	};
}

function assert(cond, msg) {
	if (!cond) { throw new Error(`assertion failed: ${msg}`); }
}

async function main() {
	const bridgeBin = path.join(__dirname, "..", "bin", "pi-agent-bridge.js");
	process.stdout.write(`[smoke] spawning bridge on 127.0.0.1:${PORT}\n`);
	const bridge = spawn(process.execPath, [bridgeBin, "--port", String(PORT), "--host", "127.0.0.1"], {
		env: { ...process.env, PI_AGENT_BRIDGE_TOKEN: TOKEN },
		stdio: ["ignore", "inherit", "inherit"],
	});
	// Give the bridge a beat to start listening
	await new Promise((r) => setTimeout(r, 600));
	process.on("exit", () => { try { bridge.kill("SIGTERM"); } catch {} });

	const sock = net.createConnection({ host: "127.0.0.1", port: PORT });
	await new Promise((res, rej) => { sock.once("connect", res); sock.once("error", rej); });

	const events = [];
	const reader = lineReader(sock);
	const waitFor = (predicate, timeoutMs = 5000) => new Promise((resolve, reject) => {
		const t = setTimeout(() => reject(new Error(`timeout waiting for predicate ${predicate}`)), timeoutMs);
		const handler = (line) => {
			try {
				const obj = JSON.parse(line);
				if (predicate(obj)) {
					clearTimeout(t);
					reader.onLine(() => {}); // detach
					resolve(obj);
				} else {
					events.push(obj);
				}
			} catch { /* skip */ }
		};
		reader.onLine(handler);
	});

	// 1) hello
	const hello = await waitFor((m) => m.type === "hello");
	assert(hello.protocol === 1, "protocol");
	process.stdout.write(`[smoke] hello: ${JSON.stringify(hello)}\n`);

	// 2) auth ok
	sock.write(JSON.stringify({ type: "auth", token: TOKEN }) + "\n");
	const authOk = await waitFor((m) => m.type === "auth_ok");
	assert(authOk.type === "auth_ok", "auth");
	process.stdout.write(`[smoke] auth_ok ✓\n`);

	// 3) init
	sock.write(JSON.stringify({ type: "init", cwd: process.cwd(), sessionId: null }) + "\n");
	const init = await waitFor((m) => m.type === "init_ok" || m.type === "init_error");
	assert(init.type === "init_ok", `init failed: ${JSON.stringify(init)}`);
	process.stdout.write(`[smoke] init_ok sessionId=${init.sessionId}\n`);

	// 4) bridge.ping
	const pingId = `req_${Math.floor(Math.random() * 1e6)}`;
	sock.write(JSON.stringify({ id: pingId, type: "bridge.ping" }) + "\n");
	const pingResp = await waitFor((m) => m.type === "response" && m.id === pingId);
	assert(pingResp.success, `ping not success: ${JSON.stringify(pingResp)}`);
	process.stdout.write(`[smoke] bridge.ping ✓\n`);

	// 5) bridge.list_sessions
	const lsId = `req_${Math.floor(Math.random() * 1e6)}`;
	sock.write(JSON.stringify({ id: lsId, type: "bridge.list_sessions" }) + "\n");
	const lsResp = await waitFor((m) => m.type === "response" && m.id === lsId);
	assert(lsResp.success, `list_sessions not success: ${JSON.stringify(lsResp)}`);
	assert(Array.isArray(lsResp.data?.sessions), "sessions array");
	process.stdout.write(`[smoke] list_sessions returned ${lsResp.data.sessions.length} sessions ✓\n`);

	// 6) auth failure path — fresh connection, fresh reader
	const sock2 = net.createConnection({ host: "127.0.0.1", port: PORT });
	await new Promise((res, rej) => { sock2.once("connect", res); sock2.once("error", rej); });
	const lines2 = [];
	const reader2 = lineReader(sock2);
	const authFailReader = new Promise((resolve, reject) => {
		const t = setTimeout(() => reject(new Error("timeout waiting for auth_error")), 3000);
		reader2.onLine((line) => {
			lines2.push(line);
			let obj;
			try { obj = JSON.parse(line); } catch { return; }
			if (obj && obj.type === "auth_error") {
				clearTimeout(t);
				resolve(obj);
			}
		});
	});
	// Read until we see hello, then send a bad token
	await new Promise((resolve) => {
		const t = setTimeout(resolve, 1500);
		reader2.onLine((line) => {
			try { if (JSON.parse(line).type === "hello") { clearTimeout(t); resolve(); } } catch {}
		});
	});
	sock2.write(JSON.stringify({ type: "auth", token: "WRONG-TOKEN-TOTALLY-NOT-IT" }) + "\n");
	const authFail = await authFailReader;
	assert(authFail.type === "auth_error", `auth_error expected, got: ${JSON.stringify(authFail)}`);
	process.stdout.write(`[smoke] auth rejected on bad token ✓\n`);
	sock2.destroy();
	await new Promise((r) => sock2.once("close", r));

	sock.end();
	await new Promise((r) => sock.once("close", r));
	bridge.kill("SIGTERM");
	process.stdout.write(`[smoke] PASS\n`);
	process.exit(0);
}

main().catch((e) => { process.stderr.write(`[smoke] FAIL: ${e.message}\n`); process.exit(1); });
