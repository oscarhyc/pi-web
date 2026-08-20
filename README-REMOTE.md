# pi-web remote-agent mode — deployment guide

This fork adds an "remote agent" transport to pi-web. Instead of running the
pi AgentSession **in the same Node process as the web UI**, every command is
forwarded over a TCP/Unix socket to a small bridge process (`pi-agent-bridge`)
on a separate machine. The web UI stays stateless, all session files and the
agent's filesystem access live on the agent machine.

```
┌──────────────────────────────┐                  ┌─────────────────────────────────┐
│ .120  (frontend)             │    TCP/JSONL     │ .122  (agent machine)           │
│                              │◀────────────────▶│                                 │
│  pi-web (Next.js)            │  line-delimited  │  pi-agent-bridge.js             │
│   ├─ Browser SSE → /api/...  │  JSON,           │   ├─ per-conn: spawn            │
│   └─ patched rpc-manager     │  token-auth      │   │   `pi --mode rpc --cwd X`    │
│       routes everything      │  (LAN/VPN/TLS)   │   ├─ bridge.list_sessions,      │
│       through the bridge     │                  │   │   bridge.read_file, etc.     │
│                              │                  │   └─ rejects unauthorized,      │
│                              │                  │      enforces path sandbox      │
└──────────────────────────────┘                  └─────────────────────────────────┘
```

## What this fork adds

| File | Purpose |
|---|---|
| `agent-host/bin/pi-agent-bridge.js` | New: TCP/Unix-socket server. Spawns one `pi --mode rpc` per pi-web session, plus bridge-only filesystem commands. |
| `agent-host/package.json` | Tiny package for the bridge; depends only on `@earendil-works/pi-coding-agent`. |
| `lib/remote-agent-transport.ts` | New: TCP/JSONL client. Faithful port of the SDK's stdio `RpcClient` to a network transport, plus typed wrappers around the bridge-only commands. |
| `lib/transports/index.ts` | New: `SessionTransport` interface + factory. |
| `lib/transports/in-process.ts` | New: default behaviour preserved as a transport. |
| `lib/transports/remote.ts` | New: `RemoteHandle` — bridges `TransportHandle` ↔ `RemoteAgentTransport`. |
| `lib/transports/remote-agent-session-like.ts` | New: minimal `AgentSessionLike` adapter that forwards every required SDK method to the remote agent. |
| `lib/transports/agent-session-adapter.ts` | New (alternative path): turns `TransportHandle` into something `AgentSessionWrapper` can wrap without further edits. |
| `lib/rpc-manager.ts` | **Patched**: `startRpcSession()` branches to remote transport when `PI_WEB_AGENT_URL` is set. Everything else (idle timer, registry, running-state notifications) is reused unchanged. |
| `lib/session-reader.ts` | **Patched**: `loadAllSessions()` uses `bridge.list_sessions` in remote mode. |

## Quick start

### One-time setup

```bash
# On the agent machine (e.g. 192.168.1.122) — the bridge
git clone <your-fork>
cd pi-web
cd agent-host
npm install --omit=dev
# Generate a token. Must be ≥ 16 chars; share out-of-band with the .120 host.
export PI_AGENT_BRIDGE_TOKEN="$(openssl rand -hex 32)"
echo "$PI_AGENT_BRIDGE_TOKEN" > ~/.pi-bridge-token
```

```bash
# On the frontend machine (e.g. 192.168.1.120) — pi-web
git clone <your-fork>
cd pi-web
npm install
npm run build
```

### Run the bridge on `.122`

The bridge listens on TCP by default. Pick a port; 30142 is recommended to keep
clear of pi-web's 30141.

```bash
# Foreground:
PI_AGENT_BRIDGE_TOKEN="$(cat ~/.pi-bridge-token)" \
  node agent-host/bin/pi-agent-bridge.js --port 30142 --host 0.0.0.0
```

Or as a systemd unit (`/etc/systemd/system/pi-agent-bridge.service`):

```ini
[Unit]
Description=pi-agent-bridge
After=network-online.target

[Service]
Type=simple
User=YOUR_USER
EnvironmentFile=/home/YOUR_USER/.pi-bridge-token.env
ExecStart=/usr/bin/env node /opt/pi-web/agent-host/bin/pi-agent-bridge.js --port 30142 --host 0.0.0.0
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Where `~/.pi-bridge-token.env` contains one line:
```
PI_AGENT_BRIDGE_TOKEN=that_long_base64_string
```

Open the firewall on `.122` for port 30142 from your LAN (TCP).

### Run pi-web on `.120` against the bridge

```bash
PI_WEB_AGENT_URL=tcp://192.168.1.122:30142 \
PI_WEB_AGENT_TOKEN="$(cat ~/.pi-bridge-token)" \
PI_WEB_PASSWORD='<strong-password-for-pi-web-basic-auth>' \
  npx pi-web --hostname 0.0.0.0 -p 30141 --no-open
```

Visit `http://192.168.1.100:30141` from your PC's browser, sign in as `pi`
with the password, and the session list, prompts, and streaming will all flow
through the bridge to `.122`.

> Note: keep `.120` and `.122` on a trusted LAN or terminate TLS at a reverse
> proxy. The bridge token and pi-web password are both base64-encoded over the
> wire. See "Security" below.

## Configuration matrix

| Variable / flag | Default | Effect |
|---|---|---|
| `PI_WEB_AGENT_URL` | unset | If set, pi-web uses the remote transport. Example: `tcp://192.168.1.122:30142` |
| `PI_WEB_AGENT_URL=unix:/path/to/socket` | — | Unix-domain sockets also work, useful for same-host but privilege-separated deployments |
| `PI_WEB_AGENT_TOKEN` | unset | Required when `PI_WEB_AGENT_URL` is set. Shared bearer token matched against the bridge's `PI_AGENT_BRIDGE_TOKEN`. |
| `PI_AGENT_BRIDGE_TOKEN` | required | Server-side token; ≥ 16 chars. |
| `PI_AGENT_BRIDGE_PORT` | 30142 | TCP port (alternative to `--port`) |
| `PI_AGENT_BRIDGE_HOST` | 0.0.0.0 | Bind address (alternative to `--host`) |
| `PI_AGENT_BRIDGE_SOCKET` | unset | Unix-socket path (alternative to `--socket`) |
| `PI_AGENT_BRIDGE_CLI` | auto-probed | Explicit path to `@earendil-works/pi-coding-agent/dist/cli.js` |
| `PI_AGENT_BRIDGE_EXTRA_ROOTS` | `~/.pi`, `~/.pi/agent/sessions` | Comma-separated additional filesystem roots the bridge may read (e.g. project repositories on `.122`). |
| `PI_AGENT_BRIDGE_MAX_CONNS` | 32 | Bridge-side maximum concurrent connections. |
| `PI_WEB_PASSWORD` | unset | pi-web HTTP Basic Auth (independent of bridge token). |
| `PORT`, `--port` | 30141 | pi-web port |
| `PI_WEB_HOSTNAME`, `--hostname` | 127.0.0.1 | pi-web bind address (use `0.0.0.0` for LAN) |

## Feature parity (what works, what doesn't, what's missing)

The fork supports everything pi's `RpcClient` does. RPC's command surface is
smaller than what `AgentSessionWrapper` exposes; the gaps below are honest:

### ✅ Works

- Session listing (via `bridge.list_sessions`)
- New session, switch session, fork session
- `prompt`, `steer`, `follow_up`, `abort`
- Model + thinking-level selection
- Bash command execution (`bash`, `abort_bash`)
- Compact, auto-compaction toggle, auto-retry toggle
- Session names, last-assistant-text, session stats
- Streaming agent events → SSE → browser
- Extension UI requests (`select`/`confirm`/`input`/`editor`/`notify`/`setStatus`/`setWidget`/`setTitle`)

### ⚠️  Partial / workaround

| Feature | Status | Notes |
|---|---|---|
| Tool selection (`get_tools`, `set_tools`) | initial tool list is set when the session is spawned; changes after start are unsupported. | Workaround: rebuild session via fork-or-restart. |
| `navigate_tree` (in-session branch switch) | not in RPC; remote mode uses **fork** to follow a different leaf instead. | UI surfaces this as a copy-and-branch action when remote. |
| `reload` of session metadata | not surfaced; reload happens implicitly when a new session is opened via the bridge. | |
| `clear_queue` | best-effort: an `abort` is sent; in-flight messages may still drain. | |
| `abort_compaction` | falls back to `abort` if the bridge can't react faster than the agent finishes compacting. | |
| Custom TUI extensions (`set_widget` with factories, `set_footer`, `set_header`) | RPC supports `setWidget` with string arrays; factories are not part of the protocol. | The UI already renders widget lines as plain strings; this is sufficient. |

### ❌ Not implemented (out of scope for this fork's MVP)

- File browser over the wire. The `app/api/files/[...path]` route still reads
  the local filesystem on `.120`. To make it work, route reads through
  `bridge.read_file`/`bridge.readdir` and write a parallel filesystem stub
  on `.120`. See "Extending this fork" below.
- Auth provider OAuth flows over RPC. The `auth/login/[provider]` routes
  still use `AuthStorage` locally. Providers are configured on the **frontend**
  by signing in via the Models panel; the resulting `~/.pi/agent/auth.json` is
  read by the agent on `.122` if you share or sync that directory, or you can
  set environment variables for provider keys directly on `.122`.
- Skills install (`/api/skills/install`) shells out to `npx skills add` on
  `.120`. In remote mode, run skill installs on `.122` directly (or share
  `~/.pi/agent/skills`).
- Plugin package management (`/api/plugins`) — same as skills, run on `.122`.
- Worktree creation should run on `.122` (the branch is checked out from
  `.122`'s working copy of the repo). Currently worktree operations fall
  through the existing in-process code path that runs on `.120`; you should
  pre-create worktrees on `.122` if your project's repo lives there.

## Security

- **Bridge token over plain TCP** is not encrypted. Acceptable on a trusted
  LAN, across a WireGuard/Tailscale mesh, or behind a TLS-terminating reverse
  proxy. SSH local port forward also works:
  `ssh -L 30142:127.0.0.1:30142 user@.122` then point pi-web at
  `tcp://127.0.0.1:30142`.
- The bridge rejects any file path that is not within `~/.pi`,
  `~/.pi/agent/sessions`, or an entry of `PI_AGENT_BRIDGE_EXTRA_ROOTS`.
  Add the project repository roots you want the UI to be able to browse.
- Each accepted bridge connection spawns one `pi --mode rpc` subprocess with
  a sanitized environment (no `PI_WEB_*` and no `PI_AGENT_BRIDGE_TOKEN`).
- There is **no per-user sandboxing** — whoever holds the token can do
  anything that the bridge process's OS user can do. Treat the token like
  an SSH key.

## Reverse-proxy recipes

### Caddy (TLS in front of pi-web)

```
pi.example.com {
    encode zstd gzip
    reverse_proxy 127.0.0.1:30141
    basicauth {
        pi JDJhJDE0JDc4YS4uLi53ZWxsIGxvbmcgcmFuZG9tIHBhc3N3b3Jk
    }
}
```

If you front the bridge with TLS too, also add:
```
agent.example.com:30142 {
    reverse_proxy 192.168.1.122:30142
}
```
…and set `PI_WEB_AGENT_URL=tcp://agent.example.com:30142` on `.120`.

### nginx + TLS

`/etc/nginx/sites-available/pi-web.conf`:
```nginx
server {
    listen 443 ssl http2;
    server_name pi.example.com;

    ssl_certificate     /etc/letsencrypt/live/pi.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pi.example.com/privkey.pem;

    # Auth header is required by pi-web when PI_WEB_PASSWORD is set
    location / {
        proxy_pass http://127.0.0.1:30141;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        # SSE: turn off proxy buffering so events stream live
        proxy_buffering off;
        proxy_read_timeout 24h;
        proxy_set_header Connection "";
    }
}
```

## Smoke-testing the bridge manually

In one terminal, start the bridge:
```bash
PI_AGENT_BRIDGE_TOKEN=test-token-at-least-sixteen-bytes \
  node agent-host/bin/pi-agent-bridge.js --port 30142 --host 127.0.0.1
```

In another terminal, run a one-liner to speak the protocol with `nc(1)`:
```bash
{ printf '{"type":"auth","token":"test-token-at-least-sixteen-bytes"}\n{"type":"init","cwd":"'$(pwd)'","sessionId":null}\n{"type":"bridge.ping"}\n'; sleep 2; } \
  | nc -q1 127.0.0.1 30142
```

You should see three or more lines from the server: `hello`, `auth_ok` /
`init_ok` / a JSON response to `bridge.ping`.

You can also run the bridge's npm script:
```bash
cd agent-host && npm run start
```

## Extending this fork

The intentional layer for further work is `lib/transports/`. Each file route
that reads from disk (`/api/files/[...]`) can be made remote-aware by adding
to `transports/remote-agent-session-like.ts` plus calling the appropriate
`bridge.*` RPC, then patching the route to use that.

Routing `auth/*` and `skills/install` over the wire is the next most useful
follow-up; they would each add a new bridge-only command (`bridge.provider_status`,
`bridge.skills_install`) and consume it from the corresponding route handler.

## Compatibility

- Tested against `@earendil-works/pi-coding-agent@0.84.2` (same pin pi-web uses).
- Requires Node.js ≥ 22.19.0 on both machines (already a pi-web requirement).
- The bridge does not mutate agent state. Each pi-web session corresponds to
  one `pi --mode rpc` subprocess; killing the bridge terminates the
  subprocess and the session ends (no in-flight recovery — sessions are
  JSONL so reopening on next connection works).
