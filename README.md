# neem

A tiny **stdio MCP server** that lets Claude Code (and any other MCP client) talk
to a **Mnemosyne / garden** knowledge-graph backend.

`neem` is a **proxy**. It does not implement tools. It forwards `initialize`,
`tools/list`, and `tools/call` to a backend, and the backend's tools are
**autopopulated** — whatever the backend exposes is what the agent sees. Garden
owns the tools; neem owns *who you are*, *which graph*, and *which backend*.

There are two backends, sharing the exact same proxy core:

| Backend | What it is | When |
|---|---|---|
| **LOCAL** (default) | neem **starts** a headless garden on your machine and proxies to its loopback. Near-zero config. | Out-of-the-box. Just run `neem`. |
| **REMOTE `<url>`** | neem **connects** to an existing backend — a platform-next gateway cell `/g/{id}/mcp`, or any garden loopback `/mcp` — with auth. | You already have a hosted/shared graph. |

The only difference between them is whether neem *starts* the backend or just
*connects* to it.

---

## Install / build

```bash
cargo build --release
# binary at ./target/release/neem
```

Requires a recent stable Rust (edition 2021, rustc ≥ 1.85).

---

## Out-of-the-box (LOCAL backend)

```bash
neem
```

That's it. With no arguments neem:

1. spawns a headless **`gardend`** on a local profile dir (default
   `~/.neem/profile`, created on first run),
2. waits for its `/health`,
3. discovers the loopback endpoint + token from garden's `loopback.json`,
4. proxies your agent's MCP traffic to `http://127.0.0.1:<port>/mcp`.

Your agent immediately sees garden's full tool catalog (`search_documents`,
`read_document`, `write_document`, `remember`, `list_graphs`, …) — **none of it
hardcoded in neem**.

### Prerequisite: the `gardend` binary

The LOCAL backend runs the stock, OSS **garden** cell binary. neem finds it via,
in order:

1. `--garden-bin <path>` (or `NEEM_GARDEN_BIN`),
2. a `gardend` next to the `neem` binary,
3. `../garden/src-tauri/target/release/gardend` (sibling checkout),
4. `gardend` on `PATH`.

Build it once from the garden repo (it's a normal headless Cargo target):

```bash
# in the garden checkout:
cargo build --release --no-default-features --features headless --bin gardend
```

Then point neem at it if it isn't already discoverable:

```bash
neem --garden-bin /path/to/garden/src-tauri/target/release/gardend
```

> **Why a subprocess, not a library link?** garden's in-process headless
> entrypoint needs a Tauri MockRuntime `AppHandle` minted via `generate_context!`,
> which would drag garden's entire native build (oxigraph, candle,
> fastembed/onnxruntime, turso, yrs, tauri-build) into neem. The whole rest of
> the system already treats `gardend` as a process/image (the platform-next
> gateway runs the `gardend` container and never links it). neem mirrors that.
> An experimental in-process variant is scaffolded behind the
> `local-garden-lib` feature — see `src/backend/local_lib.rs` for the blocker.

---

## Point at an existing backend (REMOTE)

```bash
# A platform-next gateway, naming the graph:
neem --backend https://gateway.example.com --graph my-graph \
     --token "$PN_SERVICE_TOKEN" --on-behalf-of "$MY_COGNITO_SUB"

# …or a full MCP endpoint directly:
neem --backend https://gateway.example.com/g/my-graph/mcp --token "$JWT"

# …or any garden loopback:
neem --backend http://127.0.0.1:8086/mcp --token "$LOOPBACK_TOKEN"
```

URL resolution:

* a URL ending in `/mcp` is used as-is;
* a **base** URL plus `--graph <id>` becomes `<base>/g/<id>/mcp` (the gateway
  contract);
* otherwise neem appends `/mcp`.

Auth header shapes (mirroring the choreograph reference proxy):

| Mode | Headers neem sends |
|---|---|
| Gateway **service-auth** | `Authorization: Bearer <serviceToken>` + `x-pn-on-behalf-of: <sub>` |
| Direct user (JWT) | `Authorization: Bearer <jwt>` (+ optional `X-User-ID` via `--user-id`) |
| Garden loopback | `Authorization: Bearer <loopbackToken>` |

When `--on-behalf-of` is set, neem does **not** also send `X-User-ID` — identity
is the on-behalf-of header (the gateway runs its own per-graph ACL for that
subject).

---

## CLI / config

Every flag has an env var twin.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--backend` | `NEEM_BACKEND` | `local` | `local`, or a backend URL |
| `--token` | `NEEM_TOKEN` | — | bearer token for REMOTE |
| `--on-behalf-of` | `NEEM_ON_BEHALF_OF` | — | gateway service-auth subject |
| `--user-id` | `NEEM_USER_ID` | — | `X-User-ID` side-channel |
| `--graph` | `NEEM_GRAPH` | — | graph id (builds `/g/{id}/mcp` for a base URL) |
| `--profile-dir` | `NEEM_PROFILE_DIR` | `~/.neem/profile` | LOCAL data dir |
| `--garden-bin` | `NEEM_GARDEN_BIN` | auto-discover | LOCAL `gardend` path |
| `--local-port` | `NEEM_LOCAL_PORT` | `0` (OS-assigned) | LOCAL loopback port |
| `--local-health-timeout` | `NEEM_LOCAL_HEALTH_TIMEOUT` | `30` | seconds to wait for `/health` |

Logging goes to **stderr** (stdout is the MCP channel). Set `NEEM_LOG=debug` for
verbose output.

---

## Wire up Claude Code

Claude Code launches MCP servers over stdio. Add neem to your `mcpServers`
config (`.mcp.json` at the project root, or your user-level Claude config):

**LOCAL (out-of-the-box):**

```json
{
  "mcpServers": {
    "mnemosyne": {
      "command": "/absolute/path/to/neem",
      "args": ["--backend", "local"],
      "env": {
        "NEEM_GARDEN_BIN": "/absolute/path/to/gardend"
      }
    }
  }
}
```

**REMOTE (hosted gateway):**

```json
{
  "mcpServers": {
    "mnemosyne": {
      "command": "/absolute/path/to/neem",
      "args": [
        "--backend", "https://gateway.example.com",
        "--graph", "my-graph"
      ],
      "env": {
        "NEEM_TOKEN": "your-service-or-jwt-token",
        "NEEM_ON_BEHALF_OF": "your-cognito-sub"
      }
    }
  }
}
```

Or register it from the CLI:

```bash
claude mcp add mnemosyne -- /absolute/path/to/neem --backend local
```

Restart Claude Code; the Mnemosyne tools appear automatically.

---

## How it works

```
Claude Code ──stdio JSON-RPC──▶ neem ──HTTP JSON-RPC──▶ backend /mcp
            (initialize,                (same 3 methods,    (LOCAL gardend
             tools/list,                 verbatim           or REMOTE gateway
             tools/call)                 passthrough)        /g/{id}/mcp)
```

* **Transport in:** newline-delimited JSON-RPC 2.0 on stdin/stdout.
* **Transport out:** "streamable-http-json" — a single JSON-RPC request POSTed
  to `/mcp`, a single JSON response. (Both gardend's loopback and the gateway
  speak this; the gateway forwards it byte-for-byte.)
* **Tools:** never hardcoded. `tools/list` returns the backend's catalog;
  `tools/call` forwards `{name, arguments}` and returns the result envelope.

### Layout

```
src/
  main.rs              CLI entry → build backend → serve stdio
  config.rs            clap CLI + env config
  mcp.rs               JSON-RPC 2.0 wire types + MCP method/error constants
  server.rs            stdio MCP loop (agent-facing): initialize/tools/list/tools/call
  backend/
    mod.rs             the `Backend` trait
    remote.rs          RemoteHttp — reqwest MCP client + auth headers (the proxy core)
    local.rs           LocalGarden — spawn gardend, wait /health, reuse RemoteHttp
    local_lib.rs       experimental in-process variant (feature `local-garden-lib`)
tests/
  remote_proxy.rs      end-to-end RemoteHttp against a mock /mcp
```

---

## Tests

```bash
cargo test
```

13 tests: URL resolution, auth-header construction, the stdio dispatch
(initialize backfill, tools passthrough, notification handling, unknown
method), and a wiremock-backed end-to-end of the remote proxy.

---

## License

MIT OR Apache-2.0.
