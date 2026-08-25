# sophia-mcp

A tiny **stdio MCP server** that lets Claude Code (and any other MCP client) talk
to a **Mnemosyne / garden** knowledge-graph backend.

`sophia-mcp` is a **proxy**. It does not implement tools. It forwards `initialize`,
`tools/list`, and `tools/call` to a backend, and the backend's tools are
**autopopulated** — whatever the backend exposes is what the agent sees. Garden
owns the tools; sophia-mcp owns *who you are*, *which graph*, and *which backend*.

There are two backends, sharing the exact same proxy core:

| Backend | What it is | When |
|---|---|---|
| **LOCAL** (default) | sophia-mcp **starts** a headless garden on your machine and proxies to its loopback. Near-zero config. | Out-of-the-box. Just run `sophia-mcp`. |
| **REMOTE `<url>`** | sophia-mcp discovers and activates an authorized platform-next cell at `/o/{owner}/g/{id}/mcp` — plus the gateway's control-plane tools and `graph_id` routing to your other graphs (see *Multi-graph*) — or connects to an explicit Garden loopback `/mcp`. | You already have a hosted/shared graph. |

The only difference between them is whether sophia-mcp *starts* the backend or just
*connects* to it.

---

## Install / build

```bash
cargo build --release
# binary at ./target/release/sophia-mcp
```

Requires a recent stable Rust (edition 2021, rustc ≥ 1.85).

---

## Out-of-the-box (LOCAL backend)

```bash
sophia-mcp
```

That's it. With no arguments sophia-mcp:

1. spawns a headless **`gardend`** on a local profile dir (default
   `~/.sophia-mcp/profile`, created on first run),
2. waits for its `/health`,
3. discovers the loopback endpoint + token from garden's `loopback.json`,
4. proxies your agent's MCP traffic to `http://127.0.0.1:<port>/mcp`.

Your agent immediately sees garden's full tool catalog (`search_documents`,
`read_document`, `write_document`, `remember`, `list_graphs`, …) — **none of it
hardcoded in sophia-mcp**.

### Prerequisite: the `gardend` binary

The LOCAL backend runs the stock, OSS **garden** cell binary. sophia-mcp finds it via,
in order:

1. `--garden-bin <path>` (or `SOPHIA_MCP_GARDEN_BIN`),
2. a `gardend` next to the `sophia-mcp` binary,
3. `../garden/src-tauri/target/release/gardend` (sibling checkout),
4. `gardend` on `PATH`.

Build it once from the garden repo (it's a normal headless Cargo target):

```bash
# in the garden checkout:
cargo build --release --no-default-features --features headless --bin gardend
```

Then point sophia-mcp at it if it isn't already discoverable:

```bash
sophia-mcp --garden-bin /path/to/garden/src-tauri/target/release/gardend
```

> **Why a subprocess, not a library link?** garden's in-process headless
> entrypoint needs a Tauri MockRuntime `AppHandle` minted via `generate_context!`,
> which would drag garden's entire native build (oxigraph, candle,
> fastembed/onnxruntime, turso, yrs, tauri-build) into sophia-mcp. The whole rest of
> the system already treats `gardend` as a process/image (the platform-next
> gateway runs the `gardend` container and never links it). sophia-mcp mirrors that.
> An experimental in-process variant is scaffolded behind the
> `local-garden-lib` feature — see `src/backend/local_lib.rs` for the blocker.

---

## Point at an existing backend (REMOTE)

```bash
# A platform-next gateway, naming the canonical owner tuple:
sophia-mcp --backend https://gateway.example.com \
     --owner user:$MY_COGNITO_SUB --graph my-graph \
     --token "$PN_SERVICE_TOKEN" --on-behalf-of "$MY_COGNITO_SUB"

# …or an explicit Garden loopback:
sophia-mcp --backend http://127.0.0.1:8086/mcp --token "$LOOPBACK_TOKEN"
```

URL resolution:

* a cloud-2 base URL plus `--owner <typed-id> --graph <id>` first checks the
  tuple against `/control/mcp` `list_graphs`, waits for activation, and binds
  `<base>/o/<owner>/g/<id>/mcp`;
* an explicit non-gateway URL ending in `/mcp` is used as-is;
* ambiguous remote URLs fail closed.

Auth header shapes (mirroring the choreograph reference proxy):

| Mode | Headers sophia-mcp sends |
|---|---|
| Gateway **service-auth** | `Authorization: Bearer <serviceToken>` + `x-pn-on-behalf-of: <sub>` |
| Direct user (JWT) | `Authorization: Bearer <jwt>` (+ optional `X-User-ID` via `--user-id`) |
| Garden loopback | `Authorization: Bearer <loopbackToken>` |

When `--on-behalf-of` is set, sophia-mcp does **not** also send `X-User-ID` — identity
is the on-behalf-of header (the gateway runs its own per-graph ACL for that
subject).

### Multi-graph: control tools + `graph_id` routing

Against a gateway, the agent sees **one** tool catalog that is the union of two
upstreams:

| Upstream | Endpoint | Tools |
|---|---|---|
| gateway control plane | `POST {base}/control/mcp` (probed first; `{base}/mcp` only with `--unified-mcp-fallback` when `/control/mcp` is 404) | `list_graphs`, `create_graph`, `manage_access`, `tombstone_graph`, `control_job_status`, … |
| the bound graph's cell | `POST {base}/o/{owner}/g/{graph}/mcp` | garden's own tools (`search_documents`, `sparql_query`, `remember`, …) |

`tools/list` merges both. Control tools are **always** exposed as `control_<name>`
(`control_list_graphs`, `control_create_graph`, …) so the agent-facing names stay
stable across cell releases whatever a cell happens to call its own tools; cell
tool names are untouched. When the cell paginates (`nextCursor`), control tools are
appended only on the last page. `tools/call` routes by name to the right upstream;
an unknown name is a JSON-RPC `-32601` naming the tool.

Every cell tool additionally accepts an optional **`graph_id`** (or `graphId`)
argument. When it names a graph other than the bound one, sophia-mcp routes the
call to `{base}/o/{listing-owner}/g/{that_graph}/mcp` — one cached upstream
session per `(owner, graph)`. Routing is **by path**, so the gateway's ACL is the
policy enforcement point:

* the graph must appear in this identity's control-plane `list_graphs`
  (refreshed once on a miss; the listing is *replaced*, never unioned, so a
  revoked graph stops routing) — an unlisted name is refused *before* any request
  touches its path, and sophia-mcp never creates or activates it;
* the **owner comes from the listing**, never from an argument — a graph another
  user shared with you routes to *their* `/o/…` path; a graph id listed under two
  owners is refused naming both;
* rows whose `lifecycleState` is not activatable (`tombstoned`, `purging`,
  `purged`) are never activated — the refusal names the state;
* a gateway `403`/`404` on a listed graph is surfaced verbatim (status + body).

Naming a dormant graph wakes it: a node may be provisioned and the call may block
for up to `--activation-timeout`.

`graph_id` and `graphId` given together must agree, or the call is refused before
any request; the body is rewritten so the target cell sees **exactly one** graph
argument, equal to the routed path. (`graph_id` as a bare tool argument was once an
unpoliced second carrier that made cells auto-create graphs; routing it through the
gateway path and normalizing the body closes that.)

### Waiting for a routable cell

A cloud-2 cell can be *boot-ready but not routable* (a running pod that does not
yet answer MCP), and waking a dormant cell takes minutes. sophia-mcp therefore:

1. on connect, proves the tuple via `list_graphs` and **kicks** activation
   (`POST …/activate`) — then answers the stdio `initialize` immediately, as
   `sophia-mcp`, without blocking on the cell;
2. before the first cell call (and in the background right after connect), polls
   `GET /activations/{id}` every `--activation-poll` seconds until the gateway's
   terminal phase `ready` (`failed` is surfaced with its error), **then proves
   routability with an MCP `initialize` on the cell path** — the only two
   observations that count. A `200` from `/health`-style probes, or `activate`
   answering `ready:true`, is never treated as readiness. A cell path that still
   answers 202/502/503 is re-probed with backoff (`--activation-poll` × 2ⁿ, capped
   at 8×); `activate` is POSTed at most once per wait;
3. on any mid-session `202 graph_activating` / `502` / `503` from the cell path,
   drops the cached session, re-waits, and retries the call;
4. bounds every wait by `--activation-timeout` (default 300 s), *including* time
   spent queued behind another waiter on the same graph, and bounds every single
   HTTP request by the remaining budget and `--request-timeout` — a hung upstream
   cannot stretch a wait. On expiry the JSON-RPC error carries the elapsed
   seconds, the budget, the poll count and the **last observed activation state**
   (e.g. `phase=hydrating — cell is hydrating its registered generation`, or
   `cell path answered HTTP 503: …`, or `request timed out after 12.0s: POST …`)
   — never a fabricated "not found";
5. follows an activation `pollUrl` only on the gateway's own origin — an
   off-origin URL is refused rather than sent the bearer token. A retryable
   `5xx` at the connect-time kick is logged, not fatal (`graph_repair_required`
   is typed non-retryable and surfaced at once).

Progress is logged to stderr only (`SOPHIA_MCP_LOG=info`).

---

## CLI / config

Every flag has an env var twin.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--backend` | `SOPHIA_MCP_BACKEND` | `local` | `local`, or a backend URL |
| `--token` | `SOPHIA_MCP_TOKEN` | — | bearer token for REMOTE |
| `--on-behalf-of` | `SOPHIA_MCP_ON_BEHALF_OF` | — | gateway service-auth subject |
| `--user-id` | `SOPHIA_MCP_USER_ID` | — | `X-User-ID` side-channel |
| `--owner` | `SOPHIA_MCP_OWNER` | — | stable typed owner required for cloud-2 |
| `--graph` | `SOPHIA_MCP_GRAPH` | — | local graph id required for cloud-2 (the *bound* graph) |
| `--activation-timeout` | `SOPHIA_MCP_ACTIVATION_TIMEOUT` | `300` | seconds to wait for a cell to become routable |
| `--activation-poll` | `SOPHIA_MCP_ACTIVATION_POLL` | `2` | seconds between activation polls; base of the re-probe backoff |
| `--request-timeout` | `SOPHIA_MCP_REQUEST_TIMEOUT` | `120` | per-request ceiling for any single HTTP request to the gateway |
| `--unified-mcp-fallback` | `SOPHIA_MCP_UNIFIED_MCP_FALLBACK` | `false` | also try `{base}/mcp` for control tools when `/control/mcp` is 404 |
| `--profile-dir` | `SOPHIA_MCP_PROFILE_DIR` | `~/.sophia-mcp/profile` | LOCAL data dir |
| `--garden-bin` | `SOPHIA_MCP_GARDEN_BIN` | auto-discover | LOCAL `gardend` path |
| `--local-port` | `SOPHIA_MCP_LOCAL_PORT` | `0` (OS-assigned) | LOCAL loopback port |
| `--local-health-timeout` | `SOPHIA_MCP_LOCAL_HEALTH_TIMEOUT` | `30` | seconds to wait for `/health` |

Logging goes to **stderr** (stdout is the MCP channel). Set `SOPHIA_MCP_LOG=debug` for
verbose output.

---

## Wire up Claude Code

Claude Code launches MCP servers over stdio. Add sophia-mcp to your `mcpServers`
config (`.mcp.json` at the project root, or your user-level Claude config):

**LOCAL (out-of-the-box):**

```json
{
  "mcpServers": {
    "mnemosyne": {
      "command": "/absolute/path/to/sophia-mcp",
      "args": ["--backend", "local"],
      "env": {
        "SOPHIA_MCP_GARDEN_BIN": "/absolute/path/to/gardend"
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
      "command": "/absolute/path/to/sophia-mcp",
      "args": [
        "--backend", "https://gateway.example.com",
        "--owner", "user:your-cognito-sub",
        "--graph", "my-graph"
      ],
      "env": {
        "SOPHIA_MCP_TOKEN": "your-service-or-jwt-token",
        "SOPHIA_MCP_ON_BEHALF_OF": "your-cognito-sub"
      }
    }
  }
}
```

Or register it from the CLI:

```bash
claude mcp add mnemosyne -- /absolute/path/to/sophia-mcp --backend local
```

Restart Claude Code; the Mnemosyne tools appear automatically.

---

## How it works

```
Claude Code ──stdio JSON-RPC──▶ sophia-mcp ──HTTP JSON-RPC──▶ backend /mcp
            (initialize,                (same 3 methods,    (LOCAL gardend
             tools/list,                 verbatim           or REMOTE gateway
             tools/call)                 passthrough)        /o/{owner}/g/{id}/mcp
                                                             + /control/mcp)
```

* **Transport in:** newline-delimited JSON-RPC 2.0 on stdin/stdout.
* **Transport out:** "streamable-http-json" — a single JSON-RPC request POSTed
  to `/mcp`, a single JSON response. (Both gardend's loopback and the gateway
  speak this; the gateway forwards it byte-for-byte.)
* **Tools:** never hardcoded. `tools/list` returns the backend's catalog (for a
  gateway: cell ∪ control, see *Multi-graph*); `tools/call` forwards
  `{name, arguments}` and returns the result envelope.

### Layout

```
src/
  main.rs              CLI entry → build backend → serve stdio
  config.rs            clap CLI + env config
  mcp.rs               JSON-RPC 2.0 wire types + MCP method/error constants
  server.rs            stdio MCP loop (agent-facing): initialize/tools/list/tools/call
  backend/
    mod.rs             the `Backend` trait + `ToolNotFound`
    remote.rs          RemoteHttp — single-endpoint reqwest MCP client + auth headers
    gateway.rs         GatewayBackend — control ∪ cell tools, graph_id routing,
                       wait-for-routable across activation
    local.rs           LocalGarden — spawn gardend, wait /health, reuse RemoteHttp
    local_lib.rs       experimental in-process variant (feature `local-garden-lib`)
tests/
  remote_proxy.rs      end-to-end RemoteHttp against a mock /mcp
  gateway_multigraph.rs  end-to-end GatewayBackend against a mock gateway
```

---

## Tests

```bash
cargo test
```

51 tests: URL resolution, auth-header construction, catalog merging (prefixing,
pagination), graph-argument parsing/normalization, the stdio dispatch (initialize
backfill, tools passthrough, notification handling, unknown method / unknown tool),
a wiremock-backed end-to-end of the direct remote proxy, and a wiremock gateway
covering the union catalog, `graph_id` routing (listed, unlisted, revoked-on-refresh,
tombstoned, shared-by-another-owner, ambiguous, disagreeing spellings, 403, 404),
activation waiting (flip-after-N, 503-forever with backoff, hung upstream, queued
waiter, `failed`, repair-required at connect and on the cell path, off-origin
pollUrl, transient 503 at kick, health-200-is-not-readiness) and mid-session
re-activation.

---

## License

MIT OR Apache-2.0.
