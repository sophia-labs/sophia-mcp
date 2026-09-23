# sophia-mcp

[![CI](https://github.com/sophia-labs/sophia-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/sophia-labs/sophia-mcp/actions/workflows/ci.yml)

A tiny **stdio MCP server** that lets Claude Code (and any other MCP client) talk
to a **Mnemosyne / garden** knowledge-graph backend.

`sophia-mcp` is a **proxy**. It forwards `initialize`, `tools/list`, and
`tools/call` to a backend, and the backend's tools are **autopopulated**.
Garden owns the graph tools; sophia-mcp owns identity, graph routing, and its
optional process-local mode controls.

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

Prebuilt binaries (linux x86_64/aarch64, macOS aarch64/x86_64) are attached to
each [GitHub Release](https://github.com/sophia-labs/sophia-mcp/releases), with
a `SHA256SUMS` file:

```bash
curl -fsSLO https://github.com/sophia-labs/sophia-mcp/releases/latest/download/sophia-mcp-vX.Y.Z-aarch64-apple-darwin.tar.gz
```

---

## Updating

```bash
sophia-mcp update --check   # is there a newer release?
sophia-mcp update           # replace this binary with it
```

`update` downloads the newest release's tarball for this platform, verifies it
against the release's `SHA256SUMS`, runs the new binary's `--version` as a
smoke test, then atomically renames it over the running executable (the
install directory must be writable). Restart your MCP clients afterwards.
`SOPHIA_MCP_UPDATE_URL=https://github.com/<fork>/sophia-mcp` points it at a
fork or mirror. A binary you built from source updates the same way, or just
`git pull && cargo build --release`.

When serving, sophia-mcp checks for a newer release at most once a day
(stamp in `~/Library/Caches/sophia-mcp` or `$XDG_CACHE_HOME/sophia-mcp`,
override with `SOPHIA_MCP_CACHE_DIR`), in the background, and prints one line
to **stderr** if there is one — never to stdout, which is the MCP channel.
Set `SOPHIA_MCP_NO_UPDATE_CHECK=1` to turn it off; it is also off whenever `CI`
is set.

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

**Restarting just works.** Stopping sophia-mcp and starting it again against
the same `--profile-dir` (the common case: restarting your MCP client) spawns
a fresh `gardend` cleanly, with no manual cleanup step. sophia-mcp clears any
`loopback.json` left over from the prior run before spawning, so it can only
ever observe the manifest the new `gardend` writes — never a stale one
pointing at a now-dead port from the process that just exited.
Closing stdin, SIGINT, and SIGTERM all release the local child before exit.

### Prerequisite: the `gardend` binary

The LOCAL backend runs garden's headless **gardend** cell binary. Garden is
**source-available**, not OSS — it's licensed under the PolyForm Noncommercial
License 1.0.0 (see garden's own `LICENSE` / `NOTICE.md`); non-commercial use,
modification, and redistribution are permitted, commercial use requires a
separate agreement with the maintainers. sophia-mcp finds the binary via, in
order:

1. `--garden-bin <path>` (or `SOPHIA_MCP_GARDEN_BIN`),
2. a `gardend` next to the `sophia-mcp` binary,
3. the sibling garden checkout's headless build —
   `../garden/src-tauri/target/release/examples/gardend`, falling back to the
   `debug` variant,
4. `gardend` on `PATH`.

Build it once from the garden repo. `gardend` is a cargo **example**, not a
`[[bin]]` — garden's desktop Tauri bundler copies every manifest `[[bin]]`
into the `.app`, so gardend is deliberately kept out of `[[bin]]` and lives
under `examples/` instead (see garden's `build-gardend-headless.sh`):

```bash
# in the garden checkout:
cargo build --release --no-default-features --features headless --example gardend
# binary lands at target/release/examples/gardend
```

Then point sophia-mcp at it if it isn't already discoverable:

```bash
sophia-mcp --garden-bin /path/to/garden/src-tauri/target/release/examples/gardend
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
# A platform-next gateway, naming the canonical owner tuple. Prefer the env
# var over --token: a token passed on the command line is visible to anyone
# who can list this machine's processes for as long as sophia-mcp runs.
SOPHIA_MCP_TOKEN="$PN_SERVICE_TOKEN" \
sophia-mcp --backend https://gateway.example.com \
     --owner user:$MY_COGNITO_SUB --graph my-graph \
     --on-behalf-of "$MY_COGNITO_SUB"

# …or an explicit Garden loopback:
SOPHIA_MCP_TOKEN="$LOOPBACK_TOKEN" sophia-mcp --backend http://127.0.0.1:8086/mcp
```

(`--token` still works, for one-off local testing or an environment that
already isolates argv, but `SOPHIA_MCP_TOKEN` is the one to reach for by
default.)

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

### Sub-MCPs

sophia-mcp can mount **additional, independently-owned MCP servers** above whichever
backend you've chosen (LOCAL, direct remote `/mcp`, or a gateway) — no change to
that backend required. A sub is any HTTP server speaking the same
"streamable-http-json" wire (POST JSON-RPC 2.0 to one URL: `tools/list`,
`tools/call`) that `RemoteHttp` already speaks to a cell.

```bash
sophia-mcp --backend local --sub layout=http://127.0.0.1:5199/mcp
```

* **Naming.** A sub reports **bare** tool names (`world`, `moves`, …); sophia-mcp
  exposes them to the agent as `<prefix>_<name>` (`layout_world`, `layout_moves`),
  descriptions and schemas passed through verbatim. A `tools/call` whose name
  starts with `<prefix>_` routes to that sub with the bare name, arguments
  untouched. This generalizes the `control_` merge `GatewayBackend` already does
  for the gateway's own control plane (see *Multi-graph* above) to any number of
  independently-owned upstreams — it does not change that merge, and a
  `ComposedBackend` may itself wrap a `GatewayBackend`.
* **Config.** `--sub <prefix>=<url>` (repeatable; env `SOPHIA_MCP_SUBS` as a
  comma-separated list of the same `prefix=url` pairs) and `--sub-token
  <prefix>=<token>` (repeatable; env `SOPHIA_MCP_SUB_TOKENS`) for a per-sub
  bearer, sent only on that sub's requests — never on the primary backend's, and
  never on another sub's. `prefix` must match `[a-z][a-z0-9]*` (refused at config
  parse otherwise, e.g. `Layout=`, `1x=`).
* **Resilience.** Each sub is probed with one `tools/list` call when sophia-mcp
  starts. A sub that fails that probe is logged to stderr (prefix + error —
  never the token) and **skipped** for the rest of the process's life: the
  primary's own catalog still serves, and `tools/call` to `<prefix>_<name>` for a
  skipped sub is a JSON-RPC `-32601` naming the tool, without ever touching the
  network. A sub error during a live `tools/call` surfaces as `BACKEND_ERROR` with
  the chain, exactly like the primary's own upstream failures.

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
   off-origin URL is refused rather than sent the bearer token — and never
   follows HTTP redirects (a `3xx` is surfaced as a gateway rejection naming
   the `Location`; reqwest would keep `x-pn-on-behalf-of` across hosts and the
   redirected body would otherwise be trusted as an activation record). A
   retryable `5xx` at the connect-time kick is logged, not fatal
   (`graph_repair_required` is typed non-retryable and surfaced at once).

`--request-timeout` now bounds every backend alike (LOCAL's proxy to
gardend's loopback, a direct `--backend <url>/mcp`, every sub-MCP, and the
gateway) — set as the reqwest client's own total-request timeout
(connect + send + full response read) at `build_client`, not just the
gateway's manual activation-wait bookkeeping. Every HTTP response body is
also read incrementally and capped at 10 MiB (`remote::MAX_RESPONSE_BYTES`),
so an oversized or slow-drip response can't grow unbounded memory before
sophia-mcp notices and aborts the read.

Progress is logged to stderr only (`SOPHIA_MCP_LOG=info`).

---

## MCP modes

To run one MCP process as an agent with graph-defined modes, supply its
canonical agent ID and bound graph:

```sh
sophia-mcp --backend local --graph my-graph --agent-id agent-deadbeef
# The same flags work with a hosted gateway plus --owner and its normal auth.
```

The agent's `agt:defaultMode` and `agt:mayUseMode` assignments and each
`agt:Mode` definition live in the bound graph's
`urn:mnemosyne:local:graph:{graphId}:user:rdf` partition. The proxy reads them
through the backend's existing `sparql_query` MCP tool. Shrubbery's Modes
editor writes this RDF; no new Garden route or hosted gateway route is needed.
This works for local gardend, direct Garden `/mcp`, and the hosted gateway.
Create the graph, agent, and mode assignments first with a trusted graph writer
(for example Shrubbery's Modes editor). An empty local profile has no mode
assignment to select.

With `--agent-id`, three proxy tools are always available:

| Tool | Purpose |
|---|---|
| `sophia_mode_status` | Show the selected mode and whether its assignment and definition are current |
| `sophia_mode_list` | List this agent's assigned modes |
| `sophia_mode_set` | Select an assigned mode by its exact `modeIri` for this MCP process |

The initial selection is `agt:defaultMode`. If there is no default, only the
mode control tools appear until a mode is selected. `tools/list` includes only
tools in the current mode, and `tools/call` independently rejects calls outside
it. A read mode admits only tools whose Garden scope metadata proves a read
effect; unknown effects are withheld. Graph scopes fence the bound graph and
any `graphId` or `graph_id` argument; a scoped mode also withholds tools that
lack Garden's scope metadata. Approval-required calls are denied until
there is a human approval queue. Switching sends
`notifications/tools/list_changed` so MCP clients can refresh discovery.

Selection is process-local and resets to the default on restart. The proxy
pins the initially assigned mode definitions, re-reads the graph before each
call, and fails closed if the active mode was edited or revoked. New modes and
edits become selectable after a proxy restart. The underlying Garden/gateway
credential and its ACLs remain the authority ceiling. A credential that can
edit its own mode RDF can change what a *future* proxy process may select; use
a separate trusted graph writer when modes need to serve as durable policy.
Choreograph Sessions have their own launch-time mode pin; changing this MCP
process's mode does not change an already running Choreograph Session.

---

## CLI / config

Every flag has an env var twin.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--backend` | `SOPHIA_MCP_BACKEND` | `local` | `local`, or a backend URL |
| `--token` | `SOPHIA_MCP_TOKEN` | — | bearer token for REMOTE. **Prefer the env var** — a token on the command line is visible to anyone who can list this machine's processes for as long as sophia-mcp runs |
| `--on-behalf-of` | `SOPHIA_MCP_ON_BEHALF_OF` | — | gateway service-auth subject |
| `--user-id` | `SOPHIA_MCP_USER_ID` | — | `X-User-ID` side-channel |
| `--owner` | `SOPHIA_MCP_OWNER` | — | stable typed owner required for cloud-2 |
| `--graph` | `SOPHIA_MCP_GRAPH` | — | local graph id required for cloud-2 (the *bound* graph) |
| `--agent-id` | `SOPHIA_MCP_AGENT_ID` | — | enable process-scoped MCP modes for canonical `agent-<hex>` in `--graph` |
| `--allow-insecure-http` | `SOPHIA_MCP_ALLOW_INSECURE_HTTP` | `false` | allow sending a bearer over plain `http://` to a non-loopback host (loopback is always allowed regardless) |
| `--activation-timeout` | `SOPHIA_MCP_ACTIVATION_TIMEOUT` | `300` | seconds to wait for a cell to become routable (gateway-only) |
| `--activation-poll` | `SOPHIA_MCP_ACTIVATION_POLL` | `2` | seconds between activation polls; base of the re-probe backoff (gateway-only) |
| `--request-timeout` | `SOPHIA_MCP_REQUEST_TIMEOUT` | `120` | per-request ceiling (connect + send + full response read) for any single HTTP request — applies to every backend, not just the gateway |
| `--unified-mcp-fallback` | `SOPHIA_MCP_UNIFIED_MCP_FALLBACK` | `false` | also try `{base}/mcp` for control tools when `/control/mcp` is 404 (gateway-only) |
| `--profile-dir` | `SOPHIA_MCP_PROFILE_DIR` | `~/.sophia-mcp/profile` | LOCAL data dir |
| `--garden-bin` | `SOPHIA_MCP_GARDEN_BIN` | auto-discover | LOCAL `gardend` path |
| `--local-port` | `SOPHIA_MCP_LOCAL_PORT` | `0` (OS-assigned) | LOCAL loopback port |
| `--local-health-timeout` | `SOPHIA_MCP_LOCAL_HEALTH_TIMEOUT` | `30` | seconds to wait for `/health` |
| `--sub` | `SOPHIA_MCP_SUBS` | — | mount a sub-MCP, `<prefix>=<url>` (repeatable; env is comma-separated) |
| `--sub-token` | `SOPHIA_MCP_SUB_TOKENS` | — | bearer for one sub, `<prefix>=<token>` (repeatable; env is comma-separated) |

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
* **Tools:** graph tools come from the backend's catalog (for a gateway: cell ∪
  control, see *Multi-graph*). With `--agent-id`, the proxy adds its three mode
  controls and fences both discovery and calls to the selected mode.
* **`structuredContent` is always an object at the client:** MCP requires it, and Claude Code rejects anything else; when an upstream answers with an array (the gateway control plane's `list_graphs` does) sophia-mcp wraps it as `{"items": [...]}` (a scalar as `{"value": …}`), leaving objects and `content` untouched — logged at `debug` once per tool.
* **Error messages to the client are bounded and redacted; full detail goes to stderr.** A failed call's full `anyhow` chain is always logged (`tracing::error!`) for operators; the JSON-RPC error message the MCP client actually sees is capped at 2 KB, and any raw upstream HTTP body embedded along the way is separately capped at 500 bytes with an explicit truncation marker before it's ever interpolated into a message — never shipped whole to an untrusted-by-default client. This is a real error-contract behavior change from 0.2.x, where the full chain (including full upstream bodies) went straight to the client — one reason this release is 0.3.0.

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
    composed.rs        ComposedBackend — mounts namespaced `--sub` MCPs above any backend
tests/
  remote_proxy.rs      end-to-end RemoteHttp against a mock /mcp
  gateway_multigraph.rs  end-to-end GatewayBackend against a mock gateway
  composed_subs.rs     end-to-end ComposedBackend against mock primary + sub servers
```

---

## Tests

```bash
cargo test
```

101 tests: URL resolution, auth-header construction, catalog merging (prefixing,
pagination), graph-argument parsing/normalization, sub-MCP prefix parsing/validation,
`gardend` discovery resolution order (explicit `--garden-bin` wins and errors if
missing, exe-adjacent, sibling-checkout `examples/` release then debug, `PATH`
fallback last), loopback-manifest parsing (with and without the now-optional
`token` field), restart safety (a stale `loopback.json` from a prior run is
cleared before spawning, and a pid-mismatched manifest is never trusted),
client-facing error redaction (a huge or secret-bearing upstream/backend error
never reaches the MCP client unbounded, at both the per-site body redaction
and the final client-message cap), the plain-`http://`-plus-bearer refusal
(loopback always allowed, `--allow-insecure-http` overrides elsewhere), and
the response-size cap enforced during the read rather than after, the
stdio dispatch (initialize backfill, tools passthrough, `structuredContent`
normalization, notification handling, unknown method / unknown tool),
a wiremock-backed end-to-end of the direct remote proxy, a wiremock gateway
covering the union catalog, `graph_id` routing (listed, unlisted, revoked-on-refresh,
tombstoned, shared-by-another-owner, ambiguous, disagreeing spellings, 403, 404),
activation waiting (flip-after-N, 503-forever with backoff, hung upstream, queued
waiter, `failed`, repair-required at connect and on the cell path, off-origin
pollUrl, transient 503 at kick, health-200-is-not-readiness) and mid-session
re-activation, and a wiremock `ComposedBackend` covering the merged catalog
(descriptions verbatim), `<prefix>_<name>` routing through the real stdio dispatch,
a non-prefixed call still reaching the primary, a sub whose `tools/list` 500s at
start being skipped (`layout_x` → `-32601`, no network touched), and per-sub
bearer tokens never crossing to the primary or another sub.

---

## License

Licensed under the [MIT license](LICENSE-MIT).

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you shall be licensed as above,
without any additional terms or conditions.
