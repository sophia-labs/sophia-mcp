# Changelog

## 0.6.2 — 2026-10-12

- **Behind Cloudflare Access.** `--cf-access-client-id` /
  `--cf-access-client-secret` (env `CF_ACCESS_CLIENT_ID` /
  `CF_ACCESS_CLIENT_SECRET`) send an Access service token as
  `CF-Access-Client-Id` / `CF-Access-Client-Secret`, so sophia-mcp can reach a
  self-hosted Sophia behind a Cloudflare Tunnel and Access application directly
  (`--backend https://sophia.example.com/mcp`). See README *Behind Cloudflare
  Access*.
- **Extra request headers.** `--header 'Name: value'` (repeatable) and
  `SOPHIA_MCP_HEADERS` (one per line, or `;`-separated). Like the Access pair,
  they go on every request to the remote `--backend` and nowhere else: never a
  `--sub`, never a redirect (none are followed), and never plain `http://` to a
  non-loopback host without `--allow-insecure-http`. Values are never logged.
  Headers sophia-mcp sets itself (`Authorization`, `Origin`,
  `x-pn-on-behalf-of`, `X-User-ID`), framing headers and duplicate names are
  refused. Ignored for `--backend local`.
- **Access refusals are named.** A redirect to Access's login page, a non-JSON
  401/403 from the Cloudflare edge, or an HTML page where JSON was expected now
  fails with an error that says Cloudflare Access refused and what to check,
  instead of a JSON parse error. Sophia's own JSON 401/403 and a proxy's HTML
  5xx keep their previous handling; a Cloudflare bot challenge is named as such.
- `--help` no longer prints the values of `SOPHIA_MCP_TOKEN`,
  `SOPHIA_MCP_SUB_TOKENS` or `CF_ACCESS_CLIENT_SECRET` when they are set.

## 0.6.1 — 2026-09-23

- **Per-sub acting identity.** `--sub-on-behalf-of <prefix>=<subject>`
  (env `SOPHIA_MCP_SUB_ON_BEHALF_OF`) sends `x-pn-on-behalf-of` to that one
  sub-MCP, alongside its own `--sub-token`. It is refused without a matching
  `--sub` and bearer, and the primary backend's acting user is never reused
  for a sub. This is what mounts Sirin's hosted notebook MCP
  (`/o/{owner}/g/{graph}/notebooks/mcp`) as `notebooks_*` tools; graph-defined
  modes filter those names like any other tool.

## 0.6.0 — 2026-09-23

- **Declare your agent over MCP.** Every graph-bound session exposes
  `sophia_agent_declare { agentId }`, `sophia_agent_status` and
  `sophia_agent_clear`. Undeclared sessions keep the full catalogue. Declaring
  selects the agent's default mode (or controls only), adds the
  `sophia_mode_*` tools and sends `notifications/tools/list_changed`;
  clearing restores the full catalogue. `--agent-id` is now an optional
  startup preset.
- **Live mode definitions.** Replaces 0.4.0's pin-at-start/fail-closed-on-edit:
  the agent's modes are re-read before `tools/list`/`tools/call` (TTL
  `--mode-cache-ttl-ms`, default 3 s) and polled in the background
  (`--mode-poll-ms`, default 15 s). Edits and new modes apply without a
  restart and emit `list_changed`; a revoked active mode falls back to the
  default with a notice on the next call; an unreadable graph fails calls
  closed with a retryable error while controls stay available.
- The stdio server now writes backend-initiated notifications between
  responses (stdout stays pure JSON-RPC).
- A declared agent is a claim by the caller; binding agent identity to
  credentials is future work (see README *MCP modes*).

## 0.5.0 — 2026-09-23

- **Self-update.** `sophia-mcp update` replaces the running binary with the
  newest GitHub Release after verifying its tarball against the release's
  `SHA256SUMS` and proving the new binary runs (`--version`). `sophia-mcp
  update --check` only reports. `SOPHIA_MCP_UPDATE_URL` points at a fork or
  mirror.
- **Startup update check.** At most once a day (stamp under the user cache
  dir), non-blocking, and on **stderr only** — stdout stays the MCP JSON-RPC
  channel. Disabled by `SOPHIA_MCP_NO_UPDATE_CHECK=1` and whenever `CI` is set.
- **Release pipeline.** A `v*` tag builds linux x86_64/aarch64 and macOS
  aarch64/x86_64 tarballs and publishes them with `SHA256SUMS` and
  `release.json` on a GitHub Release.

## 0.4.0 and earlier

See the git history.
