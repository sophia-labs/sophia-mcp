# Changelog

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
