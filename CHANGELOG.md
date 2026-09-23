# Changelog

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
