//! `LocalGarden` — the out-of-the-box hero backend.
//!
//! Spawns the prebuilt headless `gardend` binary as a subprocess (configured
//! ENTIRELY by env vars — gardend takes no CLI args), waits for its `/health`,
//! discovers the loopback port + token, then delegates every MCP call to a
//! [`RemoteHttp`] pointed at `http://127.0.0.1:<port>/mcp`.
//!
//! Why subprocess and not an in-process garden Cargo dependency: the in-process
//! entrypoint (`garden_lib::headless::setup`) needs a Tauri MockRuntime
//! `AppHandle` minted via `generate_context!`, dragging garden's entire
//! tauri-build apparatus + heavy deps (oxigraph, candle, fastembed/onnxruntime,
//! turso, yrs) into a tool meant to be near-zero-friction. The whole rest of the
//! system already treats gardend as a process/image (the platform-next gateway
//! runs the `gardend` container and never links it). sophia-mcp mirrors that. The
//! in-process variant is scaffolded behind the `local-garden-lib` feature in
//! `local_lib.rs` with the exact blocker.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

use super::{AuthHeaders, Backend, RemoteHttp};

/// Subset of gardend's `loopback.json` manifest sophia-mcp needs (see garden
/// `src-tauri/src/loopback_state.rs`). Field names are camelCase on the wire.
///
/// `token` is `Option`: sophia-mcp mints its own token and injects it into
/// gardend's environment *before* spawning it (see `start()` below), so it
/// already knows the token without ever reading it back from disk. Garden's
/// manifest DTO is moving toward secret-free (a parallel workstream drops
/// `token` from what it writes to `loopback.json`), and this proxy must not
/// fail to deserialize — with a misleading timeout, no less — on that day.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoopbackManifest {
    port: u16,
    api_url: String,
    mcp_url: String,
    token: Option<String>,
}

/// Options for launching the local headless garden.
pub struct LocalGardenOptions {
    /// Profile/data dir (`GARDEN_PROFILE_DIR`). Created on first run by gardend.
    pub profile_dir: PathBuf,
    /// Explicit `gardend` binary path, or `None` to auto-discover.
    pub garden_bin: Option<PathBuf>,
    /// Loopback port (`GARDEN_LOOPBACK_PORT`). `0` = OS-assigned.
    pub port: u16,
    /// How long to wait for `/health` to come up.
    pub health_timeout: Duration,
}

pub struct LocalGarden {
    /// Kept alive for the lifetime of the proxy; dropping it kills gardend.
    _child: Child,
    remote: RemoteHttp,
    /// Resolved loopback MCP endpoint; exposed via [`LocalGarden::mcp_url`] for
    /// diagnostics.
    #[allow(dead_code)]
    mcp_url: String,
}

impl LocalGarden {
    /// Spawn gardend, wait for readiness, and build the inner proxy.
    pub async fn start(opts: LocalGardenOptions) -> anyhow::Result<Self> {
        let bin = resolve_gardend_bin(opts.garden_bin.as_deref())?;
        std::fs::create_dir_all(&opts.profile_dir).with_context(|| {
            format!(
                "create sophia-mcp profile dir {}",
                opts.profile_dir.display()
            )
        })?;

        // We generate a fixed token so we don't have to race gardend's manifest
        // write for it — but we STILL read the manifest to learn the chosen port
        // when port == 0 (OS-assigned), which is the default. gardend is driven
        // purely by env; no CLI args.
        let token = uuid::Uuid::new_v4().simple().to_string();

        tracing::info!(
            binary = %bin.display(),
            profile = %opts.profile_dir.display(),
            "starting headless gardend"
        );

        let child = Command::new(&bin)
            .env("GARDEN_PROFILE_DIR", &opts.profile_dir)
            .env("GARDEN_LOOPBACK_HOST", "127.0.0.1")
            .env("GARDEN_LOOPBACK_PORT", opts.port.to_string())
            .env("GARDEN_LOOPBACK_TOKEN", &token)
            // gardend logs to stderr; let it flow to sophia-mcp's stderr (stdout is the
            // MCP channel and must stay clean). Deliberately NOT Stdio::inherit()
            // for stdout: sophia-mcp's own stdout IS the stdio JSON-RPC channel to
            // its client, so inheriting it would hand gardend a direct line to
            // corrupt that channel with any stray write (a panic message, a
            // leftover println!, a library that defaults to stdout logging).
            // Stdio::null() discards it unconditionally rather than trusting that
            // gardend never writes there.
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn gardend at {}", bin.display()))?;

        // Discover the loopback endpoint from the manifest gardend writes into
        // the profile dir. When port is fixed (non-zero) we already know it, but
        // reading the manifest also confirms gardend booted far enough to bind.
        let manifest_path = opts.profile_dir.join("loopback.json");
        let manifest = wait_for_manifest(&manifest_path, opts.health_timeout)
            .await
            .context("waiting for gardend loopback manifest (loopback.json)")?;

        let mcp_url = manifest.mcp_url.clone();
        tracing::info!(port = manifest.port, mcp_url = %mcp_url, "gardend loopback manifest found");

        let remote = RemoteHttp::new(
            mcp_url.clone(),
            AuthHeaders {
                // Prefer the token we minted and handed gardend via
                // GARDEN_LOOPBACK_TOKEN above — we already know it, no need to
                // trust the disk manifest for it. Fall back to manifest.token
                // only if it's ever present and our own token were somehow
                // unavailable; this keeps the auth path alive on the day
                // Garden's manifest DTO drops `token` entirely.
                bearer: Some(token.clone()).or_else(|| manifest.token.clone()),
                // gardend's origin_ok requires a loopback / null Origin.
                origin: Some("http://127.0.0.1".to_string()),
                ..Default::default()
            },
        )?;

        // Poll /health until ready (the manifest can land a beat before the
        // server is serving requests).
        wait_for_health(&manifest.api_url, opts.health_timeout)
            .await
            .context("waiting for gardend /health")?;

        Ok(Self {
            _child: child,
            remote,
            mcp_url,
        })
    }

    /// The resolved loopback MCP endpoint (`http://127.0.0.1:<port>/mcp`).
    #[allow(dead_code)]
    pub fn mcp_url(&self) -> &str {
        &self.mcp_url
    }
}

#[async_trait]
impl Backend for LocalGarden {
    async fn initialize(&self, client_params: Value) -> anyhow::Result<Value> {
        self.remote.initialize(client_params).await
    }
    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        self.remote.list_tools(params).await
    }
    async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
        self.remote.call_tool(params).await
    }
}

/// Find the `gardend` binary: explicit `--garden-bin` (env `SOPHIA_MCP_GARDEN_BIN`,
/// see `src/config.rs`) override, then a `gardend` next to the sophia-mcp
/// executable, then the sibling garden checkout's headless example target
/// (release, then debug), then bare `gardend` left for the OS to resolve on
/// `PATH` at spawn time.
///
/// Thin wrapper around [`resolve_gardend_bin_from`] that supplies the real
/// process `current_exe`/`current_dir`; kept separate so tests can drive the
/// resolution logic with temp-dir fixtures instead of mutating real process
/// state (`current_dir` is process-global and unsafe to change under
/// parallel tests).
fn resolve_gardend_bin(explicit: Option<&Path>) -> anyhow::Result<PathBuf> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    resolve_gardend_bin_from(explicit, exe_dir.as_deref(), &cwd)
}

/// Pure discovery logic. Resolution order:
///
/// 1. `explicit` (`--garden-bin` / `SOPHIA_MCP_GARDEN_BIN`) — used as-is if it
///    exists, else an error (never silently falls through).
/// 2. `<exe_dir>/gardend` — a `gardend` placed next to the sophia-mcp binary.
/// 3. `<cwd>/../garden/src-tauri/target/release/examples/gardend`, then the
///    `debug` variant — the sibling garden checkout's headless build.
///    `gardend` is a cargo `[[example]]`, never a `[[bin]]` (garden's
///    `src-tauri/Cargo.toml`: the desktop Tauri bundler copies every `[[bin]]`
///    into the app bundle, so gardend is kept out of `[[bin]]` on purpose) —
///    its real artifact always lands under `target/<profile>/examples/`, so
///    the pre-`examples/` paths this function used to check could never exist
///    and are dropped rather than kept as dead fallbacks.
/// 4. Bare `gardend`, left for the OS to resolve via `PATH` at spawn time.
fn resolve_gardend_bin_from(
    explicit: Option<&Path>,
    exe_dir: Option<&Path>,
    cwd: &Path,
) -> anyhow::Result<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() {
            return Ok(p.to_path_buf());
        }
        return Err(anyhow!(
            "gardend binary not found at --garden-bin {}",
            p.display()
        ));
    }

    let mut candidates: Vec<PathBuf> = Vec::new();
    // Next to the sophia-mcp binary.
    if let Some(dir) = exe_dir {
        candidates.push(dir.join("gardend"));
    }
    // Sibling garden checkout's headless example target (dev convenience).
    candidates.push(cwd.join("../garden/src-tauri/target/release/examples/gardend"));
    candidates.push(cwd.join("../garden/src-tauri/target/debug/examples/gardend"));
    for cand in &candidates {
        if cand.exists() {
            return Ok(cand.clone());
        }
    }
    // Fall back to letting the OS resolve `gardend` on PATH at spawn time.
    Ok(PathBuf::from("gardend"))
}

async fn wait_for_manifest(path: &Path, timeout: Duration) -> anyhow::Result<LoopbackManifest> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(body) => match serde_json::from_str::<LoopbackManifest>(&body) {
                    Ok(m) => return Ok(m),
                    Err(e) => tracing::debug!("manifest not yet parseable: {e}"),
                },
                Err(e) => tracing::debug!("manifest not yet readable: {e}"),
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out after {:?} waiting for {}",
                timeout,
                path.display()
            ));
        }
        sleep(Duration::from_millis(150)).await;
    }
}

async fn wait_for_health(api_url: &str, timeout: Duration) -> anyhow::Result<()> {
    let health_url = format!("{}/health", api_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let deadline = Instant::now() + timeout;
    loop {
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => tracing::debug!("/health not ready: HTTP {}", resp.status()),
            Err(e) => tracing::debug!("/health unreachable: {e}"),
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out after {:?} waiting for {health_url}",
                timeout
            ));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique scratch directory under the OS temp dir, removed on drop.
    /// Hand-rolled rather than pulling in a `tempfile` dev-dependency — these
    /// tests only need "an empty dir nobody else is using", checked purely
    /// via `Path::exists()` on dummy files (no real gardend needed).
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "sophia-mcp-local-rs-test-{tag}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        /// Create an empty dummy file at `self.path().join(rel)`, creating
        /// parent dirs as needed, and return its full path.
        fn touch(&self, rel: &str) -> PathBuf {
            let p = self.0.join(rel);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).expect("create parent dirs");
            }
            std::fs::write(&p, b"").expect("write dummy file");
            p
        }

        /// A subdirectory to use as `cwd`, so `cwd.join("../garden/...")`
        /// lands back on files touched at this scratch dir's root.
        fn subdir_cwd(&self) -> PathBuf {
            let cwd = self.0.join("cwd");
            std::fs::create_dir_all(&cwd).expect("create cwd subdir");
            cwd
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    // ---- discovery resolution order ----

    #[test]
    fn explicit_path_wins_even_when_other_candidates_exist() {
        let scratch = ScratchDir::new("explicit-wins");
        let explicit_bin = scratch.touch("explicit/gardend");
        let exe_dir = scratch.path().join("exe-dir");
        scratch.touch("exe-dir/gardend"); // a competing, otherwise-valid candidate
        let cwd = scratch.subdir_cwd();

        let resolved = resolve_gardend_bin_from(Some(&explicit_bin), Some(&exe_dir), &cwd).unwrap();
        assert_eq!(resolved, explicit_bin);
    }

    #[test]
    fn explicit_path_errors_when_missing() {
        let scratch = ScratchDir::new("explicit-missing");
        let missing = scratch.path().join("no-such-gardend");
        let cwd = scratch.subdir_cwd();

        let err = resolve_gardend_bin_from(Some(&missing), None, &cwd).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("--garden-bin") && msg.contains("no-such-gardend"),
            "error should name --garden-bin and the missing path: {msg}"
        );
    }

    #[test]
    fn exe_adjacent_gardend_found_and_wins_over_sibling_checkout() {
        let scratch = ScratchDir::new("exe-adjacent");
        let exe_dir = scratch.path().join("exe-dir");
        let exe_bin = scratch.touch("exe-dir/gardend");
        // A sibling-checkout candidate is ALSO present; exe-adjacent must win.
        scratch.touch("garden/src-tauri/target/release/examples/gardend");
        let cwd = scratch.subdir_cwd();

        let resolved = resolve_gardend_bin_from(None, Some(&exe_dir), &cwd).unwrap();
        assert_eq!(resolved, exe_bin);
    }

    #[test]
    fn sibling_release_examples_gardend_is_found() {
        let scratch = ScratchDir::new("sibling-release");
        let expected = scratch.touch("garden/src-tauri/target/release/examples/gardend");
        let cwd = scratch.subdir_cwd();

        let resolved = resolve_gardend_bin_from(None, None, &cwd).unwrap();
        assert_eq!(
            resolved.canonicalize().unwrap(),
            expected.canonicalize().unwrap(),
            "expected the sibling checkout's release examples/gardend, got {}",
            resolved.display()
        );
    }

    #[test]
    fn sibling_release_examples_wins_over_debug_when_both_present() {
        let scratch = ScratchDir::new("sibling-release-over-debug");
        let release = scratch.touch("garden/src-tauri/target/release/examples/gardend");
        scratch.touch("garden/src-tauri/target/debug/examples/gardend");
        let cwd = scratch.subdir_cwd();

        let resolved = resolve_gardend_bin_from(None, None, &cwd).unwrap();
        assert_eq!(
            resolved.canonicalize().unwrap(),
            release.canonicalize().unwrap()
        );
    }

    #[test]
    fn sibling_debug_examples_gardend_is_found_when_release_missing() {
        let scratch = ScratchDir::new("sibling-debug");
        let debug = scratch.touch("garden/src-tauri/target/debug/examples/gardend");
        let cwd = scratch.subdir_cwd();

        let resolved = resolve_gardend_bin_from(None, None, &cwd).unwrap();
        assert_eq!(
            resolved.canonicalize().unwrap(),
            debug.canonicalize().unwrap()
        );
    }

    #[test]
    fn path_fallback_is_last_when_nothing_else_found() {
        let scratch = ScratchDir::new("path-fallback");
        let cwd = scratch.subdir_cwd();
        // No explicit, no exe dir, no sibling checkout present at all.

        let resolved = resolve_gardend_bin_from(None, None, &cwd).unwrap();
        assert_eq!(resolved, PathBuf::from("gardend"));
    }

    // ---- manifest parsing ----

    #[test]
    fn manifest_without_token_field_parses() {
        let json = r#"{
            "port": 8086,
            "apiUrl": "http://127.0.0.1:8086",
            "mcpUrl": "http://127.0.0.1:8086/mcp"
        }"#;
        let manifest: LoopbackManifest = serde_json::from_str(json)
            .expect("manifest without `token` must still parse (Garden is going secret-free)");
        assert_eq!(manifest.port, 8086);
        assert_eq!(manifest.token, None);
    }

    #[test]
    fn legacy_manifest_with_token_field_still_parses() {
        let json = r#"{
            "port": 8086,
            "apiUrl": "http://127.0.0.1:8086",
            "mcpUrl": "http://127.0.0.1:8086/mcp",
            "token": "abc123"
        }"#;
        let manifest: LoopbackManifest =
            serde_json::from_str(json).expect("legacy manifest with `token` must still parse");
        assert_eq!(manifest.token.as_deref(), Some("abc123"));
    }
}
