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
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoopbackManifest {
    port: u16,
    api_url: String,
    mcp_url: String,
    token: String,
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
            // MCP channel and must stay clean).
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
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
                bearer: Some(manifest.token.clone()),
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

/// Find the `gardend` binary: explicit override, then `$GARDEN_BIN`/PATH-ish
/// candidates, then the sibling garden checkout's release target.
fn resolve_gardend_bin(explicit: Option<&Path>) -> anyhow::Result<PathBuf> {
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
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("gardend"));
        }
    }
    // Sibling garden checkout relative to cwd (dev convenience).
    candidates.push(PathBuf::from(
        "../garden/src-tauri/target/release/gardend",
    ));
    candidates.push(PathBuf::from("../garden/src-tauri/target/debug/gardend"));
    // Bare name (let the OS resolve via PATH on spawn).
    for cand in &candidates {
        if cand.exists() {
            return Ok(cand.clone());
        }
    }
    // Fall back to letting the OS resolve `gardend` on PATH at spawn time.
    Ok(PathBuf::from("gardend"))
}

async fn wait_for_manifest(
    path: &Path,
    timeout: Duration,
) -> anyhow::Result<LoopbackManifest> {
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
