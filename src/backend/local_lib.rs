//! EXPERIMENTAL in-process LocalGarden variant (feature `local-garden-lib`).
//!
//! This module is the integration seam for linking garden AS A CRATE and
//! booting its loopback server in-process, instead of spawning the `gardend`
//! binary (see `local.rs`). It is OFF by default and intentionally does not
//! compile-link garden, so the core build is never affected.
//!
//! ============================ EXACT BLOCKER ============================
//! garden's headless entrypoint is:
//!
//!     garden_lib::headless::setup(handle: &garden_lib::app_runtime::AppHandle)
//!         -> Result<RuntimeMode, Box<dyn Error>>
//!
//! It REQUIRES a Tauri MockRuntime `AppHandle`, and the only supported way to
//! mint one headlessly (per `garden/src-tauri/src/bin/gardend.rs:99-103`) is:
//!
//!     tauri::test::mock_builder().build(tauri::generate_context!())
//!
//! `tauri::generate_context!()` is a build-time macro that needs garden's
//! `tauri.conf.json` + build context and garden's `build.rs` (tauri-build).
//! To use it from neem, neem would have to:
//!
//!   1. Add garden as a path/git Cargo dependency with
//!      `default-features = false, features = ["headless"]` (which activates
//!      `tauri/test`). garden's `[package] publish = false`, so it can only be a
//!      path/git dep, never crates.io.
//!   2. Inherit garden's ENTIRE build: `tauri-build` + `generate_context!`,
//!      plus its heavy runtime deps — oxigraph, candle-core,
//!      fastembed/onnxruntime (native ORT), turso, yrs, axum, reqwest. This is a
//!      multi-minute, native-toolchain-heavy build that defeats neem's
//!      "near-zero config, low-friction" goal.
//!   3. Replicate gardend's main: leak a multi-thread tokio runtime, install it
//!      via `tauri::async_runtime::set` BEFORE any async touches it, build the
//!      mock app, call `setup(app.handle())`, then read back the chosen port +
//!      token from `<profile_dir>/loopback.json` (the loopback server is spawned
//!      as a fire-and-forget task and returns no handle/port to the caller).
//!
//! NET: even in-process you recover the endpoint by reading `loopback.json` —
//! exactly as the subprocess path does — so in-process buys nothing but a much
//! heavier build. That is why the subprocess path in `local.rs` is the default
//! and recommended LocalGarden.
//!
//! ============================ INTEGRATION SEAM ============================
//! To complete this variant:
//!   * In Cargo.toml, add (manually, under the `local-garden-lib` feature):
//!         [dependencies]
//!         garden = { path = "../garden/src-tauri", package = "garden",
//!                    default-features = false, features = ["headless"],
//!                    optional = true }
//!     and make the feature activate it: `local-garden-lib = ["dep:garden"]`.
//!   * Implement `start_in_process` below using the gardend main recipe.
//!   * Return a `RemoteHttp` against the loopback `http://127.0.0.1:<port>/mcp`
//!     read from `loopback.json`, mirroring `local.rs`.
//!
//! Until then, this variant is a stub that errors clearly.

use std::path::PathBuf;

use anyhow::anyhow;

use super::RemoteHttp;

pub struct LocalGardenLibOptions {
    pub profile_dir: PathBuf,
    pub port: u16,
}

/// TODO(local-garden-lib): implement in-process boot per the recipe above.
/// Blocked on accepting garden as a path Cargo dependency (heavy build +
/// tauri-build/generate_context!). The subprocess path (`LocalGarden`) is the
/// supported local backend.
pub async fn start_in_process(_opts: LocalGardenLibOptions) -> anyhow::Result<RemoteHttp> {
    Err(anyhow!(
        "in-process LocalGarden (feature `local-garden-lib`) is not implemented: \
         it requires linking garden as a Cargo dependency and minting a Tauri \
         MockRuntime AppHandle via generate_context!. Use the default subprocess \
         backend (`--backend local`, which spawns `gardend`) instead. See \
         src/backend/local_lib.rs for the full blocker + integration seam."
    ))
}
