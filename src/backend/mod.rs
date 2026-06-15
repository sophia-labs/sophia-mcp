//! The `Backend` trait — the seam every MCP backend implements, and which the
//! stdio server proxies to.
//!
//! There are exactly two concrete backends:
//!   * [`RemoteHttp`] — a reqwest MCP client to an existing backend URL with
//!     auth headers. Used directly for `--backend <url>`, and reused internally
//!     by the local backend once garden's loopback is up.
//!   * [`LocalGarden`] — starts a headless `gardend` subprocess, then delegates
//!     to a `RemoteHttp` pointed at its loopback `/mcp`. (`--backend local`)
//!
//! Both speak the identical MCP wire shape; the only difference is whether neem
//! *starts* the backend or merely *connects* to it.

use async_trait::async_trait;
use serde_json::Value;

mod remote;
pub use remote::{AuthHeaders, RemoteHttp};

pub mod local;
pub use local::LocalGarden;

#[cfg(feature = "local-garden-lib")]
pub mod local_lib;

/// A proxy target. Methods mirror the three MCP methods neem forwards; tool
/// payloads stay as opaque `Value`s — garden owns the schema, neem passes it
/// through.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Forward `initialize`. `client_params` is the agent's `initialize` params
    /// (capabilities, clientInfo). The backend returns the server's
    /// `initialize` result verbatim.
    async fn initialize(&self, client_params: Value) -> anyhow::Result<Value>;

    /// Forward `tools/list`. Returns the backend's result object, which contains
    /// the `tools` array (autopopulated catalog) and optional `nextCursor`.
    async fn list_tools(&self, params: Value) -> anyhow::Result<Value>;

    /// Forward `tools/call`. `params` is `{ name, arguments }`. Returns the
    /// backend's result object (`{ content, structuredContent, isError? }`).
    async fn call_tool(&self, params: Value) -> anyhow::Result<Value>;
}
