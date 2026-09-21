//! The `Backend` trait — the seam every MCP backend implements, and which the
//! stdio server proxies to.
//!
//! There are three concrete backends:
//!   * [`RemoteHttp`] — a reqwest MCP client to one existing endpoint with auth
//!     headers. Used directly for an explicit `--backend <url>/mcp`, and reused
//!     internally by the local backend once garden's loopback is up.
//!   * [`LocalGarden`] — starts a headless `gardend` subprocess, then delegates
//!     to a `RemoteHttp` pointed at its loopback `/mcp`. (`--backend local`)
//!   * [`GatewayBackend`] — a platform-next gateway base URL + `--owner` +
//!     `--graph`: the union of the gateway's control-plane tools and the bound
//!     graph's cell tools, `graph_id` routing to sibling graphs, and
//!     wait-for-routable across cell activation.
//!
//! All speak the identical MCP wire shape toward the agent.
//!
//! [`ComposedBackend`] wraps any of the above (`--sub <prefix>=<url>`,
//! repeatable) with additional namespaced sub-MCPs — independent HTTP MCP
//! servers whose tools are merged into the catalog under `<prefix>_<name>`.
//! It is a generalization of the `control_` merge `GatewayBackend` already
//! does for exactly one upstream; composition applies above any backend
//! without changing it.

use async_trait::async_trait;
use serde_json::Value;

mod remote;
pub use remote::{AuthHeaders, RemoteHttp};

pub mod gateway;
pub use gateway::{GatewayBackend, GatewayOptions};

pub mod local;
pub use local::LocalGarden;

pub mod composed;
pub use composed::ComposedBackend;

#[cfg(feature = "local-garden-lib")]
pub mod local_lib;

/// `tools/call` named a tool no upstream serves. The stdio server maps this to
/// a JSON-RPC `METHOD_NOT_FOUND` naming the tool (instead of a generic backend
/// error), so an agent can tell "no such tool" from "the tool failed".
#[derive(Debug, thiserror::Error)]
#[error("unknown tool: {0}")]
pub struct ToolNotFound(pub String);

/// A proxy target. Methods mirror the three MCP methods sophia-mcp forwards; tool
/// payloads stay as opaque `Value`s — garden owns the schema, sophia-mcp passes it
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
