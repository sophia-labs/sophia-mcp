//! Minimal MCP JSON-RPC 2.0 wire types.
//!
//! These mirror the wire contract that gardend's loopback `/mcp` route speaks
//! (see garden `src-tauri/src/mcp_rpc_protocol.rs` and `loopback_mcp_routes.rs`)
//! and that the platform-next gateway forwards verbatim at `/g/{id}/mcp`. We
//! keep them deliberately thin: sophia-mcp is a *proxy*, so most payloads (tool
//! catalogs, tool-call results) pass through as `serde_json::Value` and are
//! never reshaped. Garden owns the tool registry; sophia-mcp owns identity + routing.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON-RPC 2.0 request, as received from the agent on stdin and as sent to
/// the backend over HTTP. `id` is absent for notifications.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcRequest {
    #[serde(default = "jsonrpc_version")]
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

/// A JSON-RPC 2.0 response. Exactly one of `result` / `error` is set.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

fn jsonrpc_version() -> String {
    "2.0".to_string()
}

impl JsonRpcResponse {
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: jsonrpc_version(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Option<Value>, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: jsonrpc_version(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

// Standard JSON-RPC / MCP error codes used by sophia-mcp. The full set is kept for
// completeness/reference even where sophia-mcp doesn't currently emit each one.
pub const PARSE_ERROR: i64 = -32700;
#[allow(dead_code)]
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
#[allow(dead_code)]
pub const INTERNAL_ERROR: i64 = -32603;
/// sophia-mcp-originated upstream/transport failure (backend unreachable, etc).
pub const BACKEND_ERROR: i64 = -32010;

/// MCP method names sophia-mcp understands at the stdio edge.
pub mod method {
    pub const INITIALIZE: &str = "initialize";
    /// `notifications/initialized` — handled generically as a notification (no
    /// reply); named here for reference and used in tests.
    #[allow(dead_code)]
    pub const INITIALIZED: &str = "notifications/initialized";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const TOOLS_CALL: &str = "tools/call";
    pub const PING: &str = "ping";
}

/// The MCP protocol version sophia-mcp advertises to the agent. Matches gardend
/// (`loopback_mcp_routes.rs` -> "2025-03-26").
pub const PROTOCOL_VERSION: &str = "2025-03-26";
