//! Stdio MCP server (agent-facing).
//!
//! Reads newline-delimited JSON-RPC requests from stdin (the framing Claude
//! Code's stdio MCP transport uses), forwards the three MCP methods to the
//! [`Backend`], and writes JSON-RPC responses to stdout. Notifications (no `id`)
//! get no reply. All logging goes to stderr — stdout is the MCP channel and must
//! stay clean.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::backend::Backend;
use crate::mcp::{self, method, JsonRpcRequest, JsonRpcResponse};

pub async fn serve_stdio(backend: Arc<dyn Backend>) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = reader.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<JsonRpcRequest>(line) {
            Ok(req) => handle_request(backend.as_ref(), req).await,
            Err(e) => Some(JsonRpcResponse::error(
                None,
                mcp::PARSE_ERROR,
                format!("invalid JSON-RPC request: {e}"),
            )),
        };

        if let Some(resp) = response {
            let mut bytes = serde_json::to_vec(&resp)?;
            bytes.push(b'\n');
            stdout.write_all(&bytes).await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

/// Returns `Some(response)` for requests, `None` for notifications.
async fn handle_request(backend: &dyn Backend, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
    let id = req.id.clone();
    let is_notification = id.is_none();

    let result: anyhow::Result<Value> = match req.method.as_str() {
        method::INITIALIZE => backend
            .initialize(req.params.clone())
            .await
            .map(reconcile_initialize),

        method::TOOLS_LIST => backend.list_tools(req.params.clone()).await,

        method::TOOLS_CALL => backend.call_tool(req.params.clone()).await,

        // Liveness ping handled locally.
        method::PING => Ok(json!({})),

        // `notifications/initialized` and other notifications: ack silently.
        _ if is_notification => return None,

        other => {
            return Some(JsonRpcResponse::error(
                id,
                mcp::METHOD_NOT_FOUND,
                format!("method not supported by sophia-mcp proxy: {other}"),
            ));
        }
    };

    if is_notification {
        // A method we proxy but that arrived as a notification — don't reply.
        return None;
    }

    Some(match result {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err(e) => JsonRpcResponse::error(id, mcp::BACKEND_ERROR, format!("{e:#}")),
    })
}

/// Ensure the `initialize` result we hand the agent advertises a protocol
/// version + tools capability even if a backend returned a sparse object. We
/// pass the backend's result through but fill obvious gaps so Claude Code's
/// handshake always succeeds.
fn reconcile_initialize(mut backend_result: Value) -> Value {
    if !backend_result.is_object() {
        backend_result = json!({});
    }
    let obj = backend_result.as_object_mut().expect("object");

    obj.entry("protocolVersion")
        .or_insert_with(|| json!(mcp::PROTOCOL_VERSION));
    obj.entry("capabilities")
        .or_insert_with(|| json!({ "tools": {} }));
    obj.entry("serverInfo")
        .or_insert_with(|| json!({ "name": "sophia-mcp", "version": env!("CARGO_PKG_VERSION") }));
    backend_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct Echo;

    #[async_trait]
    impl Backend for Echo {
        async fn initialize(&self, _p: Value) -> anyhow::Result<Value> {
            // Sparse result: reconcile_initialize must backfill.
            Ok(json!({ "serverInfo": { "name": "sophia-local-native", "version": "9.9" } }))
        }
        async fn list_tools(&self, _p: Value) -> anyhow::Result<Value> {
            Ok(json!({ "tools": [ { "name": "search_documents" } ] }))
        }
        async fn call_tool(&self, p: Value) -> anyhow::Result<Value> {
            Ok(json!({ "content": [], "structuredContent": p }))
        }
    }

    fn req(id: Option<Value>, method: &str, params: Value) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id,
            method: method.into(),
            params,
        }
    }

    #[tokio::test]
    async fn initialize_backfills_protocol_and_capabilities() {
        let resp = handle_request(&Echo, req(Some(json!(1)), method::INITIALIZE, json!({})))
            .await
            .unwrap();
        let result = resp.result.unwrap();
        assert_eq!(result["protocolVersion"], json!(mcp::PROTOCOL_VERSION));
        assert!(result["capabilities"]["tools"].is_object());
        // Backend-provided serverInfo is preserved, not overwritten.
        assert_eq!(result["serverInfo"]["name"], json!("sophia-local-native"));
    }

    #[tokio::test]
    async fn tools_list_passes_through_verbatim() {
        let resp = handle_request(&Echo, req(Some(json!(2)), method::TOOLS_LIST, json!({})))
            .await
            .unwrap();
        assert_eq!(
            resp.result.unwrap()["tools"][0]["name"],
            json!("search_documents")
        );
    }

    #[tokio::test]
    async fn tools_call_forwards_params() {
        let params = json!({ "name": "remember", "arguments": { "x": 1 } });
        let resp = handle_request(
            &Echo,
            req(Some(json!(3)), method::TOOLS_CALL, params.clone()),
        )
        .await
        .unwrap();
        assert_eq!(resp.result.unwrap()["structuredContent"], params);
    }

    #[tokio::test]
    async fn notifications_get_no_reply() {
        let resp = handle_request(&Echo, req(None, method::INITIALIZED, json!({}))).await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn unknown_method_errors() {
        let resp = handle_request(&Echo, req(Some(json!(4)), "frobnicate", json!({})))
            .await
            .unwrap();
        assert_eq!(resp.error.unwrap().code, mcp::METHOD_NOT_FOUND);
    }
}
