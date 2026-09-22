//! Stdio MCP server (agent-facing).
//!
//! Reads newline-delimited JSON-RPC requests from stdin (the framing Claude
//! Code's stdio MCP transport uses), forwards the three MCP methods to the
//! [`Backend`], and writes JSON-RPC responses to stdout. Notifications (no `id`)
//! get no reply. All logging goes to stderr — stdout is the MCP channel and must
//! stay clean.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::backend::{Backend, ToolNotFound};
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

/// Dispatch one JSON-RPC request to the backend and shape the reply for the
/// agent. This is the single client-facing choke point every backend's result
/// passes through: `initialize` is reconciled, `tools/call` results are
/// normalized ([`normalize_tool_result`]), everything else is verbatim.
///
/// Returns `Some(response)` for requests, `None` for notifications.
pub async fn handle_request(backend: &dyn Backend, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
    let id = req.id.clone();
    let is_notification = id.is_none();

    let result: anyhow::Result<Value> = match req.method.as_str() {
        method::INITIALIZE => backend
            .initialize(req.params.clone())
            .await
            .map(reconcile_initialize),

        method::TOOLS_LIST => backend.list_tools(req.params.clone()).await,

        method::TOOLS_CALL => {
            let tool = req.params["name"]
                .as_str()
                .unwrap_or("<unnamed>")
                .to_owned();
            backend
                .call_tool(req.params.clone())
                .await
                .map(|result| normalize_tool_result(&tool, result))
        }

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
        Err(e) => {
            // Full, unredacted chain to stderr for operators — the MCP
            // CLIENT only ever sees the bounded rendering below.
            tracing::error!(error = %format!("{e:#}"), "backend call failed");
            JsonRpcResponse::error(id, error_code(&e), redact_error_message(&e))
        }
    })
}

/// Cap on the rendered error chain handed to the MCP CLIENT. Independent of,
/// and smaller than the sum of, any per-site upstream-body redaction further
/// down the stack (`backend::remote::redact_body`) — this is the backstop:
/// even a chain built from several already-bounded pieces (nested `anyhow`
/// `.context()` layers) can't grow past a sane size here, and anything that
/// somehow reached this point without going through a redaction site still
/// gets capped rather than shipped whole to an untrusted-by-default client.
const CLIENT_ERROR_MAX_BYTES: usize = 2048;

/// Render an error for the MCP CLIENT: the full `anyhow` chain (`{:#}`),
/// capped at [`CLIENT_ERROR_MAX_BYTES`]. Full, unredacted detail always goes
/// to stderr first (see the `tracing::error!` call above this function's one
/// call site) — this function's return value is the ONLY thing the client
/// ever sees for a failed call.
fn redact_error_message(e: &anyhow::Error) -> String {
    let full = format!("{e:#}");
    if full.len() <= CLIENT_ERROR_MAX_BYTES {
        return full;
    }
    let mut end = CLIENT_ERROR_MAX_BYTES;
    while end > 0 && !full.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}… [truncated to {CLIENT_ERROR_MAX_BYTES} bytes; full detail in server logs]",
        &full[..end]
    )
}

/// Unknown tool → `METHOD_NOT_FOUND` (the message names the tool); anything
/// else the backend raised (transport, upstream HTTP, activation timeout) →
/// `BACKEND_ERROR` with the full error chain as the message.
fn error_code(e: &anyhow::Error) -> i64 {
    if e.downcast_ref::<ToolNotFound>().is_some() {
        mcp::METHOD_NOT_FOUND
    } else {
        mcp::BACKEND_ERROR
    }
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

/// Client-facing normalization of a `tools/call` result envelope.
///
/// MCP specifies `structuredContent` as a JSON **object**; strict clients
/// (Claude Code's included) reject anything else — "structuredContent expected
/// record, received array". Some upstreams break this: the platform-next
/// gateway's control plane answers `list_graphs` with a bare array. Rather than
/// let one upstream's shape fail the whole call at the agent, sophia-mcp
/// reshapes the envelope at the stdio edge, for every backend alike:
///
/// * array → `{ "items": [...] }`
/// * any other non-object (string, number, bool, null) → `{ "value": ... }`
/// * object → untouched; absent → untouched
///
/// `content` (and every other envelope field) is never modified. Logged at
/// `debug` on stderr once per tool name so the upstream defect stays visible
/// without flooding the log.
pub fn normalize_tool_result(tool: &str, mut result: Value) -> Value {
    let Some(envelope) = result.as_object_mut() else {
        return result;
    };
    let Some(structured) = envelope.get_mut("structuredContent") else {
        return result;
    };
    let wrapped_as = match structured {
        Value::Object(_) => return result,
        Value::Array(_) => "items",
        _ => "value",
    };
    let inner = structured.take();
    *structured = json!({ wrapped_as: inner });
    log_normalization_once(tool, wrapped_as);
    result
}

fn log_normalization_once(tool: &str, wrapped_as: &str) {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    let first = seen
        .lock()
        .map(|mut set| set.insert(tool.to_owned()))
        .unwrap_or(true);
    if first {
        tracing::debug!(
            tool,
            wrapped_as,
            "upstream returned a non-object structuredContent; wrapped for the MCP client"
        );
    }
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
            if p["name"] == json!("no_such_tool") {
                return Err(ToolNotFound("no_such_tool".into()).into());
            }
            if p["name"] == json!("explodes") {
                return Err(anyhow::anyhow!("upstream HTTP 502"));
            }
            if p["name"] == json!("explodes_huge") {
                // Simulates an upstream body that reached this point
                // un-redacted (e.g. a future call site that forgets
                // `backend::remote::redact_body`) — proves the server.rs
                // choke point is a real backstop, not just decoration on
                // top of the per-site redaction already covered elsewhere.
                let secret = "SECRET_TOKEN_MUST_NOT_LEAK";
                return Err(anyhow::anyhow!(
                    "backend returned HTTP 500: {}{secret}",
                    "x".repeat(5000)
                ));
            }
            if p["name"] == json!("lists_an_array") {
                return Ok(json!({ "content": [{ "type": "text", "text": "two rows" }],
                    "structuredContent": [{ "graphId": "a" }, { "graphId": "b" }] }));
            }
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
    async fn unknown_tool_is_method_not_found_naming_the_tool() {
        let resp = handle_request(
            &Echo,
            req(
                Some(json!(5)),
                method::TOOLS_CALL,
                json!({ "name": "no_such_tool", "arguments": {} }),
            ),
        )
        .await
        .unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, mcp::METHOD_NOT_FOUND);
        assert!(err.message.contains("no_such_tool"), "got: {}", err.message);
    }

    #[tokio::test]
    async fn other_backend_failures_stay_backend_errors() {
        let resp = handle_request(
            &Echo,
            req(
                Some(json!(6)),
                method::TOOLS_CALL,
                json!({ "name": "explodes", "arguments": {} }),
            ),
        )
        .await
        .unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, mcp::BACKEND_ERROR);
        assert!(err.message.contains("502"), "got: {}", err.message);
    }

    #[tokio::test]
    async fn unknown_method_errors() {
        let resp = handle_request(&Echo, req(Some(json!(4)), "frobnicate", json!({})))
            .await
            .unwrap();
        assert_eq!(resp.error.unwrap().code, mcp::METHOD_NOT_FOUND);
    }

    // --------------------------------------- client-facing error redaction

    #[tokio::test]
    async fn a_huge_secret_bearing_backend_error_reaches_the_client_bounded_and_without_the_secret()
    {
        let resp = handle_request(
            &Echo,
            req(
                Some(json!(8)),
                method::TOOLS_CALL,
                json!({ "name": "explodes_huge", "arguments": {} }),
            ),
        )
        .await
        .unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.code, mcp::BACKEND_ERROR);
        assert!(
            err.message.len() <= CLIENT_ERROR_MAX_BYTES + 100,
            "client message must be bounded, got {} bytes",
            err.message.len()
        );
        assert!(
            !err.message.contains("SECRET_TOKEN_MUST_NOT_LEAK"),
            "the secret past the cap must never reach the client: {}",
            err.message
        );
        assert!(
            err.message.contains("truncated"),
            "must carry an explicit truncation marker: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn a_short_legitimate_error_reaches_the_client_unmodified() {
        // Regression guard: the redaction choke point must not mangle or
        // truncate an ordinary, already-short error — normal UX (a gateway's
        // legible rejection message, a plain tool failure) is unaffected.
        let resp = handle_request(
            &Echo,
            req(
                Some(json!(9)),
                method::TOOLS_CALL,
                json!({ "name": "explodes", "arguments": {} }),
            ),
        )
        .await
        .unwrap();
        let err = resp.error.unwrap();
        assert_eq!(err.message, "upstream HTTP 502");
    }

    #[test]
    fn redact_error_message_passes_a_short_chain_through_unchanged() {
        let e = anyhow::anyhow!("graph 'notes' is not routable after 1.2s");
        assert_eq!(redact_error_message(&e), format!("{e:#}"));
    }

    #[test]
    fn redact_error_message_truncates_a_huge_chain_with_a_bounded_marker() {
        let e = anyhow::anyhow!("x".repeat(CLIENT_ERROR_MAX_BYTES * 3));
        let redacted = redact_error_message(&e);
        assert!(redacted.len() <= CLIENT_ERROR_MAX_BYTES + 100);
        assert!(redacted.contains("truncated"));
    }

    // ------------------------------------------------ structuredContent shape

    #[test]
    fn normalizer_wraps_an_array_as_items_and_leaves_content_alone() {
        let out = normalize_tool_result(
            "control_list_graphs",
            json!({ "content": [{ "type": "text", "text": "t" }], "structuredContent": [1, 2] }),
        );
        assert_eq!(out["structuredContent"], json!({ "items": [1, 2] }));
        assert_eq!(out["content"], json!([{ "type": "text", "text": "t" }]));
    }

    #[test]
    fn normalizer_wraps_scalars_as_value() {
        for scalar in [json!("s"), json!(3), json!(true), Value::Null] {
            let out =
                normalize_tool_result("t", json!({ "content": [], "structuredContent": scalar }));
            assert_eq!(
                out["structuredContent"],
                json!({ "value": scalar }),
                "scalar {scalar}"
            );
        }
    }

    #[test]
    fn normalizer_leaves_objects_untouched() {
        let envelope = json!({ "content": [], "structuredContent": { "items": [1], "x": 2 }, "isError": false });
        assert_eq!(normalize_tool_result("t", envelope.clone()), envelope);
    }

    #[test]
    fn normalizer_leaves_absent_structured_content_and_non_object_envelopes_untouched() {
        let without = json!({ "content": [{ "type": "text", "text": "plain" }] });
        assert_eq!(normalize_tool_result("t", without.clone()), without);
        assert_eq!(normalize_tool_result("t", json!([1, 2])), json!([1, 2]));
    }

    #[tokio::test]
    async fn tools_call_array_structured_content_reaches_the_client_as_items() {
        let resp = handle_request(
            &Echo,
            req(
                Some(json!(7)),
                method::TOOLS_CALL,
                json!({ "name": "lists_an_array", "arguments": {} }),
            ),
        )
        .await
        .unwrap();
        let result = resp.result.unwrap();
        assert_eq!(
            result["structuredContent"],
            json!({ "items": [{ "graphId": "a" }, { "graphId": "b" }] })
        );
        assert_eq!(result["content"][0]["text"], json!("two rows"));
    }
}
