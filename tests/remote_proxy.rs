//! End-to-end test of the RemoteHttp backend against a mock garden `/mcp`
//! endpoint. Proves the proxy speaks the real wire contract: JSON-RPC 2.0 POST,
//! `tools/list` passthrough, `tools/call` passthrough, auth header injection,
//! and JSON-RPC error surfacing.

use sophia_mcp::backend::{AuthHeaders, Backend, RemoteHttp};
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn remote_tools_list_passthrough_with_auth() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", "Bearer test-token"))
        .and(header("x-pn-on-behalf-of", "user-sub"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": "ignored",
            "result": {
                "tools": [
                    { "name": "search_documents", "description": "search", "inputSchema": { "type": "object" } }
                ]
            }
        })))
        .mount(&server)
        .await;

    let url = RemoteHttp::resolve_mcp_url(&server.uri(), None);
    let backend = RemoteHttp::new(
        url,
        AuthHeaders {
            bearer: Some("test-token".into()),
            on_behalf_of: Some("user-sub".into()),
            ..Default::default()
        },
    )
    .unwrap();

    let result = backend.list_tools(json!({})).await.unwrap();
    assert_eq!(result["tools"][0]["name"], json!("search_documents"));
    assert_eq!(result["tools"][0]["inputSchema"]["type"], json!("object"));
}

#[tokio::test]
async fn remote_tools_call_passthrough() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({
            "method": "tools/call",
            "params": { "name": "remember", "arguments": { "text": "hi" } }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": "x",
            "result": {
                "content": [ { "type": "text", "text": "{}" } ],
                "structuredContent": { "ok": true }
            }
        })))
        .mount(&server)
        .await;

    let backend = RemoteHttp::new(
        RemoteHttp::resolve_mcp_url(&server.uri(), None),
        AuthHeaders::default(),
    )
    .unwrap();

    let out = backend
        .call_tool(json!({ "name": "remember", "arguments": { "text": "hi" } }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["ok"], json!(true));
}

#[tokio::test]
async fn remote_surfaces_jsonrpc_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": "x",
            "error": { "code": -32000, "message": "tool blew up" }
        })))
        .mount(&server)
        .await;

    let backend = RemoteHttp::new(
        RemoteHttp::resolve_mcp_url(&server.uri(), None),
        AuthHeaders::default(),
    )
    .unwrap();

    let err = backend
        .call_tool(json!({ "name": "x", "arguments": {} }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("-32000"), "got: {msg}");
    assert!(msg.contains("tool blew up"), "got: {msg}");
}
