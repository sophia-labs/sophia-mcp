//! End-to-end test of the RemoteHttp backend against a mock garden `/mcp`
//! endpoint. Proves the proxy speaks the real wire contract: JSON-RPC 2.0 POST,
//! `tools/list` passthrough, `tools/call` passthrough, auth header injection,
//! and JSON-RPC error surfacing.

use serde_json::json;
use sophia_mcp::backend::{AuthHeaders, Backend, RemoteHttp};
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

#[tokio::test]
async fn gateway_binding_discovers_then_waits_for_activation_before_tools() {
    let server = MockServer::start().await;
    let auth = AuthHeaders {
        bearer: Some("service-token".into()),
        on_behalf_of: Some("owner-sub".into()),
        ..Default::default()
    };
    Mock::given(method("POST"))
        .and(path("/control/mcp"))
        .and(header("authorization", "Bearer service-token"))
        .and(header("x-pn-on-behalf-of", "owner-sub"))
        .and(body_partial_json(json!({
            "method": "tools/call",
            "params": {"name": "list_graphs"}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": "control", "result": {
                "content": [],
                "structuredContent": [{"owner": "user:owner-sub", "graphId": "notes"}]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/o/user%3Aowner-sub/g/notes/activate"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "pollUrl": "/activations/activate-1"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/activate-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "activationId": "activate-1", "phase": "ready"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/o/user%3Aowner-sub/g/notes/mcp"))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": "tools", "result": {"tools": [{"name": "search_documents"}]}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let remote = RemoteHttp::connect_gateway(&server.uri(), "user:owner-sub", "notes", auth)
        .await
        .unwrap();
    assert_eq!(
        remote.mcp_url(),
        format!("{}/o/user%3Aowner-sub/g/notes/mcp", server.uri())
    );
    let tools = remote.list_tools(json!({})).await.unwrap();
    assert_eq!(tools["tools"][0]["name"], "search_documents");
}

#[tokio::test]
async fn undiscoverable_tuple_never_activates_or_calls_the_graph_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/control/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": "control", "result": {
                "content": [], "structuredContent": []
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let result = RemoteHttp::connect_gateway(
        &server.uri(),
        "user:owner-sub",
        "missing",
        AuthHeaders::default(),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("an undiscoverable graph tuple must fail closed"),
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("not in this identity's list_graphs"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/control/mcp");
}
