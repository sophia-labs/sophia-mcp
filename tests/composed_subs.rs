//! End-to-end tests of `ComposedBackend` against wiremock "primary" and "sub"
//! MCP servers: the merged catalog, `<prefix>_<name>` routing (through the
//! real stdio dispatch, `server::handle_request`, so `normalize_tool_result`
//! is exercised too), a down sub at start, per-sub bearer tokens, and invalid
//! prefixes refused at config parse. Style follows `tests/gateway_multigraph.rs`.

use std::sync::Arc;

use serde_json::{json, Value};
use sophia_mcp::backend::{AuthHeaders, Backend, ComposedBackend, RemoteHttp};
use sophia_mcp::config::{parse_sub_specs, SubSpec};
use sophia_mcp::mcp::{self, method as rpc, JsonRpcRequest};
use sophia_mcp::server::handle_request;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn rpc_ok(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "jsonrpc": "2.0", "id": "x", "result": result }))
}

fn primary_client(server: &MockServer) -> Arc<dyn Backend> {
    Arc::new(
        RemoteHttp::new(RemoteHttp::resolve_mcp_url(&server.uri(), None), AuthHeaders::default())
            .unwrap(),
    )
}

fn primary_client_with_token(server: &MockServer, token: &str) -> Arc<dyn Backend> {
    Arc::new(
        RemoteHttp::new(
            RemoteHttp::resolve_mcp_url(&server.uri(), None),
            AuthHeaders {
                bearer: Some(token.to_string()),
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn sub_spec(prefix: &str, server: &MockServer, token: Option<&str>) -> SubSpec {
    SubSpec {
        prefix: prefix.to_string(),
        url: server.uri(),
        token: token.map(str::to_string),
    }
}

/// Drive a `tools/call` through the stdio dispatch (`server::handle_request`)
/// — the exact function whose output is serialized to the MCP client — and
/// return the full JSON-RPC response (so a caller can inspect `error` too).
async fn client_call(
    backend: &dyn Backend,
    name: &str,
    arguments: Value,
) -> sophia_mcp::mcp::JsonRpcResponse {
    let req = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: Some(json!(1)),
        method: rpc::TOOLS_CALL.into(),
        params: json!({ "name": name, "arguments": arguments }),
    };
    handle_request(backend, req).await.expect("a request gets a reply")
}

// --------------------------------------------------------------- (1) merged catalog

#[tokio::test]
async fn merged_tools_list_carries_primary_tools_plus_prefixed_sub_tools_verbatim() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [
            { "name": "read_document", "description": "reads a document",
              "inputSchema": { "type": "object", "properties": { "id": { "type": "string" } } } }
        ]})))
        .mount(&primary)
        .await;

    let sub = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [
            { "name": "world", "description": "the page in words, right now",
              "inputSchema": { "type": "object", "properties": { "graphId": { "type": "string" } } } },
            { "name": "moves", "description": "the affordances from the current world",
              "inputSchema": { "type": "object" } }
        ]})))
        .mount(&sub)
        .await;

    let composed = ComposedBackend::compose(
        primary_client(&primary),
        vec![sub_spec("layout", &sub, None)],
    )
    .await
    .unwrap();

    let result = composed.list_tools(json!({})).await.unwrap();
    let tools = result["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["read_document", "layout_world", "layout_moves"]);

    let world = tools.iter().find(|t| t["name"] == json!("layout_world")).unwrap();
    assert_eq!(world["description"], json!("the page in words, right now"));
    assert_eq!(world["inputSchema"]["properties"]["graphId"]["type"], json!("string"));

    let read = tools.iter().find(|t| t["name"] == json!("read_document")).unwrap();
    assert_eq!(read["description"], json!("reads a document"));
}

// --------------------------------------------------------- (2) tools/call routes to sub

#[tokio::test]
async fn tools_call_layout_moves_reaches_the_sub_as_moves_untouched_and_is_normalized() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [] })))
        .mount(&primary)
        .await;

    let sub = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [ { "name": "moves" } ] })))
        .mount(&sub)
        .await;
    // The sub sees the BARE name "moves" and the arguments exactly as sent —
    // not "layout_moves", not rewrapped.
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({
            "method": "tools/call",
            "params": { "name": "moves", "arguments": { "graphId": "jev-hello", "layer": "middle" } }
        })))
        .respond_with(rpc_ok(json!({
            "content": [ { "type": "text", "text": "two moves" } ],
            // An array here proves normalize_tool_result runs on the WAY OUT
            // of the composed backend, at the server's stdio edge — same as
            // any other backend's result.
            "structuredContent": [ { "id": "m1" }, { "id": "m2" } ]
        })))
        .expect(1)
        .mount(&sub)
        .await;

    let composed = ComposedBackend::compose(
        primary_client(&primary),
        vec![sub_spec("layout", &sub, None)],
    )
    .await
    .unwrap();

    let resp = client_call(
        &composed,
        "layout_moves",
        json!({ "graphId": "jev-hello", "layer": "middle" }),
    )
    .await;
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let result = resp.result.unwrap();
    assert_eq!(
        result["structuredContent"],
        json!({ "items": [ { "id": "m1" }, { "id": "m2" } ] }),
        "the array must have been wrapped by normalize_tool_result, same as any backend"
    );
    assert_eq!(result["content"][0]["text"], json!("two moves"));
    sub.verify().await;
}

// ---------------------------------------------------- (3) tools/call still reaches primary

#[tokio::test]
async fn tools_call_read_document_still_reaches_the_primary_not_the_sub() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [ { "name": "read_document" } ] })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({
            "method": "tools/call",
            "params": { "name": "read_document", "arguments": { "id": "doc-1" } }
        })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "id": "doc-1", "title": "t" } })))
        .expect(1)
        .mount(&primary)
        .await;

    let sub = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [ { "name": "world" } ] })))
        .mount(&sub)
        .await;

    let composed = ComposedBackend::compose(
        primary_client(&primary),
        vec![sub_spec("layout", &sub, None)],
    )
    .await
    .unwrap();

    let out = composed
        .call_tool(json!({ "name": "read_document", "arguments": { "id": "doc-1" } }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["title"], json!("t"));

    let sub_requests = sub.received_requests().await.unwrap();
    assert!(
        sub_requests.iter().all(|r| !String::from_utf8_lossy(&r.body).contains("tools/call")),
        "the sub must never see a call meant for the primary: {sub_requests:?}"
    );
}

// ------------------------------------------------- (4) a down sub at start is skipped

#[tokio::test]
async fn a_sub_whose_tools_list_500s_at_start_is_skipped_and_layout_x_is_method_not_found() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [ { "name": "read_document" } ] })))
        .mount(&primary)
        .await;

    let sub = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .expect(1)
        .mount(&sub)
        .await;

    let composed = ComposedBackend::compose(
        primary_client(&primary),
        vec![sub_spec("layout", &sub, None)],
    )
    .await
    .expect("a down sub must not fail composition");

    // The primary's catalog still serves, with no layout_* entries.
    let result = composed.list_tools(json!({})).await.unwrap();
    let names: Vec<&str> = result["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["read_document"]);

    // A call to the down sub's namespace is METHOD_NOT_FOUND, naming the tool.
    let resp = client_call(&composed, "layout_x", json!({})).await;
    let err = resp.error.expect("a down sub's tool must error");
    assert_eq!(err.code, mcp::METHOD_NOT_FOUND);
    assert!(err.message.contains("layout_x"), "got: {}", err.message);

    // The refusal never touched the network: exactly the one probe request.
    assert_eq!(sub.received_requests().await.unwrap().len(), 1);
}

// -------------------------------------------------- (5) per-sub bearer, never shared

#[tokio::test]
async fn a_sub_bearer_token_is_sent_to_the_sub_and_never_to_the_primary() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", "Bearer primary-token"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [] })))
        .mount(&primary)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", "Bearer primary-token"))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "who": "primary" } })))
        .mount(&primary)
        .await;

    let sub = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", "Bearer sub-token"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [ { "name": "world" } ] })))
        .mount(&sub)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("authorization", "Bearer sub-token"))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "world" } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "who": "sub" } })))
        .expect(1)
        .mount(&sub)
        .await;

    let composed = ComposedBackend::compose(
        primary_client_with_token(&primary, "primary-token"),
        vec![sub_spec("layout", &sub, Some("sub-token"))],
    )
    .await
    .unwrap();

    let out = composed
        .call_tool(json!({ "name": "layout_world", "arguments": {} }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["who"], json!("sub"));

    // A non-prefixed name still reaches the primary, with the primary's own
    // bearer — proving the two tokens are not just both accepted, but each
    // sent only where it belongs (wiremock's `header(...)` matcher above
    // requires the exact value; a mismatch would 404 rather than match here).
    let out = composed
        .call_tool(json!({ "name": "read_document", "arguments": {} }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["who"], json!("primary"));

    // Cross-check directly: every request that landed on `sub` carried
    // "Bearer sub-token"; every request that landed on `primary` carried
    // "Bearer primary-token". Neither leaked to the other.
    for req in sub.received_requests().await.unwrap() {
        assert_eq!(
            req.headers.get("authorization").unwrap(),
            "Bearer sub-token",
            "sub saw: {:?}",
            req.headers
        );
    }
    for req in primary.received_requests().await.unwrap() {
        assert_eq!(
            req.headers.get("authorization").unwrap(),
            "Bearer primary-token",
            "primary saw: {:?}",
            req.headers
        );
    }
}

// --------------------------------------------------- (6) invalid prefix at config parse

#[test]
fn an_invalid_prefix_is_refused_at_config_parse() {
    for bad in ["Layout=http://127.0.0.1:5199/mcp", "1x=http://127.0.0.1:5199/mcp"] {
        let err = parse_sub_specs(&[bad.to_string()], &[]).unwrap_err();
        assert!(format!("{err}").contains("invalid"), "{bad}: {err}");
    }
    // A valid prefix is accepted, for contrast.
    assert!(parse_sub_specs(&["layout=http://127.0.0.1:5199/mcp".to_string()], &[]).is_ok());
}
