//! End-to-end tests of the multi-graph `GatewayBackend` against a wiremock
//! platform-next gateway: control-plane + cell union, `graph_id` routing by
//! path, wait-for-routable across activation, and truthful surfacing of every
//! failing direction (unlisted / tombstoned / ambiguous graph, gateway 403 and
//! 404, disagreeing graph arguments, off-origin pollUrl, unknown tool, budget
//! expiry on a slow or hung upstream, queued waiters, health-200-is-not-
//! readiness).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use sophia_mcp::backend::{AuthHeaders, Backend, GatewayBackend, GatewayOptions, ToolNotFound};
use sophia_mcp::mcp::{method as rpc, JsonRpcRequest};
use sophia_mcp::server::handle_request;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const OWNER: &str = "user:owner-sub";
const OWNER_PATH: &str = "user%3Aowner-sub";
const FRIEND: &str = "user:friend";
const FRIEND_PATH: &str = "user%3Afriend";

fn auth() -> AuthHeaders {
    AuthHeaders {
        bearer: Some("service-token".into()),
        on_behalf_of: Some("owner-sub".into()),
        ..Default::default()
    }
}

/// Sub-second budgets so the failing directions finish fast.
fn fast() -> GatewayOptions {
    GatewayOptions {
        activation_timeout: Duration::from_millis(400),
        activation_poll: Duration::from_millis(20),
        request_timeout: Duration::from_secs(5),
        unified_mcp_fallback: false,
    }
}

fn rpc_ok(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "jsonrpc": "2.0", "id": "x", "result": result }))
}

fn cell_path_of(owner_path: &str, graph: &str) -> String {
    format!("/o/{owner_path}/g/{graph}/mcp")
}

fn activate_path_of(owner_path: &str, graph: &str) -> String {
    format!("/o/{owner_path}/g/{graph}/activate")
}

fn cell_path(graph: &str) -> String {
    cell_path_of(OWNER_PATH, graph)
}

fn activate_path(graph: &str) -> String {
    activate_path_of(OWNER_PATH, graph)
}

fn row(owner: &str, graph: &str, state: &str) -> Value {
    json!({ "owner": owner, "graphId": graph, "lifecycleState": state })
}

fn listing_response(rows: Vec<Value>) -> ResponseTemplate {
    rpc_ok(json!({ "content": [], "structuredContent": rows }))
}

/// Mount the control plane at `at` with an explicit `list_graphs` responder
/// plus a fixed tools/list.
async fn mount_control_with(server: &MockServer, at: &str, list_graphs: impl Respond + 'static) {
    Mock::given(method("POST"))
        .and(path(at))
        .and(header("authorization", "Bearer service-token"))
        .and(header("x-pn-on-behalf-of", "owner-sub"))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "list_graphs" } })))
        .respond_with(list_graphs)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(at))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [
            { "name": "list_graphs", "description": "control", "inputSchema": { "type": "object", "properties": {} } },
            { "name": "create_graph", "description": "control", "inputSchema": { "type": "object", "properties": { "graphId": { "type": "string" } } } }
        ]})))
        .mount(server)
        .await;
}

/// Mount the control plane with `graphs` all owned by OWNER and active.
async fn mount_control(server: &MockServer, at: &str, graphs: &[&str]) {
    let rows = graphs.iter().map(|g| row(OWNER, g, "active")).collect();
    mount_control_with(server, at, listing_response(rows)).await;
}

/// A warm cell under `owner_path`: activate → 200 ready:true; initialize /
/// tools/list / tools/call → 200.
async fn mount_warm_cell_of(server: &MockServer, owner_path: &str, graph: &str) {
    Mock::given(method("POST"))
        .and(path(activate_path_of(owner_path, graph)))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path_of(owner_path, graph)))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "protocolVersion": "2025-03-26", "capabilities": { "tools": {} },
            "serverInfo": { "name": "gardend", "version": "0.0" } })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path_of(owner_path, graph)))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [
            { "name": "search_documents", "inputSchema": { "type": "object", "properties": { "query": { "type": "string" } } } },
            { "name": "list_graphs", "inputSchema": { "type": "object" } }
        ]})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path_of(owner_path, graph)))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": graph, "owner_path": owner_path } })))
        .mount(server)
        .await;
}

async fn mount_warm_cell(server: &MockServer, graph: &str) {
    mount_warm_cell_of(server, OWNER_PATH, graph).await;
}

/// Answers `before` for the first `n` requests, then `after` forever.
struct FlipAfter {
    n: usize,
    seen: AtomicUsize,
    before: ResponseTemplate,
    after: ResponseTemplate,
}

impl FlipAfter {
    fn new(n: usize, before: ResponseTemplate, after: ResponseTemplate) -> Self {
        Self {
            n,
            seen: AtomicUsize::new(0),
            before,
            after,
        }
    }
}

impl Respond for FlipAfter {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if self.seen.fetch_add(1, Ordering::SeqCst) < self.n {
            self.before.clone()
        } else {
            self.after.clone()
        }
    }
}

async fn requests_to(server: &MockServer, needle: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path().contains(needle))
        .count()
}

async fn bodies_to(server: &MockServer, needle: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path().contains(needle))
        .map(|r| serde_json::from_slice(&r.body).unwrap_or(Value::Null))
        .collect()
}

async fn connect(server: &MockServer, graph: &str) -> GatewayBackend {
    GatewayBackend::connect(&server.uri(), OWNER, graph, auth(), fast())
        .await
        .unwrap()
}

/// Drive a `tools/call` through the stdio dispatch (`server::handle_request`)
/// — the exact function whose output is serialized to the MCP client — and
/// return the JSON-RPC `result`.
async fn client_calls(gw: &GatewayBackend, name: &str, arguments: Value) -> Value {
    let req = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: Some(json!(1)),
        method: rpc::TOOLS_CALL.into(),
        params: json!({ "name": name, "arguments": arguments }),
    };
    let resp = handle_request(gw, req).await.expect("a request gets a reply");
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    resp.result.unwrap()
}

// ------------------------------------------------------------------ connect

#[tokio::test]
async fn connect_discovers_kicks_activation_and_proves_routability_before_tools() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false,
            "activation": { "activationId": "cell-1", "phase": "scheduling",
                "events": [{ "sequence": 0, "phase": "scheduling", "detail": "cell placement requested" }] },
            "pollUrl": "/activations/cell-1", "eventsUrl": "/activations/cell-1/events"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "activationId": "cell-1", "phase": "ready",
            "events": [{ "sequence": 3, "phase": "ready", "detail": "graph tools are ready" }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [{ "name": "search_documents" }] })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    assert_eq!(gw.mcp_url_for("notes"), format!("{}{}", server.uri(), cell_path("notes")));
    assert_eq!(gw.control_url(), format!("{}/control/mcp", server.uri()));
    // Nothing on the cell path yet: connect only kicked the activation.
    assert_eq!(requests_to(&server, "/g/notes/mcp").await, 0);

    gw.warm().await.unwrap();
    // One activation poll (ready) + one MCP probe.
    assert_eq!(gw.last_wait_polls(), 2);

    let tools = gw.list_tools(json!({})).await.unwrap();
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["search_documents", "control_list_graphs", "control_create_graph"]);
    server.verify().await;
}

#[tokio::test]
async fn undiscoverable_tuple_never_activates_or_calls_the_graph_endpoint() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &[]).await;

    let result = GatewayBackend::connect(&server.uri(), OWNER, "missing", auth(), fast()).await;
    let error = match result {
        Ok(_) => panic!("an undiscoverable graph tuple must fail closed"),
        Err(error) => error,
    };
    let msg = format!("{error:#}");
    assert!(msg.contains("not in this identity's list_graphs"), "got: {msg}");
    assert!(msg.contains("will not activate or create"), "got: {msg}");
    let requests = server.received_requests().await.unwrap();
    assert!(requests.iter().all(|r| r.url.path() == "/control/mcp"), "{requests:?}");
}

#[tokio::test]
async fn transient_503_at_the_activation_kick_does_not_kill_connect() {
    // H2: activate → 503 at_capacity once, then 200. The MCP server must
    // still start; the first use waits and succeeds.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    let capacity = ResponseTemplate::new(503).set_body_json(json!({
        "error": "graph 'user:owner-sub/notes' is at capacity, try again shortly",
        "code": "at_capacity", "retryAfter": 10
    }));
    let ready = ResponseTemplate::new(200).set_body_json(json!({ "ready": true }));
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(FlipAfter::new(1, capacity, ready))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [{ "name": "search_documents" }] })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": "notes" } })))
        .mount(&server)
        .await;

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .expect("a retryable 503 at kick must not be fatal");
    let out = gw
        .call_tool(json!({ "name": "search_documents", "arguments": {} }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["cell"], json!("notes"));
    assert_eq!(requests_to(&server, "/g/notes/activate").await, 2);
}

#[tokio::test]
async fn initialize_is_answered_locally_as_sophia_mcp_without_touching_the_cell() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "activation": { "phase": "hydrating", "events": [] },
            "pollUrl": "/activations/cell-1"
        })))
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let init = gw
        .initialize(json!({ "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": { "name": "claude-code" } }))
        .await
        .unwrap();
    assert_eq!(init["serverInfo"]["name"], json!("sophia-mcp"));
    assert_eq!(init["serverInfo"]["version"], json!(env!("CARGO_PKG_VERSION")));
    assert_eq!(init["protocolVersion"], json!("2025-03-26"));
    assert_eq!(requests_to(&server, "/g/notes/mcp").await, 0);
    assert_eq!(requests_to(&server, "/activations/").await, 0);
}

#[tokio::test]
async fn unified_mcp_fallback_is_used_only_when_control_404s_and_the_flag_is_set() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/control/mcp"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": "not found: route" })))
        .mount(&server)
        .await;
    mount_control(&server, "/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let err = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .err()
        .expect("without the flag, a 404 control plane is fatal");
    assert!(format!("{err:#}").contains("404"), "got: {err:#}");

    let opts = GatewayOptions {
        unified_mcp_fallback: true,
        ..fast()
    };
    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), opts)
        .await
        .unwrap();
    assert_eq!(gw.control_url(), format!("{}/mcp", server.uri()));
    let out = gw
        .call_tool(json!({ "name": "control_create_graph", "arguments": { "graphId": "n2" } }))
        .await;
    // /mcp (mount_control) serves list_graphs + tools/list only; the call
    // reaching it (and failing there, not in routing) proves the fallback URL.
    let msg = format!("{:#}", out.unwrap_err());
    assert!(msg.contains(&format!("{}/mcp", server.uri())), "got: {msg}");
}

// ------------------------------------------------------------- multi-graph

#[tokio::test]
async fn tools_list_is_the_union_with_control_tools_always_prefixed_and_routed() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;
    Mock::given(method("POST"))
        .and(path("/control/mcp"))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "create_graph" } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "control": "create_graph" } })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let tools = gw.list_tools(json!({})).await.unwrap();
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["search_documents", "list_graphs", "control_list_graphs", "control_create_graph"]
    );
    // Cell tools advertise the routing argument; control tools do not.
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["graph_id"]["type"], json!("string"));
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["graphId"]["type"], json!("string"));
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["query"]["type"], json!("string"));
    assert!(tools["tools"][3]["inputSchema"]["properties"].get("graph_id").is_none());

    // Unprefixed `list_graphs` is the cell's; the prefixed one is control's,
    // forwarded under its upstream name. Bare `create_graph` is not a tool.
    let cell = gw.call_tool(json!({ "name": "list_graphs", "arguments": {} })).await.unwrap();
    assert_eq!(cell["structuredContent"]["cell"], json!("notes"));
    let control = gw
        .call_tool(json!({ "name": "control_list_graphs", "arguments": {} }))
        .await
        .unwrap();
    assert!(control["structuredContent"].is_array(), "got: {control}");
    let created = gw
        .call_tool(json!({ "name": "control_create_graph", "arguments": { "graphId": "n2" } }))
        .await
        .unwrap();
    assert_eq!(created["structuredContent"]["control"], json!("create_graph"));
    let err = gw
        .call_tool(json!({ "name": "create_graph", "arguments": { "graphId": "n2" } }))
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<ToolNotFound>().is_some(), "got: {err:#}");
    server.verify().await;
}

#[tokio::test]
async fn tools_list_pagination_appends_control_tools_only_on_the_last_page() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/list", "params": { "cursor": "p2" } })))
        .respond_with(rpc_ok(json!({ "tools": [{ "name": "page_two_tool" }] })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [{ "name": "page_one_tool" }], "nextCursor": "p2" })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "page_one_tool" } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "page": 1 } })))
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let first = gw.list_tools(json!({})).await.unwrap();
    let names = |v: &Value| -> Vec<String> {
        v["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect()
    };
    assert_eq!(names(&first), vec!["page_one_tool"]);
    assert_eq!(first["nextCursor"], json!("p2"));
    let second = gw.list_tools(json!({ "cursor": "p2" })).await.unwrap();
    assert_eq!(names(&second), vec!["page_two_tool", "control_list_graphs", "control_create_graph"]);
    // Page-one routes survived the page-two refresh.
    let out = gw.call_tool(json!({ "name": "page_one_tool", "arguments": {} })).await.unwrap();
    assert_eq!(out["structuredContent"]["page"], json!(1));
}

#[tokio::test]
async fn graph_id_argument_routes_to_the_sibling_cell_by_path_with_one_cached_session() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "scratch"]).await;
    mount_warm_cell(&server, "notes").await;
    Mock::given(method("POST"))
        .and(path(activate_path("scratch")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("scratch")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("scratch")))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "search_documents" } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": "scratch" } })))
        .expect(2)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    // No tools/list first: routing is built lazily on the first call.
    let bound = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "query": "q" } }))
        .await
        .unwrap();
    assert_eq!(bound["structuredContent"]["cell"], json!("notes"));
    let a = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "query": "q", "graph_id": "scratch" } }))
        .await
        .unwrap();
    assert_eq!(a["structuredContent"]["cell"], json!("scratch"));
    let b = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "query": "q", "graphId": "scratch" } }))
        .await
        .unwrap();
    assert_eq!(b["structuredContent"]["cell"], json!("scratch"));
    // Naming the bound graph explicitly is not a sibling route.
    let same = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "notes" } }))
        .await
        .unwrap();
    assert_eq!(same["structuredContent"]["cell"], json!("notes"));
    // The cell saw exactly the spelling the agent used, equal to its path.
    let calls: Vec<Value> = bodies_to(&server, "/g/scratch/mcp")
        .await
        .into_iter()
        .filter(|b| b["method"] == json!("tools/call"))
        .map(|b| b["params"]["arguments"].clone())
        .collect();
    assert_eq!(calls[0], json!({ "query": "q", "graph_id": "scratch" }));
    assert_eq!(calls[1], json!({ "query": "q", "graphId": "scratch" }));
    // activate ×1 + initialize ×1 for scratch across two calls = one session.
    server.verify().await;
}

#[tokio::test]
async fn disagreeing_graph_id_and_graph_id_camel_are_refused_before_any_request() {
    // M3: the seam must not reopen — a body carrying a second, different
    // carrier never reaches any cell.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "scratch"]).await;
    mount_warm_cell(&server, "notes").await;
    mount_warm_cell(&server, "scratch").await;

    let gw = connect(&server, "notes").await;
    gw.list_tools(json!({})).await.unwrap();
    let before = requests_to(&server, "/mcp").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents",
            "arguments": { "graph_id": "notes", "graphId": "scratch" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("disagree"), "got: {msg}");
    assert!(msg.contains("notes") && msg.contains("scratch"), "got: {msg}");
    assert_eq!(requests_to(&server, "/mcp").await, before, "no request may be made");

    // Agreement: forwarded as ONE consistent value on the routed path.
    let out = gw
        .call_tool(json!({ "name": "search_documents",
            "arguments": { "graph_id": "scratch", "graphId": "scratch", "q": 1 } }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["cell"], json!("scratch"));
    let call = bodies_to(&server, "/g/scratch/mcp")
        .await
        .into_iter()
        .find(|b| b["method"] == json!("tools/call"))
        .unwrap();
    assert_eq!(call["params"]["arguments"], json!({ "graph_id": "scratch", "q": 1 }));
}

#[tokio::test]
async fn graph_id_naming_an_unlisted_graph_is_refused_before_any_graph_path_request() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "ghost" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("'ghost'"), "got: {msg}");
    assert!(msg.contains("not in this identity's list_graphs"), "got: {msg}");
    assert!(msg.contains("will not activate or create"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/ghost/").await, 0, "no request may touch the unlisted graph's path");
    // The listing was refreshed once before refusing (a graph may have been
    // created after connect): connect + tools/list + refresh.
    assert_eq!(requests_to(&server, "/control/mcp").await, 3);
}

#[tokio::test]
async fn listing_is_replaced_on_refresh_so_a_revoked_graph_stops_routing() {
    // M6: a row that disappears from list_graphs is forgotten, not unioned.
    let server = MockServer::start().await;
    let with_scratch = listing_response(vec![row(OWNER, "notes", "active"), row(OWNER, "scratch", "active")]);
    let without = listing_response(vec![row(OWNER, "notes", "active")]);
    mount_control_with(&server, "/control/mcp", FlipAfter::new(1, with_scratch, without)).await;
    mount_warm_cell(&server, "notes").await;
    mount_warm_cell(&server, "scratch").await;

    let gw = connect(&server, "notes").await;
    // A miss triggers the refresh, which now omits scratch.
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "ghost" } }))
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("ghost"));
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "scratch" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("'scratch'") && msg.contains("not in this identity's list_graphs"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/scratch/").await, 0, "a revoked graph must never be activated");
}

#[tokio::test]
async fn a_tombstoned_listing_row_is_never_activated() {
    let server = MockServer::start().await;
    mount_control_with(
        &server,
        "/control/mcp",
        listing_response(vec![row(OWNER, "notes", "active"), row(OWNER, "dead", "tombstoned")]),
    )
    .await;
    mount_warm_cell(&server, "notes").await;
    mount_warm_cell(&server, "dead").await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "dead" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("'dead'") && msg.contains("not activatable"), "got: {msg}");
    assert!(msg.contains("tombstoned"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/dead/").await, 0);
}

#[tokio::test]
async fn a_graph_shared_by_another_owner_routes_to_the_listing_owners_path() {
    // M5: the owner comes from the LISTING, never from an argument.
    let server = MockServer::start().await;
    mount_control_with(
        &server,
        "/control/mcp",
        listing_response(vec![row(OWNER, "notes", "active"), row(FRIEND, "shared", "active")]),
    )
    .await;
    mount_warm_cell(&server, "notes").await;
    mount_warm_cell_of(&server, FRIEND_PATH, "shared").await;

    let gw = connect(&server, "notes").await;
    let out = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "shared", "owner": "user:mallory" } }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["cell"], json!("shared"));
    assert_eq!(out["structuredContent"]["owner_path"], json!(FRIEND_PATH));
    assert_eq!(requests_to(&server, &format!("/o/{OWNER_PATH}/g/shared/")).await, 0);
    assert_eq!(requests_to(&server, "/o/user%3Amallory/").await, 0);
}

#[tokio::test]
async fn a_graph_id_listed_under_two_owners_is_refused_naming_both() {
    let server = MockServer::start().await;
    mount_control_with(
        &server,
        "/control/mcp",
        listing_response(vec![
            row(OWNER, "notes", "active"),
            row(OWNER, "dup", "active"),
            row(FRIEND, "dup", "active"),
        ]),
    )
    .await;
    mount_warm_cell(&server, "notes").await;
    mount_warm_cell(&server, "dup").await;
    mount_warm_cell_of(&server, FRIEND_PATH, "dup").await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "dup" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("ambiguous"), "got: {msg}");
    assert!(msg.contains(OWNER) && msg.contains(FRIEND), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/dup/").await, 0);
}

#[tokio::test]
async fn gateway_403_on_a_listed_graph_is_surfaced_verbatim_and_never_reaches_mcp() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "private"]).await;
    mount_warm_cell(&server, "notes").await;
    Mock::given(method("POST"))
        .and(path(activate_path("private")))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({ "error": "forbidden: viewer role required" })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "private" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("HTTP 403"), "got: {msg}");
    assert!(msg.contains("forbidden: viewer role required"), "got: {msg}");
    assert!(msg.contains("private"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/private/mcp").await, 0);
    server.verify().await;
}

#[tokio::test]
async fn gateway_404_for_a_listed_but_vanished_graph_is_surfaced_verbatim() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "gone"]).await;
    mount_warm_cell(&server, "notes").await;
    Mock::given(method("POST"))
        .and(path(activate_path("gone")))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": "not found: graph 'user:owner-sub/gone'" })))
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "gone" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("HTTP 404"), "got: {msg}");
    assert!(msg.contains("not found: graph 'user:owner-sub/gone'"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/gone/mcp").await, 0);
}

#[tokio::test]
async fn unknown_tool_is_tool_not_found_naming_the_tool() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = connect(&server, "notes").await;
    let err = gw
        .call_tool(json!({ "name": "frobnicate", "arguments": {} }))
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<ToolNotFound>().is_some(), "got: {err:#}");
    assert_eq!(format!("{err:#}"), "unknown tool: frobnicate");
    // Never forwarded to either upstream.
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests.iter().any(|r| String::from_utf8_lossy(&r.body).contains("frobnicate")),
        "unknown tool must not be forwarded"
    );
}

// ------------------------------------------------------- wait-for-routable

#[tokio::test]
async fn cell_that_answers_503_forever_times_out_naming_budget_and_last_state() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "error": "graph 'user:owner-sub/notes' is at capacity, try again shortly",
            "code": "at_capacity", "retryAfter": 10
        })))
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let err = gw.warm().await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("is not routable after"), "got: {msg}");
    assert!(msg.contains("activation budget 0.4s"), "got: {msg}");
    assert!(msg.contains("last observed activation state: cell path answered HTTP 503"), "got: {msg}");
    assert!(msg.contains("at_capacity"), "got: {msg}");
    assert!(!msg.to_lowercase().contains("not found"), "must never claim not-found: {msg}");
    assert!(gw.last_wait_polls() >= 2, "polls: {}", gw.last_wait_polls());
    // L3: re-probes back off (20/40/80/160 ms → a handful, not dozens) and
    // the wait POSTed activate at most once (here: zero — the kick did it).
    assert!(requests_to(&server, "/g/notes/mcp").await <= 8, "{}", requests_to(&server, "/g/notes/mcp").await);
    assert_eq!(requests_to(&server, "/g/notes/activate").await, 1);

    // A tool call gets the same truthful error (fresh budget), not a hang,
    // and its label counts retries, not polls.
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": {} }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("last observed activation state"), "got: {msg}");
    assert!(requests_to(&server, "/g/notes/activate").await <= 2, "at most one activate per wait");
}

#[tokio::test]
async fn a_hung_upstream_cannot_stretch_the_wait_past_its_budget() {
    // H1: one 503 delayed 2.5 s on a 0.4 s budget must not make warm() take 2.5 s.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": "slow", "code": "at_capacity" }))
                .set_delay(Duration::from_millis(2500)),
        )
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let started = Instant::now();
    let err = gw.warm().await.unwrap_err();
    let elapsed = started.elapsed();
    let msg = format!("{err:#}");
    assert!(elapsed < Duration::from_millis(1000), "warm() took {elapsed:?}: {msg}");
    assert!(msg.contains("is not routable after"), "got: {msg}");
    assert!(msg.contains("request timed out after"), "got: {msg}");
    assert!(msg.contains("(initialize)"), "got: {msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiter_queued_behind_warm_pays_one_budget_not_two() {
    // M2: background warm() holds the session lock on a 503-forever cell; a
    // foreground call must still answer within ~1× its own budget.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({ "error": "wedged", "code": "at_capacity" })))
        .mount(&server)
        .await;

    let gw = Arc::new(connect(&server, "notes").await);
    let warm = Arc::clone(&gw);
    let warm_task = tokio::spawn(async move { warm.warm().await });
    tokio::time::sleep(Duration::from_millis(50)).await; // let warm take the lock
    let started = Instant::now();
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": {} }))
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    let msg = format!("{err:#}");
    assert!(elapsed < Duration::from_millis(650), "foreground waited {elapsed:?}: {msg}");
    // Whether it timed out on the lock ("queued behind another waiter") or
    // got the lock late and ran a short wait of its own, the error is truthful.
    assert!(msg.contains("is not routable after 0."), "got: {msg}");
    assert!(msg.contains("last observed activation state"), "got: {msg}");
    assert!(msg.contains("HTTP 503") && msg.contains("wedged"), "got: {msg}");
    let _ = warm_task.await.unwrap();
}

#[tokio::test]
async fn cell_that_flips_to_routable_after_n_polls_succeeds_and_reports_n() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "activation": { "activationId": "cell-1", "phase": "scheduling", "events": [] },
            "pollUrl": "/activations/cell-1"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let hydrating = ResponseTemplate::new(200).set_body_json(json!({
        "activationId": "cell-1", "phase": "hydrating",
        "events": [{ "sequence": 2, "phase": "hydrating", "detail": "cell is hydrating its registered generation" }]
    }));
    let ready = ResponseTemplate::new(200).set_body_json(json!({
        "activationId": "cell-1", "phase": "ready",
        "events": [{ "sequence": 3, "phase": "ready", "detail": "graph tools are ready" }]
    }));
    const N: usize = 3;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(FlipAfter::new(N, hydrating, ready))
        .expect(N as u64 + 1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    gw.warm().await.unwrap();
    // N hydrating polls + 1 ready poll + 1 MCP probe — the number the log reports.
    assert_eq!(gw.last_wait_polls(), N as u64 + 2);
    server.verify().await;
}

#[tokio::test]
async fn activation_phase_failed_is_surfaced_with_its_error_not_retried() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "activation": { "phase": "scheduling", "events": [] },
            "pollUrl": "/activations/cell-1"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "activationId": "cell-1", "phase": "failed", "error": "cell activation failed: pod evicted",
            "events": [{ "sequence": 4, "phase": "failed", "detail": "cell activation failed" }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("activation failed"), "got: {msg}");
    assert!(msg.contains("pod evicted"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/notes/mcp").await, 0);
    server.verify().await;
}

#[tokio::test]
async fn an_off_origin_poll_url_is_refused_and_never_receives_credentials() {
    // M4: a pollUrl pointing elsewhere must not be followed with the bearer.
    let server = MockServer::start().await;
    let foreign = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "activation": { "phase": "scheduling", "events": [] },
            "pollUrl": format!("{}/activations/cell-1", foreign.uri())
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "phase": "ready", "events": [] })))
        .expect(0)
        .mount(&foreign)
        .await;

    let err = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .err()
        .expect("an off-origin pollUrl must be refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("off-origin"), "got: {msg}");
    assert!(msg.contains(&foreign.uri()), "got: {msg}");
    assert_eq!(foreign.received_requests().await.unwrap().len(), 0);
    foreign.verify().await;
}

/// Mount: activate → 200 ready:true; cell path → 202 graph_activating whose
/// pollUrl is `poll_url`. The wait then has to decide whether to follow it.
async fn mount_cell_activating_with_poll_url(server: &MockServer, poll_url: String) {
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "code": "graph_activating", "activationId": "cell-1", "phase": "hydrating",
            "pollUrl": poll_url
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_poll_url_whose_host_merely_starts_with_the_base_is_refused() {
    // F2: `{base}.evil.example/…` — a prefix check on the base string would
    // pass this; origin equality must not.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_cell_activating_with_poll_url(
        &server,
        format!("{}.evil.example/activations/cell-1", server.uri()),
    )
    .await;

    let gw = connect(&server, "notes").await;
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("off-origin"), "got: {msg}");
    assert!(msg.contains(".evil.example"), "got: {msg}");
    assert_eq!(requests_to(&server, "/activations/").await, 0);
}

#[tokio::test]
async fn a_poll_url_with_the_base_as_userinfo_before_a_foreign_host_is_refused() {
    // F2: `{base}@{foreign}/…` parses as userinfo + a foreign host; a prefix
    // check would send the bearer there.
    let server = MockServer::start().await;
    let foreign = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    let foreign_host = foreign.uri().trim_start_matches("http://").to_string();
    mount_cell_activating_with_poll_url(
        &server,
        format!("{}@{foreign_host}/activations/cell-1", server.uri()),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "phase": "ready", "events": [] })))
        .expect(0)
        .mount(&foreign)
        .await;

    let gw = connect(&server, "notes").await;
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("off-origin"), "got: {msg}");
    assert_eq!(foreign.received_requests().await.unwrap().len(), 0);
    foreign.verify().await;
}

#[tokio::test]
async fn an_on_origin_poll_url_that_redirects_off_origin_is_not_followed() {
    // F1: a 302 from the gateway's own poll URL to a foreign host must not be
    // followed (reqwest would keep x-pn-on-behalf-of) nor its body trusted.
    let server = MockServer::start().await;
    let foreign = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "ready": false, "activation": { "phase": "scheduling", "events": [] },
            "pollUrl": "/activations/cell-1"
        })))
        .mount(&server)
        .await;
    let target = format!("{}/activations/cell-1", foreign.uri());
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", target.as_str()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "phase": "ready", "events": [] })))
        .expect(0)
        .mount(&foreign)
        .await;

    let gw = connect(&server, "notes").await;
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("HTTP 302"), "got: {msg}");
    assert!(msg.contains("redirect to") && msg.contains(&target), "got: {msg}");
    assert!(msg.contains("not followed"), "got: {msg}");
    assert_eq!(foreign.received_requests().await.unwrap().len(), 0);
    assert_eq!(requests_to(&server, "/g/notes/mcp").await, 0, "a redirect must not count as ready");
    foreign.verify().await;
}

#[tokio::test]
async fn a_200_on_health_is_not_readiness_only_mcp_initialize_counts() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    // The keepwarm once logged a false "warm" off a 200 like these. They must
    // never be consulted: expect(0) fails the test if any readiness logic
    // starts probing them.
    Mock::given(method("GET"))
        .and(path(format!("/o/{OWNER_PATH}/g/notes/health")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "ok" })))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .expect(0)
        .mount(&server)
        .await;
    // `activate` claims a running pod — boot-ready, per CEL-AVAIL-001 not
    // proof of routability either.
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({ "error": "wedged", "code": "at_capacity" })))
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("is not routable"), "got: {msg}");
    assert!(msg.contains("HTTP 503"), "got: {msg}");
    assert!(msg.contains("wedged"), "got: {msg}");
    assert_eq!(requests_to(&server, "/health").await, 0);
    server.verify().await;
}

#[tokio::test]
async fn mid_session_202_invalidates_the_session_rewaits_then_retries_the_call() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "serverInfo": { "name": "gardend" } })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [{ "name": "search_documents" }] })))
        .mount(&server)
        .await;
    // First tools/call: the cell went cold between calls → 202 graph_activating.
    let activating = ResponseTemplate::new(202).set_body_json(json!({
        "code": "graph_activating", "graph": { "owner": OWNER, "graphId": "notes" },
        "activationId": "cell-2", "phase": "hydrating",
        "pollUrl": "/activations/cell-2", "eventsUrl": "/activations/cell-2/events"
    }));
    let served = rpc_ok(json!({ "content": [], "structuredContent": { "cell": "notes" } }));
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(FlipAfter::new(1, activating, served))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/activations/cell-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "activationId": "cell-2", "phase": "ready",
            "events": [{ "sequence": 3, "phase": "ready", "detail": "graph tools are ready" }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = connect(&server, "notes").await;
    gw.warm().await.unwrap();
    let out = gw
        .call_tool(json!({ "name": "search_documents", "arguments": {} }))
        .await
        .unwrap();
    assert_eq!(out["structuredContent"]["cell"], json!("notes"));
    // The re-wait consumed the 202's pollUrl (1 poll) then re-proved with
    // initialize (1 probe).
    assert_eq!(gw.last_wait_polls(), 2);
    server.verify().await;
}

#[tokio::test]
async fn repair_required_503_at_connect_is_surfaced_immediately() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "error": "graph 'user:owner-sub/notes' requires snapshot-authority repair",
            "code": "graph_repair_required",
            "detail": "gardend refused an impossible snapshot-authority repair",
            "runbook": "docs/runbooks/snapshot-authority-repair.md"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let err = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .err()
        .expect("a typed non-retryable 503 at connect is fatal");
    let msg = format!("{err:#}");
    assert!(msg.contains("HTTP 503"), "got: {msg}");
    assert!(msg.contains("graph_repair_required"), "got: {msg}");
    assert!(msg.contains("snapshot-authority-repair.md"), "got: {msg}");
    server.verify().await;
}

#[tokio::test]
async fn repair_required_503_on_the_cell_path_is_one_request_and_an_immediate_error() {
    // M1: a typed non-retryable 503 must not be polled until the budget.
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    Mock::given(method("POST"))
        .and(path(activate_path("notes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("notes")))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "error": "graph 'user:owner-sub/notes' requires snapshot-authority repair",
            "code": "graph_repair_required",
            "detail": "gardend refused an impossible snapshot-authority repair"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let opts = GatewayOptions {
        activation_timeout: Duration::from_secs(5),
        ..fast()
    };
    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), opts)
        .await
        .unwrap();
    let started = Instant::now();
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(started.elapsed() < Duration::from_millis(500), "took {:?}", started.elapsed());
    assert!(msg.contains("HTTP 503") && msg.contains("graph_repair_required"), "got: {msg}");
    assert!(!msg.contains("is not routable after"), "must not be a budget timeout: {msg}");
    assert_eq!(requests_to(&server, "/g/notes/mcp").await, 1);
    server.verify().await;
}

// ------------------------------------------- structuredContent at the client

/// The gateway's control plane answers `list_graphs` with a bare JSON array in
/// `structuredContent`; the MCP spec (and Claude Code's client) require an
/// object. The backend passes the upstream envelope through verbatim; the
/// stdio edge wraps it as `{ "items": [...] }` so the client accepts the call.
#[tokio::test]
async fn control_tool_array_structured_content_reaches_the_client_as_items() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "scratch"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = connect(&server, "notes").await;
    let rows = json!([row(OWNER, "notes", "active"), row(OWNER, "scratch", "active")]);

    // Backend level: verbatim (the array is what the gateway sent).
    let raw = gw
        .call_tool(json!({ "name": "control_list_graphs", "arguments": {} }))
        .await
        .unwrap();
    assert_eq!(raw["structuredContent"], rows);

    // Client level: an object, with the array intact under `items`; `content`
    // is exactly what the gateway sent.
    let seen = client_calls(&gw, "control_list_graphs", json!({})).await;
    assert_eq!(seen["structuredContent"], json!({ "items": rows }), "got: {seen}");
    assert!(seen["structuredContent"].is_object());
    assert_eq!(seen["content"], json!([]));
}

/// A cell tool that already answers with an object is not touched on the way
/// to the client — no `items` wrapping, no key added, nothing dropped.
#[tokio::test]
async fn cell_tool_object_structured_content_is_unchanged_at_the_client() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = connect(&server, "notes").await;
    let raw = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "query": "q" } }))
        .await
        .unwrap();
    let seen = client_calls(&gw, "search_documents", json!({ "query": "q" })).await;
    assert_eq!(seen, raw, "client envelope must equal the upstream envelope");
    assert_eq!(
        seen["structuredContent"],
        json!({ "cell": "notes", "owner_path": OWNER_PATH })
    );
    assert!(seen["structuredContent"].get("items").is_none());
}
