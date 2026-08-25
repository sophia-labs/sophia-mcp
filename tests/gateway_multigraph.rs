//! End-to-end tests of the multi-graph `GatewayBackend` against a wiremock
//! platform-next gateway: control-plane + cell union, `graph_id` routing by
//! path, wait-for-routable across activation, and truthful surfacing of every
//! failing direction (unlisted graph, gateway 403, unknown tool, budget expiry,
//! health-200-is-not-readiness).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{json, Value};
use sophia_mcp::backend::{AuthHeaders, Backend, GatewayBackend, GatewayOptions, ToolNotFound};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const OWNER: &str = "user:owner-sub";
const OWNER_PATH: &str = "user%3Aowner-sub";

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
        unified_mcp_fallback: false,
    }
}

fn rpc_ok(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "jsonrpc": "2.0", "id": "x", "result": result }))
}

fn cell_path(graph: &str) -> String {
    format!("/o/{OWNER_PATH}/g/{graph}/mcp")
}

fn activate_path(graph: &str) -> String {
    format!("/o/{OWNER_PATH}/g/{graph}/activate")
}

/// Mount the control plane at `at`: `list_graphs` (tools/call) + tools/list.
async fn mount_control(server: &MockServer, at: &str, graphs: &[&str]) {
    let rows: Vec<Value> = graphs
        .iter()
        .map(|g| json!({ "owner": OWNER, "graphId": g, "lifecycleState": "active" }))
        .collect();
    Mock::given(method("POST"))
        .and(path(at))
        .and(header("authorization", "Bearer service-token"))
        .and(header("x-pn-on-behalf-of", "owner-sub"))
        .and(body_partial_json(json!({ "method": "tools/call", "params": { "name": "list_graphs" } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": rows })))
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

/// A warm cell: activate → 200 ready:true; initialize / tools/list / tools/call → 200.
async fn mount_warm_cell(server: &MockServer, graph: &str) {
    Mock::given(method("POST"))
        .and(path(activate_path(graph)))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ready": true })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path(graph)))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(rpc_ok(json!({ "protocolVersion": "2025-03-26", "capabilities": { "tools": {} },
            "serverInfo": { "name": "gardend", "version": "0.0" } })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path(graph)))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(rpc_ok(json!({ "tools": [
            { "name": "search_documents", "inputSchema": { "type": "object", "properties": { "query": { "type": "string" } } } },
            { "name": "list_graphs", "inputSchema": { "type": "object" } }
        ]})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path(graph)))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": graph } })))
        .mount(server)
        .await;
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

fn requests_to(server: &MockServer, needle: &str) -> usize {
    futures_block(server.received_requests())
        .unwrap_or_default()
        .iter()
        .filter(|r| r.url.path().contains(needle))
        .count()
}

fn futures_block<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

// ------------------------------------------------------------------ connect

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    assert_eq!(gw.mcp_url_for("notes"), format!("{}{}", server.uri(), cell_path("notes")));
    assert_eq!(gw.control_url(), format!("{}/control/mcp", server.uri()));
    // Nothing on the cell path yet: connect only kicked the activation.
    assert_eq!(requests_to(&server, "/g/notes/mcp"), 0);

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
    assert_eq!(names, vec!["search_documents", "list_graphs", "create_graph"]);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
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

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let init = gw
        .initialize(json!({ "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": { "name": "claude-code" } }))
        .await
        .unwrap();
    assert_eq!(init["serverInfo"]["name"], json!("sophia-mcp"));
    assert_eq!(init["serverInfo"]["version"], json!(env!("CARGO_PKG_VERSION")));
    assert_eq!(init["protocolVersion"], json!("2025-03-26"));
    assert_eq!(requests_to(&server, "/g/notes/mcp"), 0);
    assert_eq!(requests_to(&server, "/activations/"), 0);
}

#[tokio::test(flavor = "multi_thread")]
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
        .call_tool(json!({ "name": "create_graph", "arguments": { "graphId": "n2" } }))
        .await;
    // /mcp (mount_control) serves list_graphs + tools/list only; the call
    // reaching it (and failing there, not in routing) proves the fallback URL.
    let msg = format!("{:#}", out.unwrap_err());
    assert!(msg.contains(&format!("{}/mcp", server.uri())), "got: {msg}");
}

// ------------------------------------------------------------- multi-graph

#[tokio::test(flavor = "multi_thread")]
async fn tools_list_is_the_union_with_colliding_control_tools_prefixed_and_routed() {
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let tools = gw.list_tools(json!({})).await.unwrap();
    let names: Vec<&str> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["search_documents", "list_graphs", "control_list_graphs", "create_graph"]
    );
    // Cell tools advertise the routing argument; control tools do not.
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["graph_id"]["type"], json!("string"));
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["graphId"]["type"], json!("string"));
    assert_eq!(tools["tools"][0]["inputSchema"]["properties"]["query"]["type"], json!("string"));
    assert!(tools["tools"][3]["inputSchema"]["properties"].get("graph_id").is_none());

    // Unprefixed `list_graphs` is the cell's; the prefixed one is control's,
    // forwarded under its upstream name.
    let cell = gw.call_tool(json!({ "name": "list_graphs", "arguments": {} })).await.unwrap();
    assert_eq!(cell["structuredContent"]["cell"], json!("notes"));
    let control = gw
        .call_tool(json!({ "name": "control_list_graphs", "arguments": {} }))
        .await
        .unwrap();
    assert!(control["structuredContent"].is_array(), "got: {control}");
    let created = gw
        .call_tool(json!({ "name": "create_graph", "arguments": { "graphId": "n2" } }))
        .await
        .unwrap();
    assert_eq!(created["structuredContent"]["control"], json!("create_graph"));
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
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
    // The argument stays in the body: the target cell sees its own id.
    Mock::given(method("POST"))
        .and(path(cell_path("scratch")))
        .and(body_partial_json(json!({ "method": "tools/call",
            "params": { "name": "search_documents", "arguments": { "graph_id": "scratch" } } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": "scratch", "via": "graph_id" } })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(cell_path("scratch")))
        .and(body_partial_json(json!({ "method": "tools/call",
            "params": { "name": "search_documents", "arguments": { "graphId": "scratch" } } })))
        .respond_with(rpc_ok(json!({ "content": [], "structuredContent": { "cell": "scratch", "via": "graphId" } })))
        .expect(1)
        .mount(&server)
        .await;

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
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
    assert_eq!(a["structuredContent"]["via"], json!("graph_id"));
    let b = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "query": "q", "graphId": "scratch" } }))
        .await
        .unwrap();
    assert_eq!(b["structuredContent"]["via"], json!("graphId"));
    // Naming the bound graph explicitly is not a sibling route.
    let same = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "notes" } }))
        .await
        .unwrap();
    assert_eq!(same["structuredContent"]["cell"], json!("notes"));
    // activate ×1 + initialize ×1 for scratch across two calls = one session.
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_id_naming_an_unlisted_graph_is_refused_before_any_graph_path_request() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "ghost" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("'ghost'"), "got: {msg}");
    assert!(msg.contains("not in this identity's list_graphs"), "got: {msg}");
    assert!(msg.contains("will not activate or create"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/ghost/"), 0, "no request may touch the unlisted graph's path");
    // The listing was refreshed once before refusing (a graph may have been
    // created after connect).
    assert_eq!(requests_to(&server, "/control/mcp"), 2 + 1 /* connect + tools/list + refresh */);
}

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "private" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("HTTP 403"), "got: {msg}");
    assert!(msg.contains("forbidden: viewer role required"), "got: {msg}");
    assert!(msg.contains("private"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/private/mcp"), 0);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_404_for_a_listed_but_vanished_graph_is_surfaced_verbatim() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes", "gone"]).await;
    mount_warm_cell(&server, "notes").await;
    Mock::given(method("POST"))
        .and(path(activate_path("gone")))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": "not found: graph 'user:owner-sub/gone'" })))
        .mount(&server)
        .await;

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": { "graph_id": "gone" } }))
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("HTTP 404"), "got: {msg}");
    assert!(msg.contains("not found: graph 'user:owner-sub/gone'"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/gone/mcp"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_tool_is_tool_not_found_naming_the_tool() {
    let server = MockServer::start().await;
    mount_control(&server, "/control/mcp", &["notes"]).await;
    mount_warm_cell(&server, "notes").await;

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
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

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let err = gw.warm().await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("is not routable after"), "got: {msg}");
    assert!(msg.contains("activation budget 0.4s"), "got: {msg}");
    assert!(msg.contains("last observed activation state: cell path answered HTTP 503"), "got: {msg}");
    assert!(msg.contains("at_capacity"), "got: {msg}");
    assert!(!msg.to_lowercase().contains("not found"), "must never claim not-found: {msg}");
    assert!(gw.last_wait_polls() >= 2, "polls: {}", gw.last_wait_polls());

    // A tool call gets the same truthful error (fresh budget), not a hang.
    let err = gw
        .call_tool(json!({ "name": "search_documents", "arguments": {} }))
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("last observed activation state"), "got: {err:#}");
}

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    gw.warm().await.unwrap();
    // N hydrating polls + 1 ready poll + 1 MCP probe — the number the log reports.
    assert_eq!(gw.last_wait_polls(), N as u64 + 2);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("activation failed"), "got: {msg}");
    assert!(msg.contains("pod evicted"), "got: {msg}");
    assert_eq!(requests_to(&server, "/g/notes/mcp"), 0);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
    let msg = format!("{:#}", gw.warm().await.unwrap_err());
    assert!(msg.contains("is not routable"), "got: {msg}");
    assert!(msg.contains("HTTP 503"), "got: {msg}");
    assert!(msg.contains("wedged"), "got: {msg}");
    assert_eq!(requests_to(&server, "/health"), 0);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
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

    let gw = GatewayBackend::connect(&server.uri(), OWNER, "notes", auth(), fast())
        .await
        .unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn repair_required_503_is_surfaced_immediately_not_retried_until_budget() {
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
