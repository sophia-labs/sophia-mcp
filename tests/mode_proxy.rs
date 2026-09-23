//! Agent declaration and live MCP modes over an actual HTTP JSON-RPC proxy
//! hop. This exercises the stdio framing, `notifications/tools/list_changed`,
//! and discovery exactly as a client observes them; the backend is a local
//! test server with Garden-shaped replies whose mode RDF can be edited while
//! the proxy runs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const A: &str = "http://mnemosyne.dev/agent#";
const T: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

fn rpc(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(json!({"jsonrpc":"2.0","id":"backend","result":result}))
}

/// The mode RDF of `agent-deadbeef` as the graph currently holds it:
/// (slug, relation, access, allowed tools).
#[derive(Default)]
struct Graph {
    modes: Vec<(String, String, String, Vec<String>)>,
    unreadable: bool,
}

impl Graph {
    fn initial() -> Self {
        let mode = |slug: &str, rel: &str, access: &str, tools: &[&str]| {
            (
                slug.to_owned(),
                rel.to_owned(),
                access.to_owned(),
                tools.iter().map(|t| (*t).to_owned()).collect(),
            )
        };
        Self {
            modes: vec![
                mode("reader", "defaultMode", "read", &["read_document"]),
                mode("writer", "mayUseMode", "write", &["write_document"]),
            ],
            unreadable: false,
        }
    }

    fn rows(&self) -> Vec<Value> {
        let mut rows = Vec::new();
        for (slug, relation, access, tools) in &self.modes {
            let mode = format!("<urn:sophia:mode:{slug}>");
            let rel = format!("<{A}{relation}>");
            rows.push(json!({"rel":rel,"mode":mode,"p":format!("<{T}>"),"o":format!("<{A}Mode>")}));
            rows.push(json!({"rel":rel,"mode":mode,"p":format!("<{A}access>"),"o":json!(access).to_string()}));
            for tool in tools {
                rows.push(json!({"rel":rel,"mode":mode,"p":format!("<{A}allowsTool>"),"o":json!(tool).to_string()}));
            }
        }
        rows
    }
}

struct SparqlCell(Arc<Mutex<Graph>>);

impl Respond for SparqlCell {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let query = body["params"]["arguments"]["query"].as_str().unwrap();
        assert!(query.contains("GRAPH <urn:mnemosyne:local:graph:lab:user:rdf>"));
        assert!(query.contains("<urn:sophia:agent:agent-deadbeef>"));
        let graph = self.0.lock().unwrap();
        if graph.unreadable {
            return rpc(
                json!({"isError":true,"content":[{"type":"text","text":"cell unavailable"}]}),
            );
        }
        rpc(json!({"structuredContent":{"resultType":"solutions","rows":graph.rows()}}))
    }
}

async fn cell(graph: Arc<Mutex<Graph>>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({"method":"initialize"})))
        .respond_with(rpc(json!({"capabilities":{"tools":{}}})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({"method":"tools/list"})))
        .respond_with(rpc(json!({"tools":[
            {"name":"read_document","_meta":{"sophia.local.requiredScopes":["documents.read"]}},
            {"name":"search_documents","_meta":{"sophia.local.requiredScopes":["documents.read"]}},
            {"name":"write_document","_meta":{"sophia.local.requiredScopes":["documents.write"]}}
        ]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(
            json!({"method":"tools/call","params":{"name":"sparql_query"}}),
        ))
        .respond_with(SparqlCell(graph))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({"method":"tools/call"})))
        .respond_with(rpc(
            json!({"content":[],"structuredContent":{"forwarded":true}}),
        ))
        .mount(&server)
        .await;
    server
}

/// A stdio client that separates responses from server notifications and
/// insists every stdout line is a JSON-RPC 2.0 message.
struct Client {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    next_id: i64,
    list_changed: usize,
}

impl Client {
    fn spawn(server: &MockServer, extra: &[&str]) -> Self {
        let uri = format!("{}/mcp", server.uri());
        let mut args = vec!["--backend", uri.as_str(), "--graph", "lab"];
        args.extend_from_slice(extra);
        let mut child = Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
            .args(&args)
            .env("SOPHIA_MCP_NO_UPDATE_CHECK", "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            stdin,
            stdout,
            next_id: 0,
            list_changed: 0,
        }
    }

    async fn line(&mut self, wait: Duration) -> Option<Value> {
        let line = tokio::time::timeout(wait, self.stdout.next_line())
            .await
            .ok()?
            .unwrap()
            .expect("stdout closed");
        let message: Value = serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("stdout carried a non-JSON line {line:?}: {e}"));
        assert_eq!(message["jsonrpc"], "2.0", "{line}");
        if message.get("id").is_none() {
            assert_eq!(
                message["method"], "notifications/tools/list_changed",
                "{line}"
            );
            assert!(message.get("params").is_none_or(Value::is_object));
            self.list_changed += 1;
        }
        Some(message)
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let mut line = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await.unwrap();
        self.stdin.flush().await.unwrap();
        loop {
            let message = self
                .line(Duration::from_secs(15))
                .await
                .expect("no response");
            if message["id"] == id {
                return message;
            }
        }
    }

    async fn call(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":arguments}))
            .await
    }

    async fn tools(&mut self) -> Vec<String> {
        self.request("tools/list", json!({})).await["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect()
    }

    /// Wait (without sending anything) for an unsolicited list_changed.
    async fn await_list_changed(&mut self) {
        let seen = self.list_changed;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.list_changed == seen {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                self.line(left).await.is_some(),
                "no notifications/tools/list_changed arrived"
            );
        }
    }

    /// Everything the notification counter saw since the last call.
    fn take_list_changed(&mut self) -> usize {
        std::mem::take(&mut self.list_changed)
    }
}

fn has(tools: &[String], name: &str) -> bool {
    tools.iter().any(|t| t == name)
}

#[tokio::test]
async fn preset_agent_id_can_switch_modes_and_refresh_its_tool_catalog() {
    let graph = Arc::new(Mutex::new(Graph::initial()));
    let server = cell(graph).await;
    let mut client = Client::spawn(&server, &["--agent-id", "agent-deadbeef"]);

    let init = client.request("initialize", json!({})).await;
    assert_eq!(init["result"]["capabilities"]["tools"]["listChanged"], true);
    let before = client.tools().await;
    assert!(has(&before, "read_document") && has(&before, "sophia_mode_set"));
    assert!(!has(&before, "write_document"));

    let switched = client
        .call(
            "sophia_mode_set",
            json!({"modeIri":"urn:sophia:mode:writer"}),
        )
        .await;
    assert_eq!(switched["result"]["structuredContent"]["changed"], true);
    // The notification follows its response on the next line.
    let note = client.line(Duration::from_secs(5)).await.unwrap();
    assert_eq!(note["method"], "notifications/tools/list_changed");
    let after = client.tools().await;
    assert!(has(&after, "write_document"));
    assert!(!has(&after, "read_document"));
    client.child.kill().await.unwrap();
}

#[tokio::test]
async fn runtime_declaration_follows_live_graph_edits() {
    let graph = Arc::new(Mutex::new(Graph::initial()));
    let server = cell(graph.clone()).await;
    // TTL 0: every list/call re-reads; poll 200ms: edits arrive unprompted.
    let mut client = Client::spawn(
        &server,
        &["--mode-cache-ttl-ms", "0", "--mode-poll-ms", "200"],
    );
    let init = client.request("initialize", json!({})).await;
    assert_eq!(init["result"]["capabilities"]["tools"]["listChanged"], true);

    // Undeclared: the full catalogue plus the three agent tools, no mode tools.
    let full = client.tools().await;
    for name in ["read_document", "search_documents", "write_document"] {
        assert!(has(&full, name), "{name} missing from {full:?}");
    }
    assert!(has(&full, "sophia_agent_declare") && has(&full, "sophia_agent_status"));
    assert!(!has(&full, "sophia_mode_set"));
    let passthrough = client.call("write_document", json!({})).await;
    assert_eq!(
        passthrough["result"]["structuredContent"]["forwarded"],
        true
    );
    let bad = client
        .call("sophia_agent_declare", json!({"agentId":"Agent-XYZ"}))
        .await;
    assert!(bad["error"].is_object());

    // Declare: default mode, list_changed, narrowed discovery.
    let declared = client
        .call("sophia_agent_declare", json!({"agentId":"agent-deadbeef"}))
        .await;
    assert_eq!(
        declared["result"]["structuredContent"]["activeMode"],
        "urn:sophia:mode:reader"
    );
    assert_eq!(
        declared["result"]["structuredContent"]["modes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    client.line(Duration::from_secs(5)).await.unwrap();
    assert_eq!(client.take_list_changed(), 1);
    let reader = client.tools().await;
    assert!(has(&reader, "read_document") && has(&reader, "sophia_mode_set"));
    assert!(!has(&reader, "search_documents") && !has(&reader, "write_document"));
    assert!(client.call("write_document", json!({})).await["error"].is_object());

    // Edit the active mode in the graph: list_changed arrives without a call.
    graph.lock().unwrap().modes[0]
        .3
        .push("search_documents".to_owned());
    client.await_list_changed().await;
    assert!(has(&client.tools().await, "search_documents"));
    client.take_list_changed();

    // A brand-new mode is selectable immediately.
    graph.lock().unwrap().modes.push((
        "scribe".into(),
        "mayUseMode".into(),
        "write".into(),
        vec!["write_document".into()],
    ));
    let set = client
        .call(
            "sophia_mode_set",
            json!({"modeIri":"urn:sophia:mode:scribe"}),
        )
        .await;
    assert_eq!(set["result"]["structuredContent"]["changed"], true, "{set}");
    client.line(Duration::from_secs(5)).await.unwrap();
    assert_eq!(client.take_list_changed(), 1);
    let ok = client.call("write_document", json!({})).await;
    assert_eq!(ok["result"]["structuredContent"]["forwarded"], true, "{ok}");

    // Revoke the active mode: fall back to the default, notify, explain.
    graph.lock().unwrap().modes.retain(|m| m.0 != "scribe");
    client.await_list_changed().await;
    let refused = client.call("write_document", json!({})).await;
    let message = refused["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("revoked") && message.contains("urn:sophia:mode:reader"),
        "{message}"
    );
    let fallback = client.tools().await;
    assert!(has(&fallback, "read_document") && !has(&fallback, "write_document"));
    client.take_list_changed();

    // Refresh failure: calls fail closed (retryable), controls still answer.
    graph.lock().unwrap().unreadable = true;
    let denied = client.call("read_document", json!({})).await;
    assert!(
        denied["error"]["message"]
            .as_str()
            .unwrap()
            .contains("retryable"),
        "{denied}"
    );
    let status = client.call("sophia_agent_status", json!({})).await;
    assert_eq!(status["result"]["structuredContent"]["current"], false);
    graph.lock().unwrap().unreadable = false;
    let back = client.call("read_document", json!({})).await;
    assert_eq!(
        back["result"]["structuredContent"]["forwarded"], true,
        "{back}"
    );

    // Clear: the full catalogue again.
    client.take_list_changed();
    let cleared = client.call("sophia_agent_clear", json!({})).await;
    assert_eq!(cleared["result"]["structuredContent"]["cleared"], true);
    client.line(Duration::from_secs(5)).await.unwrap();
    assert_eq!(client.take_list_changed(), 1);
    let again = client.tools().await;
    assert!(has(&again, "write_document") && has(&again, "search_documents"));
    assert!(!has(&again, "sophia_mode_set"));
    // No stray notification once undeclared (the poller goes quiet).
    assert!(client.line(Duration::from_millis(600)).await.is_none());
    client.child.kill().await.unwrap();
}

async fn send(stdin: &mut tokio::process::ChildStdin, id: i32, method: &str, params: Value) {
    let mut line = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string();
    line.push('\n');
    stdin.write_all(line.as_bytes()).await.unwrap();
    stdin.flush().await.unwrap();
}

async fn receive(reader: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>) -> Value {
    let line = tokio::time::timeout(Duration::from_secs(15), reader.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_exits_cleanly_while_client_keeps_stdin_open() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(json!({"method":"initialize"})))
        .respond_with(rpc(json!({"capabilities":{"tools":{}}})))
        .mount(&server)
        .await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
        .args(["--backend", &format!("{}/mcp", server.uri())])
        .env("SOPHIA_MCP_NO_UPDATE_CHECK", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();
    send(&mut stdin, 1, "initialize", json!({})).await;
    assert!(receive(&mut stdout).await["result"].is_object());
    let pid = child.id().unwrap().to_string();
    assert!(Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .await
        .unwrap()
        .success());
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("SIGTERM must cancel an open stdin read")
        .unwrap();
    assert_eq!(status.code(), Some(0));
}
