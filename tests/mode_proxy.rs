//! Process-level MCP mode switch over an actual HTTP JSON-RPC proxy hop.
//! This exercises the stdio notification and discovery behavior that a client
//! observes; the backend is a local test server with Garden-shaped replies.

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn rpc(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(json!({"jsonrpc":"2.0","id":"backend","result":result}))
}

fn mode_rows() -> Vec<Value> {
    let a = "http://mnemosyne.dev/agent#";
    let t = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let mut rows = Vec::new();
    for (slug, relation, tool, access) in [
        ("reader", "defaultMode", "read_document", "read"),
        ("writer", "mayUseMode", "write_document", "write"),
    ] {
        let mode = format!("<urn:sophia:mode:{slug}>");
        let rel = format!("<{a}{relation}>");
        rows.push(json!({"rel":rel,"mode":mode,"p":format!("<{t}>"),"o":format!("<{a}Mode>")}));
        rows.push(
            json!({"rel":rel,"mode":mode,"p":format!("<{a}access>"),"o":json!(access).to_string()}),
        );
        rows.push(json!({"rel":rel,"mode":mode,"p":format!("<{a}allowsTool>"),"o":json!(tool).to_string()}));
    }
    rows
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

#[tokio::test]
async fn stdio_client_can_switch_modes_and_refresh_its_tool_catalog() {
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
            {"name":"write_document","_meta":{"sophia.local.requiredScopes":["documents.write"]}}
        ]})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(body_partial_json(
            json!({"method":"tools/call","params":{"name":"sparql_query"}}),
        ))
        .respond_with(rpc(
            json!({"structuredContent":{"resultType":"solutions","rows":mode_rows()}}),
        ))
        .mount(&server)
        .await;

    let mut child = Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
        .args([
            "--backend",
            &format!("{}/mcp", server.uri()),
            "--graph",
            "lab",
            "--agent-id",
            "agent-deadbeef",
        ])
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
    assert_eq!(
        receive(&mut stdout).await["result"]["capabilities"]["tools"]["listChanged"],
        true
    );
    send(&mut stdin, 2, "tools/list", json!({})).await;
    let before = receive(&mut stdout).await;
    let before_names: Vec<_> = before["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(before_names.contains(&"read_document"));
    assert!(!before_names.contains(&"write_document"));

    send(
        &mut stdin,
        3,
        "tools/call",
        json!({"name":"sophia_mode_set","arguments":{"modeIri":"urn:sophia:mode:writer"}}),
    )
    .await;
    assert_eq!(
        receive(&mut stdout).await["result"]["structuredContent"]["changed"],
        true
    );
    assert_eq!(
        receive(&mut stdout).await["method"],
        "notifications/tools/list_changed"
    );
    send(&mut stdin, 4, "tools/list", json!({})).await;
    let after = receive(&mut stdout).await;
    let after_names: Vec<_> = after["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(after_names.contains(&"write_document"));
    assert!(!after_names.contains(&"read_document"));
    child.kill().await.unwrap();
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
