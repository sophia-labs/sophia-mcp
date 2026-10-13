//! Extra request headers (`--header`, `SOPHIA_MCP_HEADERS`, the Cloudflare
//! Access service-token pair) against real HTTP listeners each test starts on
//! 127.0.0.1: the headers reach the configured backend on every request and
//! nowhere else (not a `--sub`, not a redirect target), their values never
//! reach stderr or an error, and a Cloudflare Access refusal comes back as an
//! error that names Access while gardend's own refusal keeps its own message.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use sophia_mcp::backend::{AuthHeaders, Backend, GatewayBackend, GatewayOptions, RemoteHttp};
use sophia_mcp::config::resolve_extra_headers;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader};

const KEY: &str = "probe-sophia-key-0123456789";
const SECRET: &str = "probe-access-secret-0123456789";

/// One request as the listener saw it; header names lowercased.
#[derive(Debug, Clone)]
struct Seen {
    path: String,
    headers: Vec<(String, String)>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: String,
}

type Respond = dyn Fn(&Seen, &Value) -> Reply + Send + Sync;

/// A real HTTP/1.1 listener on 127.0.0.1 that records every request and
/// answers it with `respond`, one request per connection.
struct Listener {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Listener {
    fn start(respond: impl Fn(&Seen, &Value) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let respond: Arc<Respond> = Arc::new(respond);
        let record = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (record, respond) = (Arc::clone(&record), Arc::clone(&respond));
                std::thread::spawn(move || serve(stream, &record, respond.as_ref()));
            }
        });
        Self { base, seen }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn serve(mut stream: TcpStream, record: &Mutex<Vec<Seen>>, respond: &Respond) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .to_string();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let (name, value) = (name.trim().to_ascii_lowercase(), value.trim().to_string());
            if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }
    let mut body = vec![0u8; content_length];
    let _ = reader.read_exact(&mut body);
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let seen = Seen { path, headers };
    let reply = respond(&seen, &request);
    record.lock().unwrap().push(seen);
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(reply.body.as_bytes());
}

/// A JSON-RPC answer the way gardend gives one: the request's id, a result.
fn rpc(request: &Value, result: Value) -> Reply {
    Reply {
        status: "200 OK",
        headers: vec![("Content-Type", "application/json".into())],
        body: json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }).to_string(),
    }
}

/// A minimal MCP server: `initialize`, and a one-tool catalogue.
fn mcp(tool: &'static str) -> impl Fn(&Seen, &Value) -> Reply + Send + Sync + 'static {
    move |_, request| match request["method"].as_str() {
        Some("initialize") => rpc(
            request,
            json!({
                "protocolVersion": "2025-03-26",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "listener", "version": "0" }
            }),
        ),
        Some("tools/list") => rpc(
            request,
            json!({ "tools": [
                { "name": tool, "description": "probe", "inputSchema": { "type": "object" } }
            ] }),
        ),
        _ => rpc(
            request,
            json!({ "content": [ { "type": "text", "text": "ok" } ] }),
        ),
    }
}

/// A listener that gives every request the same answer.
fn fixed(status: &'static str, headers: &[(&'static str, &str)], body: &str) -> Listener {
    let headers: Vec<(&'static str, String)> =
        headers.iter().map(|&(n, v)| (n, v.to_string())).collect();
    let body = body.to_string();
    Listener::start(move |_, _| Reply {
        status,
        headers: headers.clone(),
        body: body.clone(),
    })
}

/// What Cloudflare's edge puts on a page it serves itself.
const EDGE_HTML: [(&str, &str); 3] = [
    ("Content-Type", "text/html; charset=UTF-8"),
    ("CF-RAY", "8c1f2a3b4d5e6f70-SJC"),
    ("Server", "cloudflare"),
];

fn access_auth() -> AuthHeaders {
    AuthHeaders {
        bearer: Some(KEY.into()),
        extra: resolve_extra_headers(None, &[], Some("probe-id.access"), Some(SECRET)).unwrap(),
        ..Default::default()
    }
}

/// `initialize` through a direct remote backend at `listener`, which must
/// fail; returns the error, after checking no credential reached it.
async fn refusal(listener: &Listener) -> String {
    let backend = RemoteHttp::new(
        format!("{}/mcp", listener.base),
        access_auth(),
        Duration::from_secs(10),
        false,
    )
    .unwrap();
    let err = backend.initialize(json!({})).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        !msg.contains(SECRET) && !msg.contains(KEY),
        "a credential reached the error: {msg}"
    );
    msg
}

#[tokio::test]
async fn extra_headers_reach_the_backend_on_every_request_and_never_a_sub_or_stderr() {
    let backend = Listener::start(mcp("probe_tool"));
    let sub = Listener::start(mcp("echo"));
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
        .arg("--backend")
        .arg(format!("{}/mcp", backend.base))
        .arg("--header")
        .arg("X-Flag: from-flag")
        .arg("--sub")
        .arg(format!("side={}/mcp", sub.base))
        // Hermetic: nothing from the developer's shell (SOPHIA_MCP_GRAPH, …).
        .env_clear()
        .env("SOPHIA_MCP_TOKEN", KEY)
        .env("SOPHIA_MCP_HEADERS", "X-Env-A: a\nX-Env-B: b; X-Env-C: c")
        .env("CF_ACCESS_CLIENT_ID", "probe-id.access")
        .env("CF_ACCESS_CLIENT_SECRET", SECRET)
        .env("SOPHIA_MCP_NO_UPDATE_CHECK", "1")
        .env("SOPHIA_MCP_LOG", "debug")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stderr_log = tokio::spawn(async move {
        let mut log = String::new();
        let _ = stderr.read_to_string(&mut log).await;
        log
    });
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = TokioBufReader::new(child.stdout.take().unwrap()).lines();

    let mut answers = Vec::new();
    for (id, method) in [(1, "initialize"), (2, "tools/list")] {
        let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": {} })
            .to_string()
            + "\n";
        stdin.write_all(line.as_bytes()).await.unwrap();
        stdin.flush().await.unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(20), stdout.next_line())
            .await
            .expect("no answer on stdout")
            .unwrap()
            .expect("stdout closed");
        answers.push(serde_json::from_str::<Value>(&answer).unwrap());
    }
    drop(stdin);
    if tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .is_err()
    {
        child.start_kill().unwrap();
        child.wait().await.unwrap();
    }
    let log = tokio::time::timeout(Duration::from_secs(10), stderr_log)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(answers[0]["id"], json!(1), "{answers:?}");
    let tools: Vec<&str> = answers[1]["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools: {answers:?}"))
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert!(
        tools.contains(&"probe_tool") && tools.contains(&"side_echo"),
        "{tools:?}"
    );

    let to_backend = backend.seen();
    assert!(to_backend.len() >= 2, "{to_backend:?}");
    let bearer = format!("Bearer {KEY}");
    for request in &to_backend {
        assert_eq!(request.path, "/mcp");
        assert_eq!(request.header("authorization"), Some(bearer.as_str()));
        assert_eq!(request.header("x-flag"), Some("from-flag"));
        assert_eq!(request.header("x-env-a"), Some("a"));
        assert_eq!(request.header("x-env-b"), Some("b"));
        assert_eq!(request.header("x-env-c"), Some("c"));
        assert_eq!(
            request.header("cf-access-client-id"),
            Some("probe-id.access")
        );
        assert_eq!(request.header("cf-access-client-secret"), Some(SECRET));
    }

    let to_sub = sub.seen();
    assert!(!to_sub.is_empty(), "the sub was never probed");
    for request in &to_sub {
        for name in [
            "authorization",
            "x-flag",
            "x-env-a",
            "x-env-b",
            "x-env-c",
            "cf-access-client-id",
            "cf-access-client-secret",
        ] {
            assert!(
                request.header(name).is_none(),
                "{name} reached the sub: {request:?}"
            );
        }
    }

    assert!(
        log.contains("cf-access-client-secret"),
        "the header names are logged: {log}"
    );
    assert!(
        !log.contains(SECRET) && !log.contains(KEY),
        "a credential reached stderr: {log}"
    );
}

#[tokio::test]
async fn an_access_login_redirect_is_named_and_never_followed() {
    let login = Listener::start(mcp("never"));
    let location = format!(
        "{}/cdn-cgi/access/login/sophia.example.com?kid=probe&redirect_url=%2Fmcp",
        login.base
    );
    let edge = fixed("302 Found", &[("Location", location.as_str())], "");
    let msg = refusal(&edge).await;
    assert!(
        msg.contains("Cloudflare Access redirected") && msg.contains("--cf-access-client-id"),
        "{msg}"
    );
    assert!(!msg.contains("kid=probe"), "{msg}");
    assert!(
        login.seen().is_empty(),
        "the redirect was followed: {:?}",
        login.seen()
    );
    let asked = edge.seen();
    assert_eq!(asked.len(), 1, "{asked:?}");
    assert_eq!(asked[0].header("cf-access-client-secret"), Some(SECRET));
}

#[tokio::test]
async fn a_403_from_the_cloudflare_edge_names_access() {
    let edge = fixed(
        "403 Forbidden",
        &EDGE_HTML,
        "<!DOCTYPE html><html><head><title>Forbidden</title></head><body>Forbidden</body></html>",
    );
    let msg = refusal(&edge).await;
    assert!(
        msg.contains("Cloudflare Access refused the request (HTTP 403"),
        "{msg}"
    );
    assert!(msg.contains("Service Auth"), "{msg}");
}

#[tokio::test]
async fn an_html_login_page_where_json_was_expected_names_access() {
    let edge = fixed(
        "200 OK",
        &[("Content-Type", "text/html")],
        "<html><head><title>Sign in - Cloudflare Access</title></head><body>Get a login code</body></html>",
    );
    let msg = refusal(&edge).await;
    assert!(
        msg.contains("Cloudflare Access answered with its login page"),
        "{msg}"
    );
}

#[tokio::test]
async fn gardends_own_401_through_the_tunnel_is_not_blamed_on_access() {
    let edge = fixed(
        "401 Unauthorized",
        &[
            ("Content-Type", "application/json"),
            ("CF-RAY", "8c1f2a3b4d5e6f70-SJC"),
            ("Server", "cloudflare"),
        ],
        r#"{"ok":false,"error":"missing or invalid bearer token"}"#,
    );
    let msg = refusal(&edge).await;
    assert!(!msg.contains("Cloudflare Access"), "{msg}");
    assert!(
        msg.contains("401") && msg.contains("missing or invalid bearer token"),
        "{msg}"
    );
}

#[tokio::test]
async fn a_gateway_behind_access_gets_the_headers_and_a_named_refusal() {
    let edge = fixed("403 Forbidden", &EDGE_HTML, "<html>Forbidden</html>");
    let connected = GatewayBackend::connect(
        &edge.base,
        "user:owner",
        "notes",
        access_auth(),
        GatewayOptions {
            request_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .await;
    let Err(err) = connected else {
        panic!("connected through an Access refusal");
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("Cloudflare Access refused"), "{msg}");
    assert!(!msg.contains(SECRET) && !msg.contains(KEY), "{msg}");
    let asked = edge.seen();
    assert_eq!(asked[0].path, "/control/mcp", "{asked:?}");
    assert_eq!(
        asked[0].header("cf-access-client-id"),
        Some("probe-id.access")
    );
    assert_eq!(asked[0].header("cf-access-client-secret"), Some(SECRET));
}
