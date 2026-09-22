use serde_json::json;
use sophia_mcp::backend::{AuthHeaders, Backend, ScopedMcp};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use wiremock::{
    matchers::{header, method, path},
    Mock, MockServer, ResponseTemplate,
};

fn auth() -> AuthHeaders {
    AuthHeaders {
        bearer: Some("sph_ak1_synthetic".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn scoped_key_uses_only_the_bound_endpoint_and_retries_pre_dispatch_activation() {
    let server = MockServer::start().await;
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    Mock::given(method("POST"))
        .and(path("/o/user%3Abob/g/notes/mcp"))
        .and(header("authorization", "Bearer sph_ak1_synthetic"))
        .respond_with(move |request: &wiremock::Request| {
            let request: serde_json::Value = request.body_json().unwrap();
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503)
                    .set_body_json(json!({"code":"graph_activating","retryable":true}))
            } else {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"jsonrpc":"2.0","id":request["id"],"result":{"ok":true}}))
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    let backend = ScopedMcp::new(
        &server.uri(),
        Some("user:bob"),
        Some("notes"),
        auth(),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(backend.initialize(json!({})).await.unwrap()["ok"], true);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn tool_writes_are_not_retried_on_ambiguous_failure_or_revocation() {
    for status in [401, 403, 502, 503] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(json!({"error":"unavailable"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let backend = ScopedMcp::new(
            &format!("{}/o/user:bob/g/notes/mcp", server.uri()),
            None,
            None,
            auth(),
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(backend
            .call_tool(json!({"name":"create_document","arguments":{"graph_id":"other"}}))
            .await
            .is_err());
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/o/user:bob/g/notes/mcp");
    }
}

#[test]
fn scoped_urls_refuse_ambient_control_authority_and_unsafe_destinations() {
    for url in [
        "https://example.com/control/mcp",
        "https://example.com/mcp",
        "https://user:secret@example.com/o/user:bob/g/notes/mcp",
        "http://example.com/o/user:bob/g/notes/mcp",
        "https://example.com/o/user:bob/g/notes/mcp?token=bad",
    ] {
        assert!(ScopedMcp::new(url, None, None, auth(), Duration::from_secs(1)).is_err());
    }
    let mut delegated = auth();
    delegated.on_behalf_of = Some("someone-else".into());
    assert!(ScopedMcp::new(
        "https://example.com",
        Some("user:bob"),
        Some("notes"),
        delegated,
        Duration::from_secs(1)
    )
    .is_err());
}
