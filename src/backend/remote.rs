//! `RemoteHttp` — a reqwest MCP client over the "streamable-http-json" transport
//! that gardend's loopback `/mcp` and the platform-next gateway's cell path
//! both speak: a single JSON-RPC 2.0 request POSTed, a single JSON response.
//!
//! This is the proxy core for a *single* upstream endpoint. It is used directly
//! for `--backend <url>/mcp` (an explicit Garden loopback) AND reused by
//! `LocalGarden` once the headless garden's loopback is listening — the only
//! difference between the two is who starts the server.
//!
//! The multi-graph gateway backend (`super::gateway::GatewayBackend`) shares the
//! HTTP helpers below (`build_client`, `post_rpc`, `rpc_result`) but owns its own
//! routing + activation logic.

use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, ORIGIN};
use reqwest::StatusCode;
use serde_json::{json, Value};

use crate::mcp::{method, JsonRpcRequest, JsonRpcResponse};

use super::Backend;

/// Auth + identity headers applied to every proxied request.
///
/// Two shapes, matching the reference proxy (`choreograph/src/mnemosyne.ts`):
///   * LOCAL: `Authorization: Bearer <loopback token>` + an allowed `Origin`
///     (gardend's `origin_ok` requires loopback/null Origin — see
///     `garden src-tauri/src/loopback_http.rs`).
///   * REMOTE gateway service-auth: `Authorization: Bearer <serviceToken>` +
///     `x-pn-on-behalf-of: <sub>`.
///   * REMOTE direct user: `Authorization: Bearer <jwt>` (+ optional
///     `X-User-ID`).
#[derive(Debug, Clone, Default)]
pub struct AuthHeaders {
    pub bearer: Option<String>,
    pub on_behalf_of: Option<String>,
    pub user_id: Option<String>,
    /// Origin header value (set for the local loopback to satisfy `origin_ok`).
    pub origin: Option<String>,
}

impl AuthHeaders {
    fn into_header_map(self) -> anyhow::Result<HeaderMap> {
        let mut map = HeaderMap::new();
        if let Some(bearer) = self.bearer {
            let value = HeaderValue::from_str(&format!("Bearer {bearer}"))
                .context("invalid bearer token (not a valid HTTP header value)")?;
            map.insert(AUTHORIZATION, value);
        }
        if let Some(origin) = self.origin {
            map.insert(
                ORIGIN,
                HeaderValue::from_str(&origin).context("invalid Origin header value")?,
            );
        }
        // Gateway service-auth: identity is the on-behalf-of header. Per the
        // reference proxy, when on-behalf-of is set we do NOT also send
        // X-User-ID.
        if let Some(sub) = self.on_behalf_of {
            let name = HeaderName::from_static("x-pn-on-behalf-of");
            map.insert(
                name,
                HeaderValue::from_str(&sub).context("invalid x-pn-on-behalf-of value")?,
            );
        } else if let Some(uid) = self.user_id {
            let name = HeaderName::from_static("x-user-id");
            map.insert(
                name,
                HeaderValue::from_str(&uid).context("invalid X-User-ID value")?,
            );
        }
        Ok(map)
    }
}

/// Is `url` `http://` (not `https://`) with a host that is NOT loopback
/// (127.0.0.0/8, `::1`, or `localhost`)? Used to gate sending a bearer token
/// in the clear — see `build_client`. Hand-rolled rather than pulling in the
/// `url` crate (see `urlencode_segment`'s rationale): this only ever needs to
/// answer one narrow question about a URL sophia-mcp itself constructed or
/// was configured with, not parse an arbitrary URL in general.
fn is_insecure_plaintext(url: &str) -> bool {
    let Some(after_scheme) = url.strip_prefix("http://") else {
        return false; // https://, or an unrecognized scheme: not this check's concern.
    };
    // Drop a "user:pass@" prefix, if any, before finding the host.
    let after_userinfo = after_scheme
        .rsplit_once('@')
        .map_or(after_scheme, |(_, h)| h);
    let host = if let Some(rest) = after_userinfo.strip_prefix('[') {
        // IPv6 literal: "[::1]:port/path" — host is up to the closing ']'.
        rest.split(']').next().unwrap_or(rest)
    } else {
        after_userinfo
            .split(['/', ':', '?', '#'])
            .next()
            .unwrap_or("")
    };
    let host = host.to_ascii_lowercase();
    !(host == "localhost" || host == "::1" || host == "127.0.0.1" || host.starts_with("127."))
}

/// Build a reqwest client that carries `auth` on every request to `target_url`.
///
/// Refuses to build a client that would send a bearer token over plain
/// `http://` to a non-loopback host, unless `allow_insecure_http` is set —
/// that combination ships a real credential in the clear to whatever's on
/// the network path. Loopback (127.0.0.0/8, `::1`, `localhost`) is always
/// allowed regardless of the flag: it's the LOCAL backend's whole mechanism
/// (a self-minted token to sophia-mcp's own spawned `gardend`), and traffic
/// that never leaves the machine isn't the threat this guards against.
pub(crate) fn build_client(
    auth: AuthHeaders,
    target_url: &str,
    request_timeout: Duration,
    allow_insecure_http: bool,
) -> anyhow::Result<reqwest::Client> {
    if auth.bearer.is_some() && !allow_insecure_http && is_insecure_plaintext(target_url) {
        return Err(anyhow!(
            "refusing to send a bearer token over plain http:// to a non-loopback host ({target_url}); \
             pass --allow-insecure-http (or SOPHIA_MCP_ALLOW_INSECURE_HTTP=1) to override, or use https://"
        ));
    }
    let headers = auth.into_header_map()?;
    reqwest::Client::builder()
        .default_headers(headers)
        // A TCP/TLS connect that hangs is never a legitimate wait.
        .connect_timeout(std::time::Duration::from_secs(10))
        // The whole request (connect + send + full response read) — bounds a
        // slow-drip or stalled upstream that a connect timeout alone would
        // never catch.
        .timeout(request_timeout)
        // Never follow redirects: reqwest strips Authorization cross-host but
        // not x-pn-on-behalf-of, and a redirected body must never be trusted
        // as a gateway answer. A 3xx surfaces to the caller as-is.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build reqwest client")
}

/// Cap on any single HTTP response body sophia-mcp reads from a backend.
/// Read incrementally (see [`read_bounded_text`]) rather than after fully
/// buffering, so an oversized or slow-drip response can't grow unbounded
/// memory before sophia-mcp notices and aborts the read.
pub(crate) const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

/// Read an HTTP response body as UTF-8 text (lossy), bounded to `max_bytes`
/// (production call sites always pass [`MAX_RESPONSE_BYTES`]; parameterized
/// so the abort behavior is directly unit-testable without a multi-megabyte
/// fixture). Reads chunk-by-chunk (`Response::chunk`, part of reqwest's base
/// API — no extra crate feature needed) rather than buffering the whole body
/// first, so the cap is enforced DURING the read, not after.
pub(crate) async fn read_bounded_text(
    mut resp: reqwest::Response,
    what: &str,
    max_bytes: usize,
) -> anyhow::Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .with_context(|| format!("read response body from {what}"))?
    {
        buf.extend_from_slice(&chunk);
        if buf.len() > max_bytes {
            return Err(anyhow!(
                "response body from {what} exceeded {max_bytes} bytes; refusing to buffer further"
            ));
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Cap on any single upstream body/text snippet embedded into a log line or
/// error message. Smaller than, and independent of, the FINAL client-facing
/// message cap (`server::redact_error_message`): several already-bounded
/// snippets can still combine across an `anyhow` `.context()` chain, so that
/// choke point is the real backstop. This is the per-site hygiene that keeps
/// any ONE upstream body — untrusted content; a misbehaving or compromised
/// endpoint could return megabytes, or text engineered to look like
/// something else — from dominating a message or an unbounded stderr log
/// line in the first place.
const REDACT_BODY_MAX_BYTES: usize = 500;

/// Bound and redact an upstream HTTP response body before it is embedded in
/// any log line or error message: trims whitespace, then truncates to
/// [`REDACT_BODY_MAX_BYTES`] bytes (at a UTF-8 char boundary) with a
/// trailing note giving the true length. An empty body renders as an
/// explicit marker rather than an empty string that could read as "no
/// detail" / "no error". Never used on a legitimate JSON-RPC `result` or
/// `error.message` — only on raw, non-JSON-RPC-shaped body text (a non-2xx
/// HTTP body, or something that failed to parse).
pub(crate) fn redact_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "<empty body>".to_string();
    }
    if trimmed.len() <= REDACT_BODY_MAX_BYTES {
        return trimmed.to_string();
    }
    let mut end = REDACT_BODY_MAX_BYTES;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}… [truncated, {} bytes total]",
        &trimmed[..end],
        trimmed.len()
    )
}

/// A raw HTTP reply: status + body text. Callers classify it (the gateway
/// answers 202/502/503 with JSON bodies that are NOT JSON-RPC envelopes).
#[derive(Debug, Clone)]
pub(crate) struct HttpReply {
    pub status: StatusCode,
    pub body: String,
}

impl HttpReply {
    /// Best-effort JSON parse of the body (gateway error bodies are JSON).
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
}

/// POST one JSON-RPC request to `url` and return the raw reply.
pub(crate) async fn post_rpc(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: Value,
) -> anyhow::Result<HttpReply> {
    let req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(uuid::Uuid::new_v4().to_string())),
        method: method.to_string(),
        params,
    };
    let http = client
        .post(url)
        .json(&req)
        .send()
        .await
        .with_context(|| format!("POST {url} ({method})"))?;
    let status = http.status();
    let body = read_bounded_text(http, url, MAX_RESPONSE_BYTES).await?;
    Ok(HttpReply { status, body })
}

/// Turn a raw reply into the JSON-RPC `result`, surfacing non-2xx statuses
/// and JSON-RPC errors as `anyhow::Error`. Any raw upstream body text is
/// redacted ([`redact_body`]) before it's embedded — `err.message` is left
/// untouched: it's the upstream's OWN structured JSON-RPC error field, not a
/// raw body dump.
pub(crate) fn rpc_result(reply: HttpReply, method: &str) -> anyhow::Result<Value> {
    if !reply.status.is_success() {
        return Err(anyhow!(
            "backend returned HTTP {} for {method}: {}",
            reply.status,
            redact_body(&reply.body)
        ));
    }
    let resp: JsonRpcResponse = serde_json::from_str(&reply.body).with_context(|| {
        format!(
            "parse JSON-RPC response for {method}: {}",
            redact_body(&reply.body)
        )
    })?;
    if let Some(err) = resp.error {
        return Err(anyhow!(
            "backend JSON-RPC error {} for {method}: {}",
            err.code,
            err.message
        ));
    }
    Ok(resp.result.unwrap_or(Value::Null))
}

pub struct RemoteHttp {
    client: reqwest::Client,
    mcp_url: String,
}

impl RemoteHttp {
    /// Build a client for `mcp_url` carrying `auth` on every request, bounded
    /// by `request_timeout` (see [`build_client`]). Refuses to send a bearer
    /// over plain `http://` to a non-loopback host unless
    /// `allow_insecure_http` is set.
    pub fn new(
        mcp_url: impl Into<String>,
        auth: AuthHeaders,
        request_timeout: Duration,
        allow_insecure_http: bool,
    ) -> anyhow::Result<Self> {
        let mcp_url = mcp_url.into();
        Ok(Self {
            client: build_client(auth, &mcp_url, request_timeout, allow_insecure_http)?,
            mcp_url,
        })
    }

    /// Resolve a `--backend <url>` value into a concrete MCP endpoint.
    ///
    /// Rules:
    ///   * if the URL already ends in `/mcp`, use it as-is;
    ///   * else if a `graph` is given, build `<base>/g/<graph>/mcp` (the gateway
    ///     contract — see `choreograph/src/mnemosyne.ts:332`);
    ///   * else append `/mcp`.
    pub fn resolve_mcp_url(raw: &str, graph: Option<&str>) -> String {
        let base = raw.trim_end_matches('/');
        if base.ends_with("/mcp") {
            return base.to_string();
        }
        match graph {
            Some(g) if !g.is_empty() => {
                let g = urlencode_segment(g);
                format!("{base}/g/{g}/mcp")
            }
            _ => format!("{base}/mcp"),
        }
    }

    pub fn mcp_url(&self) -> &str {
        &self.mcp_url
    }

    async fn rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let reply = post_rpc(&self.client, &self.mcp_url, method, params).await?;
        rpc_result(reply, method)
    }
}

#[async_trait]
impl Backend for RemoteHttp {
    async fn initialize(&self, client_params: Value) -> anyhow::Result<Value> {
        self.rpc(method::INITIALIZE, client_params).await
    }

    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        self.rpc(method::TOOLS_LIST, params).await
    }

    async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
        self.rpc(method::TOOLS_CALL, params).await
    }
}

/// Percent-encode a single path segment (graph id / owner) conservatively.
/// Avoids a url crate dependency for what is always a short, simple id.
pub(crate) fn urlencode_segment(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for b in seg.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_keeps_full_mcp_url() {
        assert_eq!(
            RemoteHttp::resolve_mcp_url("https://gw.example/g/abc/mcp", Some("xyz")),
            "https://gw.example/g/abc/mcp"
        );
        assert_eq!(
            RemoteHttp::resolve_mcp_url("http://127.0.0.1:8086/mcp/", None),
            "http://127.0.0.1:8086/mcp"
        );
    }

    #[test]
    fn resolve_builds_gateway_path_from_base_and_graph() {
        assert_eq!(
            RemoteHttp::resolve_mcp_url("https://gw.example", Some("my-graph")),
            "https://gw.example/g/my-graph/mcp"
        );
        assert_eq!(
            RemoteHttp::resolve_mcp_url("https://gw.example/", Some("a b")),
            "https://gw.example/g/a%20b/mcp"
        );
    }

    #[test]
    fn resolve_appends_mcp_without_graph() {
        assert_eq!(
            RemoteHttp::resolve_mcp_url("http://127.0.0.1:9999", None),
            "http://127.0.0.1:9999/mcp"
        );
    }

    #[test]
    fn auth_headers_service_mode_omits_user_id() {
        let map = AuthHeaders {
            bearer: Some("svc-token".into()),
            on_behalf_of: Some("user-sub-123".into()),
            user_id: Some("should-be-dropped".into()),
            origin: None,
        }
        .into_header_map()
        .unwrap();
        assert_eq!(map.get(AUTHORIZATION).unwrap(), "Bearer svc-token");
        assert_eq!(map.get("x-pn-on-behalf-of").unwrap(), "user-sub-123");
        assert!(map.get("x-user-id").is_none());
    }

    #[test]
    fn auth_headers_direct_user_mode_sets_user_id() {
        let map = AuthHeaders {
            bearer: Some("jwt".into()),
            on_behalf_of: None,
            user_id: Some("u-1".into()),
            origin: Some("http://127.0.0.1".into()),
        }
        .into_header_map()
        .unwrap();
        assert_eq!(map.get("x-user-id").unwrap(), "u-1");
        assert_eq!(map.get(ORIGIN).unwrap(), "http://127.0.0.1");
        assert!(map.get("x-pn-on-behalf-of").is_none());
    }

    // -------------------------------------------------------- redact_body

    #[test]
    fn redact_body_passes_short_bodies_through_trimmed() {
        assert_eq!(redact_body("  graph not found  "), "graph not found");
    }

    #[test]
    fn redact_body_marks_an_empty_body_explicitly() {
        assert_eq!(redact_body(""), "<empty body>");
        assert_eq!(redact_body("   "), "<empty body>");
    }

    #[test]
    fn redact_body_truncates_a_huge_body_and_never_leaks_a_secret_past_the_cap() {
        let secret = "SECRET_TOKEN_MUST_NOT_LEAK";
        // The secret sits well past REDACT_BODY_MAX_BYTES (500), simulating a
        // huge/secret-bearing upstream body (e.g. a stack trace that happens
        // to echo back a header or an internal value near the end).
        let huge = format!("{}{}", "x".repeat(2000), secret);
        let redacted = redact_body(&huge);
        assert!(
            redacted.len() < huge.len(),
            "must be shorter than the input"
        );
        assert!(
            !redacted.contains(secret),
            "the secret past the cap must not survive redaction: {redacted}"
        );
        assert!(
            redacted.contains("truncated") && redacted.contains("2026 bytes total"),
            "must carry a bounded, explicit truncation marker: {redacted}"
        );
    }

    // ------------------------------------------------- is_insecure_plaintext

    #[test]
    fn plain_http_to_a_non_loopback_host_is_insecure() {
        assert!(is_insecure_plaintext("http://example.com/mcp"));
        assert!(is_insecure_plaintext("http://gateway.internal:8080/mcp"));
        assert!(is_insecure_plaintext("http://192.168.1.5:8086/mcp"));
    }

    #[test]
    fn loopback_over_plain_http_is_never_flagged() {
        for url in [
            "http://127.0.0.1:8086/mcp",
            "http://127.5.6.7:8086/mcp",
            "http://localhost:8086/mcp",
            "http://LOCALHOST:8086/mcp",
            "http://[::1]:8086/mcp",
        ] {
            assert!(
                !is_insecure_plaintext(url),
                "{url} must be treated as loopback"
            );
        }
    }

    #[test]
    fn https_is_never_flagged_regardless_of_host() {
        assert!(!is_insecure_plaintext("https://example.com/mcp"));
    }

    // ----------------------------------------------- build_client refusal

    #[test]
    fn build_client_refuses_a_bearer_over_plain_http_to_a_non_loopback_host() {
        let auth = AuthHeaders {
            bearer: Some("s3cr3t".into()),
            ..Default::default()
        };
        let err = build_client(
            auth,
            "http://example.com/mcp",
            Duration::from_secs(30),
            false,
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("--allow-insecure-http"), "got: {msg}");
        assert!(
            !msg.contains("s3cr3t"),
            "the token itself must never appear in the error"
        );
    }

    #[test]
    fn build_client_allows_it_with_allow_insecure_http() {
        let auth = AuthHeaders {
            bearer: Some("s3cr3t".into()),
            ..Default::default()
        };
        build_client(
            auth,
            "http://example.com/mcp",
            Duration::from_secs(30),
            true,
        )
        .expect("allow_insecure_http must let it through");
    }

    #[test]
    fn build_client_allows_a_bearer_over_plain_http_to_loopback_without_the_flag() {
        let auth = AuthHeaders {
            bearer: Some("s3cr3t".into()),
            ..Default::default()
        };
        build_client(
            auth,
            "http://127.0.0.1:8086/mcp",
            Duration::from_secs(30),
            false,
        )
        .expect("loopback must never require --allow-insecure-http");
    }

    #[test]
    fn build_client_allows_a_bearer_over_https_without_the_flag() {
        let auth = AuthHeaders {
            bearer: Some("s3cr3t".into()),
            ..Default::default()
        };
        build_client(
            auth,
            "https://example.com/mcp",
            Duration::from_secs(30),
            false,
        )
        .expect("https must never require --allow-insecure-http");
    }

    #[test]
    fn build_client_allows_no_bearer_at_all_over_plain_http_non_loopback() {
        // No credential in flight — nothing to protect, so no refusal even
        // without the flag.
        build_client(
            AuthHeaders::default(),
            "http://example.com/mcp",
            Duration::from_secs(30),
            false,
        )
        .expect("no bearer means nothing to refuse");
    }

    // ------------------------------------------------- read_bounded_text

    #[tokio::test]
    async fn read_bounded_text_passes_a_response_under_the_cap() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("hello"))
            .mount(&server)
            .await;
        let resp = reqwest::get(server.uri()).await.unwrap();
        let text = read_bounded_text(resp, "test", 100).await.unwrap();
        assert_eq!(text, "hello");
    }

    #[tokio::test]
    async fn read_bounded_text_aborts_a_response_over_the_cap_instead_of_buffering_it_whole() {
        let server = wiremock::MockServer::start().await;
        let huge = "x".repeat(10_000);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(huge))
            .mount(&server)
            .await;
        let resp = reqwest::get(server.uri()).await.unwrap();
        // A cap far smaller than the real 10 MiB production default, so this
        // test stays fast — the abort mechanism itself doesn't care about
        // the absolute number, only that it's enforced during the read.
        let err = read_bounded_text(resp, "test", 100).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("exceeded 100 bytes"), "got: {msg}");
    }
}
