//! `RemoteHttp` — a reqwest MCP client over the "streamable-http-json" transport
//! that gardend's loopback `/mcp` and the platform-next gateway `/g/{id}/mcp`
//! both speak: a single JSON-RPC 2.0 request POSTed, a single JSON response.
//!
//! This is the proxy core. It is used directly for `--backend <url>` AND reused
//! by `LocalGarden` once the headless garden's loopback is listening — the only
//! difference between the two backends is who starts the server.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, ORIGIN};
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

pub struct RemoteHttp {
    client: reqwest::Client,
    mcp_url: String,
}

impl RemoteHttp {
    /// Build a client for `mcp_url` carrying `auth` on every request.
    pub fn new(mcp_url: impl Into<String>, auth: AuthHeaders) -> anyhow::Result<Self> {
        let headers = auth.into_header_map()?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            client,
            mcp_url: mcp_url.into(),
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

    /// POST a single JSON-RPC request and return its `result`, surfacing
    /// JSON-RPC errors as `anyhow::Error`.
    async fn rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(uuid::Uuid::new_v4().to_string())),
            method: method.to_string(),
            params,
        };
        let http = self
            .client
            .post(&self.mcp_url)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("POST {} ({method})", self.mcp_url))?;

        let status = http.status();
        let body = http
            .text()
            .await
            .with_context(|| format!("read response body from {}", self.mcp_url))?;

        if !status.is_success() {
            return Err(anyhow!(
                "backend returned HTTP {status} for {method}: {}",
                body.trim()
            ));
        }

        let resp: JsonRpcResponse = serde_json::from_str(&body)
            .with_context(|| format!("parse JSON-RPC response for {method}: {body}"))?;

        if let Some(err) = resp.error {
            return Err(anyhow!(
                "backend JSON-RPC error {} for {method}: {}",
                err.code,
                err.message
            ));
        }
        Ok(resp.result.unwrap_or(Value::Null))
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

/// Percent-encode a single path segment (graph id) conservatively. Avoids a url
/// crate dependency for what is always a short, simple id.
fn urlencode_segment(seg: &str) -> String {
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
}
