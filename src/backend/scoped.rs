//! API-key connections address one workspace directly. They neither discover
//! account/control tools nor turn a tool argument into a different endpoint.
use super::{
    remote::{build_client, post_rpc_with_version, rpc_result, urlencode_segment},
    AuthHeaders, Backend,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

pub struct ScopedMcp {
    client: reqwest::Client,
    url: String,
    timeout: Duration,
}

impl ScopedMcp {
    pub fn new(
        base: &str,
        owner: Option<&str>,
        graph: Option<&str>,
        auth: AuthHeaders,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        if auth.on_behalf_of.is_some() || auth.user_id.is_some() {
            bail!("API keys identify their owner; omit on-behalf-of and user-id");
        }
        let mut url = reqwest::Url::parse(base).context("invalid MCP URL")?;
        if !matches!(url.scheme(), "https" | "http")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("MCP URL must be HTTP(S), without credentials, query, or fragment");
        }
        if url.scheme() == "http"
            && !matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
        {
            bail!("remote API-key connections require HTTPS");
        }
        match (owner, graph) {
            (Some(owner), Some(graph)) if url.path().trim_matches('/').is_empty()
                && owner.starts_with("user:") && !owner.contains('/') && !graph.is_empty() && !graph.contains('/') => {
                url.set_path(&format!("/o/{}/g/{}/mcp", urlencode_segment(owner), urlencode_segment(graph)));
            }
            (None, None) => {
                let parts: Vec<_> = url.path().split('/').collect();
                if parts.len() != 6 || parts[1] != "o" || parts[2].is_empty()
                    || parts[3] != "g" || parts[4].is_empty() || parts[5] != "mcp" {
                    bail!("API keys require an owner-qualified /o/<owner>/g/<graph>/mcp URL, or --owner and --graph with the gateway base URL");
                }
            }
            _ => bail!("supply either a workspace MCP URL or the gateway base with both --owner and --graph"),
        }
        Ok(Self {
            client: build_client(auth)?,
            url: url.into(),
            timeout,
        })
    }

    async fn rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            let reply = tokio::time::timeout_at(
                deadline,
                post_rpc_with_version(
                    &self.client,
                    &self.url,
                    method,
                    params.clone(),
                    Some("2025-03-26"),
                ),
            )
            .await
            .context("workspace MCP request/activation deadline exceeded")??;
            // Only this explicit pre-dispatch response is safe to retry. A
            // timeout, generic 502/503, or tool error may follow a real write.
            if reply.status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && reply.json()["code"] == "graph_activating"
                && reply.json()["retryable"] == true
            {
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_secs(2)))
                    .await
                    .context("workspace activation deadline exceeded")?;
                continue;
            }
            return rpc_result(reply, method);
        }
    }
}

#[async_trait]
impl Backend for ScopedMcp {
    async fn initialize(&self, params: Value) -> anyhow::Result<Value> {
        self.rpc("initialize", params).await
    }
    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        self.rpc("tools/list", params).await
    }
    async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
        self.rpc("tools/call", params).await
    }
}
