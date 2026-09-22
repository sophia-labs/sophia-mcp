//! `ComposedBackend` — mounts one or more **sub-MCPs** above any other
//! [`Backend`] (local, remote, or gateway) without touching it.
//!
//! A sub-MCP is a second HTTP MCP endpoint (any server speaking the same
//! streamable-http-json wire [`RemoteHttp`] speaks — see `--sub` in
//! `config.rs`) whose tools are namespaced under a stable prefix:
//! `world` on `layout=http://127.0.0.1:5199/mcp` is exposed to the agent as
//! `layout_world`, and a `tools/call` whose name starts with `layout_` is
//! routed to that sub with the bare name, arguments untouched. This is the
//! same shape `gateway.rs` uses for `control_` — a merged catalog, routing by
//! name — generalized to an arbitrary number of independently-owned
//! upstreams instead of exactly the gateway's two. It does not replace or
//! touch `GatewayBackend`'s own `control_` merge; a `ComposedBackend` may
//! itself wrap a `GatewayBackend` as its `primary`.
//!
//! **Resilience.** Each sub is probed with exactly one `tools/list` call when
//! the `ComposedBackend` is built. A sub that fails that probe is logged to
//! stderr (prefix + error — never a bearer token) and skipped for the
//! process's lifetime: the primary's own catalog still serves, and
//! `tools/call` to `<prefix>_<name>` for a skipped sub is [`ToolNotFound`]
//! without ever touching the network. A sub that answered the probe but then
//! fails a later `tools/call` surfaces as a normal `BACKEND_ERROR` with the
//! error chain, exactly like the primary's own upstream failures — no
//! special-casing at this layer.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{json, Value};

use crate::config::SubSpec;

use super::remote::RemoteHttp;
use super::{AuthHeaders, Backend, ToolNotFound};

/// One configured sub-MCP: a namespace prefix plus the client that reaches
/// it, plus its own tool catalog from the one start-of-process probe.
struct SubMcp {
    prefix: String,
    client: RemoteHttp,
    /// The sub's own `tools/list` result (bare names, tool objects verbatim —
    /// including `description`) from the one probe at construction.
    /// `None` means that probe failed: the sub is skipped for this process's
    /// life (no retries — see the module doc's Resilience section).
    catalog: RwLock<Option<Vec<Value>>>,
}

impl SubMcp {
    /// This sub's tools, each renamed `<prefix>_<name>` for the merged
    /// catalog handed to the agent. `None` (a skipped/down sub) contributes
    /// nothing.
    fn exposed_tools(&self) -> Vec<Value> {
        let Some(tools) = self.catalog.read().expect("sub catalog lock").clone() else {
            return Vec::new();
        };
        tools
            .into_iter()
            .map(|mut tool| {
                if let Some(name) = tool.get("name").and_then(Value::as_str) {
                    let exposed = format!("{}_{name}", self.prefix);
                    tool["name"] = json!(exposed);
                }
                tool
            })
            .collect()
    }

    fn is_up(&self) -> bool {
        self.catalog.read().expect("sub catalog lock").is_some()
    }
}

/// Wraps `primary` with zero or more namespaced sub-MCPs. Built once via
/// [`ComposedBackend::compose`]; the merged behavior is described in the
/// module doc.
pub struct ComposedBackend {
    primary: Arc<dyn Backend>,
    subs: Vec<SubMcp>,
}

impl ComposedBackend {
    /// Build a client for each `spec`, probe its `tools/list` once, and wrap
    /// `primary`. Never fails because a sub is down — that is exactly the
    /// resilience this type exists to provide; a genuinely malformed `spec`
    /// (an unparseable `mcp_url`, which cannot happen for specs produced by
    /// `config::parse_sub_specs`) — or one that would send its bearer over
    /// plain `http://` to a non-loopback host without `allow_insecure_http`
    /// — is the only way this can error.
    pub async fn compose(
        primary: Arc<dyn Backend>,
        specs: Vec<SubSpec>,
        request_timeout: Duration,
        allow_insecure_http: bool,
    ) -> anyhow::Result<Self> {
        let mut subs = Vec::with_capacity(specs.len());
        for spec in specs {
            let mcp_url = RemoteHttp::resolve_mcp_url(&spec.url, None);
            let client = RemoteHttp::new(
                mcp_url.clone(),
                AuthHeaders {
                    bearer: spec.token.clone(),
                    ..Default::default()
                },
                request_timeout,
                allow_insecure_http,
            )
            .with_context(|| format!("building client for sub '{}' ({mcp_url})", spec.prefix))?;

            let catalog = match client.list_tools(json!({})).await {
                Ok(result) => {
                    let tools = result
                        .get("tools")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    tracing::info!(
                        sub = %spec.prefix,
                        url = %mcp_url,
                        tools = tools.len(),
                        "sub-MCP tools/list probe ok"
                    );
                    Some(tools)
                }
                Err(e) => {
                    // `e` is the RemoteHttp/reqwest error chain (transport
                    // failure, HTTP status + body, JSON-RPC error) — never the
                    // bearer token, which lives only in the request header.
                    tracing::warn!(
                        sub = %spec.prefix,
                        url = %mcp_url,
                        error = %format!("{e:#}"),
                        "sub-MCP tools/list probe failed at start; skipping this sub \
                         for the life of this process (the primary's catalog still serves)"
                    );
                    None
                }
            };

            subs.push(SubMcp {
                prefix: spec.prefix,
                client,
                catalog: RwLock::new(catalog),
            });
        }
        Ok(Self { primary, subs })
    }

    /// Prefixes of every configured sub (probed up or skipped down) — for
    /// startup logging.
    pub fn sub_prefixes(&self) -> Vec<&str> {
        self.subs.iter().map(|s| s.prefix.as_str()).collect()
    }
}

#[async_trait]
impl Backend for ComposedBackend {
    /// Answered by the primary alone; subs are never sent `initialize` (the
    /// resilience contract probes with `tools/list` only — see the module
    /// doc).
    async fn initialize(&self, client_params: Value) -> anyhow::Result<Value> {
        self.primary.initialize(client_params).await
    }

    /// The primary's own `tools/list` result, plus every up sub's tools
    /// renamed `<prefix>_<name>` — appended only on the primary's last page
    /// (mirrors `gateway.rs`'s control-tool append: a paginated primary
    /// catalog is never interleaved with the subs' tools). A down sub
    /// contributes nothing; its absence from the catalog is the only visible
    /// sign of it being skipped (`tools/call` to it is the loud sign).
    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        let mut result = self.primary.list_tools(params).await?;
        let paged = result.get("nextCursor").is_some_and(|c| !c.is_null());
        if !paged && !self.subs.is_empty() {
            let mut tools = result
                .get("tools")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for sub in &self.subs {
                tools.extend(sub.exposed_tools());
            }
            result["tools"] = json!(tools);
        }
        Ok(result)
    }

    /// Routes by name: a name starting with a configured `<prefix>_` always
    /// goes to that sub (a same-named primary tool is shadowed for calling
    /// purposes only — it is untouched in the catalog; see the module doc's
    /// collision note), bare with the prefix stripped, arguments untouched.
    /// A down sub is [`ToolNotFound`] without a network call. Anything else
    /// falls through to the primary unchanged.
    async fn call_tool(&self, mut params: Value) -> anyhow::Result<Value> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(name) = &name {
            for sub in &self.subs {
                let Some(bare) = name.strip_prefix(&format!("{}_", sub.prefix)) else {
                    continue;
                };
                if !sub.is_up() {
                    return Err(ToolNotFound(name.clone()).into());
                }
                params["name"] = json!(bare);
                return sub.client.call_tool(params).await;
            }
        }
        self.primary.call_tool(params).await
    }
}
