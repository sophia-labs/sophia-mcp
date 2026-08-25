//! `GatewayBackend` — the multi-graph REMOTE backend for a platform-next
//! (cloud-2) gateway.
//!
//! One proxy, two upstreams, one merged tool surface:
//!
//!   * the gateway **control plane** at `POST {base}/control/mcp`
//!     (`list_graphs`, `create_graph`, `manage_access`, `control_job_status`, …);
//!   * the **bound graph's cell** at `POST {base}/o/{owner}/g/{graph}/mcp`
//!     (garden's own tools — `search_documents`, `sparql_query`, `remember`, …).
//!
//! `tools/list` is the union of both catalogs (a control tool is prefixed
//! `control_` only when its name collides with a cell tool; the collision is
//! logged). `tools/call` routes by tool name. Every cell tool additionally
//! accepts an optional `graph_id` / `graphId` argument: when it names a graph
//! other than the bound one, the call is routed to `/o/{owner}/g/{that}/mcp`
//! — **by path**, so the gateway's ACL (the policy-enforcement point) decides,
//! and sophia-mcp never invents or auto-creates a graph. One upstream session
//! is cached per graph.
//!
//! **Routable ≠ boot-ready** (CEL-AVAIL-001). A cell can have a running pod
//! and still not answer MCP. sophia-mcp therefore treats exactly two things
//! as proof of routability: the activation record's terminal `ready` phase
//! *followed by* a successful MCP `initialize` on the cell path. A 200 from any
//! non-MCP probe (`/health`, `/healthz`, `activate` → `ready:true`) is never
//! taken as readiness. Waits are bounded (`--activation-timeout`, polled every
//! `--activation-poll`); on expiry the error names the elapsed seconds and the
//! last observed activation state — truthfully, never "not found".

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde_json::{json, Value};

use crate::mcp::{self, method};

use super::remote::{build_client, post_rpc, rpc_result, urlencode_segment, HttpReply};
use super::{AuthHeaders, Backend, ToolNotFound};

/// The gateway's terminal activation phases (`platform-next/gateway/src/activation.rs`,
/// `ActivationPhase`, serialized snake_case). Non-terminal: `scheduling`,
/// `scaling`, `hydrating`, `repairing`.
pub const PHASE_READY: &str = "ready";
pub const PHASE_FAILED: &str = "failed";

/// Gateway error `code` that is a typed, non-retryable 503: retrying the same
/// pod is deterministic churn (`AppError::CellRepairRequired`).
const CODE_REPAIR_REQUIRED: &str = "graph_repair_required";

/// Prefix applied to a control tool whose name collides with a cell tool.
pub const CONTROL_PREFIX: &str = "control_";

#[derive(Debug, Clone)]
pub struct GatewayOptions {
    /// Bound wait for a cell to become routable (default 300 s).
    pub activation_timeout: Duration,
    /// Interval between activation polls / cell-path retries (default 2 s).
    pub activation_poll: Duration,
    /// Also try `{base}/mcp` for control tools when `/control/mcp` is 404.
    pub unified_mcp_fallback: bool,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            activation_timeout: Duration::from_secs(300),
            activation_poll: Duration::from_secs(2),
            unified_mcp_fallback: false,
        }
    }
}

/// One cached upstream session per graph.
struct GraphSession {
    graph_id: String,
    mcp_url: String,
    activate_url: String,
    /// The cell's `initialize` result once it has been proven routable.
    /// `None` = not (or no longer) proven. Held across the whole wait so
    /// concurrent waiters serialize instead of racing the activation.
    init: tokio::sync::Mutex<Option<Value>>,
    /// A poll URL handed back by a 202 that has not been consumed yet (set by
    /// the connect-time activation kick).
    pending_poll: Mutex<Option<String>>,
    /// The connect-time kick already got `ready:true` from `activate`; the
    /// next wait may go straight to the MCP probe.
    kicked_ready: AtomicBool,
}

impl GraphSession {
    async fn invalidate(&self) {
        *self.init.lock().await = None;
    }
}

#[derive(Debug, Clone)]
enum Route {
    Control { upstream_name: String },
    Cell,
}

pub struct GatewayBackend {
    client: reqwest::Client,
    base: String,
    owner: String,
    bound_graph: String,
    control_url: String,
    opts: GatewayOptions,
    sessions: Mutex<HashMap<String, Arc<GraphSession>>>,
    /// Merged-name → upstream. Built by `tools/list`; built lazily by
    /// `tools/call` if the agent calls before listing.
    routing: Mutex<Option<HashMap<String, Route>>>,
    /// `(owner, graphId)` tuples the control plane listed for this identity.
    listed: Mutex<HashSet<(String, String)>>,
    /// The agent's `initialize` params, replayed to each cell on probe.
    client_init_params: Mutex<Value>,
    /// Polls (activation GETs + activate POSTs + MCP probes) spent by the most
    /// recent wait-for-routable. Also logged; exposed for tests/diagnostics.
    last_wait_polls: AtomicU64,
}

impl GatewayBackend {
    /// Bind to `(owner, graph)` on the gateway at `raw_base`.
    ///
    /// Strict, fast parts happen here: the control plane is probed
    /// (`/control/mcp`, optionally `/mcp`) and the tuple must appear in this
    /// identity's `list_graphs` — merely naming an unknown tuple never creates
    /// it. Activation of the bound graph is *kicked* (one `POST …/activate`)
    /// but not awaited: call [`GatewayBackend::warm`] for the bounded wait, so
    /// the stdio `initialize` handshake never blocks on a dormant cell.
    pub async fn connect(
        raw_base: &str,
        owner: &str,
        graph: &str,
        auth: AuthHeaders,
        opts: GatewayOptions,
    ) -> anyhow::Result<Self> {
        if owner.trim().is_empty() || graph.trim().is_empty() {
            return Err(anyhow!("owner and graph must be non-empty"));
        }
        let base = gateway_base(raw_base);
        let client = build_client(auth)?;
        let (control_url, graphs) =
            probe_control(&client, &base, opts.unified_mcp_fallback).await?;
        let this = Self {
            client,
            base,
            owner: owner.to_string(),
            bound_graph: graph.to_string(),
            control_url,
            opts,
            sessions: Mutex::new(HashMap::new()),
            routing: Mutex::new(None),
            listed: Mutex::new(HashSet::new()),
            client_init_params: Mutex::new(default_client_init_params()),
            last_wait_polls: AtomicU64::new(0),
        };
        this.absorb_listing(&graphs);
        this.assert_listed(graph).await?;
        let session = this.session_for(graph);
        this.kick_activation(&session).await?;
        Ok(this)
    }

    /// Bounded wait until the bound graph is routable. Progress goes to
    /// stderr (tracing); the error carries the last observed activation state
    /// and elapsed seconds.
    pub async fn warm(&self) -> anyhow::Result<()> {
        let session = self.session_for(&self.bound_graph);
        self.ensure_routable(&session).await.map(|_| ())
    }

    pub fn bound_graph(&self) -> &str {
        &self.bound_graph
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn control_url(&self) -> &str {
        &self.control_url
    }

    /// The cell MCP endpoint for `graph` under this backend's owner.
    pub fn mcp_url_for(&self, graph: &str) -> String {
        format!(
            "{}/o/{}/g/{}/mcp",
            self.base,
            urlencode_segment(&self.owner),
            urlencode_segment(graph)
        )
    }

    /// Polls spent by the most recent wait-for-routable (0 if the session was
    /// already proven).
    pub fn last_wait_polls(&self) -> u64 {
        self.last_wait_polls.load(Ordering::SeqCst)
    }

    // ---------------------------------------------------------------- sessions

    fn session_for(&self, graph: &str) -> Arc<GraphSession> {
        let mut sessions = self.sessions.lock().expect("sessions lock");
        sessions
            .entry(graph.to_string())
            .or_insert_with(|| {
                Arc::new(GraphSession {
                    graph_id: graph.to_string(),
                    mcp_url: self.mcp_url_for(graph),
                    activate_url: format!(
                        "{}/o/{}/g/{}/activate",
                        self.base,
                        urlencode_segment(&self.owner),
                        urlencode_segment(graph)
                    ),
                    init: tokio::sync::Mutex::new(None),
                    pending_poll: Mutex::new(None),
                    kicked_ready: AtomicBool::new(false),
                })
            })
            .clone()
    }

    fn absorb_listing(&self, graphs: &[Value]) {
        let mut listed = self.listed.lock().expect("listed lock");
        for item in graphs {
            if let (Some(owner), Some(graph)) = (
                item.get("owner").and_then(Value::as_str),
                item.get("graphId").and_then(Value::as_str),
            ) {
                listed.insert((owner.to_string(), graph.to_string()));
            }
        }
    }

    fn is_listed(&self, graph: &str) -> bool {
        self.listed
            .lock()
            .expect("listed lock")
            .contains(&(self.owner.clone(), graph.to_string()))
    }

    /// The tuple must be in this identity's `list_graphs`. Refreshes the
    /// listing once on a miss (a graph created after connect). Never touches
    /// the graph path for an unlisted tuple.
    async fn assert_listed(&self, graph: &str) -> anyhow::Result<()> {
        if self.is_listed(graph) {
            return Ok(());
        }
        let graphs = control_list_graphs(&self.client, &self.control_url)
            .await
            .context("refresh list_graphs through the control plane")?;
        self.absorb_listing(&graphs);
        if self.is_listed(graph) {
            return Ok(());
        }
        Err(anyhow!(
            "graph '{graph}' (owner {}) is not in this identity's list_graphs; \
             sophia-mcp will not activate or create it",
            self.owner
        ))
    }

    // -------------------------------------------------------------- activation

    /// One `POST …/activate` to start waking the cell. Records the poll URL
    /// (202) or the ready claim (200) for the next wait; surfaces the
    /// gateway's own 4xx verbatim.
    async fn kick_activation(&self, session: &GraphSession) -> anyhow::Result<()> {
        let reply = self.post_activate(session).await?;
        match reply.status {
            StatusCode::ACCEPTED => {
                let body = reply.json();
                let phase = body
                    .pointer("/activation/phase")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                if let Some(poll) = body.get("pollUrl").and_then(Value::as_str) {
                    *session.pending_poll.lock().expect("pending lock") =
                        Some(self.absolute(poll));
                }
                tracing::info!(
                    graph = %session.graph_id,
                    phase,
                    "activation started; will wait for routability on first use"
                );
                Ok(())
            }
            s if s.is_success() => {
                session.kicked_ready.store(true, Ordering::SeqCst);
                tracing::info!(
                    graph = %session.graph_id,
                    "activate reports a running cell (ready:true) — not yet proven routable"
                );
                Ok(())
            }
            s => Err(self.gateway_rejection(&session.graph_id, "activate", s, &reply.body)),
        }
    }

    /// Cached `initialize` result if the session is proven routable, else run
    /// the bounded wait. The budget starts when this waiter gets the session
    /// lock, so a waiter queued behind another does not inherit its clock.
    async fn ensure_routable(&self, session: &Arc<GraphSession>) -> anyhow::Result<Value> {
        let mut guard = session.init.lock().await;
        if let Some(init) = guard.as_ref() {
            return Ok(init.clone());
        }
        let init = self.wait_for_routable(session).await?;
        *guard = Some(init.clone());
        Ok(init)
    }

    /// The wait state machine: activate → poll `/activations/{id}` until
    /// terminal → prove with MCP `initialize` on the cell path → repeat on
    /// 202/502/503, all bounded by `activation_timeout`.
    async fn wait_for_routable(&self, session: &GraphSession) -> anyhow::Result<Value> {
        enum Step {
            Activate,
            Poll(String),
            Probe,
        }
        let started = Instant::now();
        let deadline = started + self.opts.activation_timeout;
        let graph = session.graph_id.as_str();
        let mut polls: u64 = 0;
        let mut last_state = String::from("no activation observation yet");
        let mut step = if let Some(url) = session.pending_poll.lock().expect("pending lock").take()
        {
            Step::Poll(url)
        } else if session.kicked_ready.swap(false, Ordering::SeqCst) {
            Step::Probe
        } else {
            Step::Activate
        };
        loop {
            if Instant::now() >= deadline {
                self.last_wait_polls.store(polls, Ordering::SeqCst);
                return Err(self.timeout_error(graph, started, polls, &last_state));
            }
            match step {
                Step::Activate => {
                    polls += 1;
                    let reply = self.post_activate(session).await?;
                    let body = reply.json();
                    match reply.status {
                        StatusCode::ACCEPTED => {
                            last_state = describe_activation(
                                body.get("activation").unwrap_or(&Value::Null),
                            );
                            match body.get("pollUrl").and_then(Value::as_str) {
                                Some(poll) => {
                                    step = Step::Poll(self.absolute(poll));
                                    continue;
                                }
                                None => {
                                    last_state.push_str(" (202 without pollUrl)");
                                }
                            }
                        }
                        s if s.is_success() => {
                            last_state = "activate reports ready:true (a running pod — \
                                          not yet proven routable)"
                                .to_string();
                            step = Step::Probe;
                            continue;
                        }
                        s if is_retryable(s, &body) => {
                            last_state = format!("activate answered HTTP {s}: {}", reply.body.trim());
                        }
                        s => {
                            return Err(self.gateway_rejection(graph, "activate", s, &reply.body))
                        }
                    }
                    tracing::info!(graph, polls, state = %last_state, "waiting for activation");
                    tokio::time::sleep(self.opts.activation_poll).await;
                }
                Step::Poll(ref url) => {
                    polls += 1;
                    let reply = self.get(url).await?;
                    let body = reply.json();
                    match reply.status {
                        s if s.is_success() => {
                            last_state = describe_activation(&body);
                            match body.get("phase").and_then(Value::as_str) {
                                Some(PHASE_READY) => {
                                    tracing::info!(
                                        graph,
                                        polls,
                                        "activation reached phase=ready; probing MCP"
                                    );
                                    step = Step::Probe;
                                    continue;
                                }
                                Some(PHASE_FAILED) => {
                                    self.last_wait_polls.store(polls, Ordering::SeqCst);
                                    return Err(anyhow!(
                                        "graph '{}/{graph}' activation failed after {}s: {}",
                                        self.owner,
                                        started.elapsed().as_secs(),
                                        body.get("error")
                                            .and_then(Value::as_str)
                                            .unwrap_or("no error detail")
                                    ));
                                }
                                _ => {}
                            }
                        }
                        s if is_retryable(s, &body) => {
                            last_state =
                                format!("activation poll answered HTTP {s}: {}", reply.body.trim());
                        }
                        s => {
                            return Err(self.gateway_rejection(
                                graph,
                                "activation poll",
                                s,
                                &reply.body,
                            ))
                        }
                    }
                    tracing::info!(graph, polls, state = %last_state, "waiting for activation");
                    tokio::time::sleep(self.opts.activation_poll).await;
                }
                Step::Probe => {
                    polls += 1;
                    let params = self.client_init_params.lock().expect("init lock").clone();
                    let reply =
                        post_rpc(&self.client, &session.mcp_url, method::INITIALIZE, params).await?;
                    match classify_cell_reply(reply) {
                        CellReply::Result(init) => {
                            self.last_wait_polls.store(polls, Ordering::SeqCst);
                            tracing::info!(
                                graph,
                                polls,
                                elapsed_s = started.elapsed().as_secs(),
                                "cell routable (MCP initialize succeeded)"
                            );
                            return Ok(init);
                        }
                        CellReply::RpcError { code, message } => {
                            self.last_wait_polls.store(polls, Ordering::SeqCst);
                            return Err(anyhow!(
                                "cell for graph '{}/{graph}' answered initialize with JSON-RPC error {code}: {message}",
                                self.owner
                            ));
                        }
                        CellReply::Activating { poll_url, state } => {
                            last_state = state;
                            if let Some(poll) = poll_url {
                                step = Step::Poll(self.absolute(&poll));
                                continue;
                            }
                            step = Step::Activate;
                        }
                        CellReply::Unavailable { status, body } => {
                            last_state = format!("cell path answered HTTP {status}: {}", body.trim());
                            step = Step::Activate;
                        }
                        CellReply::Rejected { status, body } => {
                            self.last_wait_polls.store(polls, Ordering::SeqCst);
                            return Err(self.gateway_rejection(graph, "mcp", status, &body));
                        }
                    }
                    tracing::info!(graph, polls, state = %last_state, "cell not routable yet");
                    tokio::time::sleep(self.opts.activation_poll).await;
                }
            }
        }
    }

    fn timeout_error(
        &self,
        graph: &str,
        started: Instant,
        polls: u64,
        last_state: &str,
    ) -> anyhow::Error {
        anyhow!(
            "graph '{}/{graph}' is not routable after {:.1}s (activation budget {:.1}s, {polls} polls); \
             last observed activation state: {last_state}",
            self.owner,
            started.elapsed().as_secs_f64(),
            self.opts.activation_timeout.as_secs_f64()
        )
    }

    /// The gateway said no (403/404/400/…): surface status + body verbatim.
    /// The gateway is the policy-enforcement point; sophia-mcp does not
    /// reinterpret its verdicts.
    fn gateway_rejection(
        &self,
        graph: &str,
        what: &str,
        status: StatusCode,
        body: &str,
    ) -> anyhow::Error {
        anyhow!(
            "gateway returned HTTP {status} for {what} of graph '{}/{graph}': {}",
            self.owner,
            body.trim()
        )
    }

    async fn post_activate(&self, session: &GraphSession) -> anyhow::Result<HttpReply> {
        let http = self
            .client
            .post(&session.activate_url)
            .send()
            .await
            .with_context(|| format!("POST {}", session.activate_url))?;
        let status = http.status();
        let body = http.text().await.context("read activate response body")?;
        Ok(HttpReply { status, body })
    }

    async fn get(&self, url: &str) -> anyhow::Result<HttpReply> {
        let http = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = http.status();
        let body = http.text().await.context("read activation poll body")?;
        Ok(HttpReply { status, body })
    }

    fn absolute(&self, url: &str) -> String {
        if url.starts_with("http://") || url.starts_with("https://") {
            url.to_string()
        } else {
            format!("{}/{}", self.base, url.trim_start_matches('/'))
        }
    }

    // ------------------------------------------------------------------- rpc

    async fn control_rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let reply = post_rpc(&self.client, &self.control_url, method, params).await?;
        rpc_result(reply, method).with_context(|| format!("control plane {}", self.control_url))
    }

    /// One MCP call against a graph cell, with wait-for-routable before and
    /// retry-after-wait on any 202/502/503 answer, bounded by the activation
    /// budget measured from this call's start.
    async fn cell_rpc(
        &self,
        session: &Arc<GraphSession>,
        method: &str,
        params: Value,
    ) -> anyhow::Result<Value> {
        let started = Instant::now();
        let deadline = started + self.opts.activation_timeout;
        let mut retries: u64 = 0;
        loop {
            self.ensure_routable(session).await?;
            let reply = post_rpc(&self.client, &session.mcp_url, method, params.clone()).await?;
            let state = match classify_cell_reply(reply) {
                CellReply::Result(value) => return Ok(value),
                CellReply::RpcError { code, message } => {
                    return Err(anyhow!(
                        "backend JSON-RPC error {code} for {method}: {message}"
                    ))
                }
                CellReply::Rejected { status, body } => {
                    return Err(self.gateway_rejection(&session.graph_id, method, status, &body))
                }
                CellReply::Activating { poll_url, state } => {
                    if let Some(poll) = poll_url {
                        *session.pending_poll.lock().expect("pending lock") =
                            Some(self.absolute(&poll));
                    }
                    state
                }
                CellReply::Unavailable { status, body } => {
                    format!("cell path answered HTTP {status}: {}", body.trim())
                }
            };
            session.invalidate().await;
            retries += 1;
            tracing::warn!(
                graph = %session.graph_id,
                method,
                retries,
                state = %state,
                "cell answered not-routable mid-session; re-waiting for activation"
            );
            if Instant::now() >= deadline {
                return Err(self.timeout_error(&session.graph_id, started, retries, &state));
            }
            tokio::time::sleep(self.opts.activation_poll).await;
        }
    }

    // --------------------------------------------------------------- routing

    /// Fetch both catalogs, merge them, and (re)build the routing table.
    async fn refresh_catalog(&self, params: Value) -> anyhow::Result<Value> {
        let control = self.control_rpc(method::TOOLS_LIST, json!({})).await?;
        let session = self.session_for(&self.bound_graph);
        let cell = self.cell_rpc(&session, method::TOOLS_LIST, params).await?;
        let merged = merge_catalogs(&cell, &control, &self.bound_graph);
        for name in &merged.collisions {
            tracing::warn!(
                tool = %name,
                renamed = %format!("{CONTROL_PREFIX}{name}"),
                "control tool name collides with a cell tool; control tool exposed under the prefixed name"
            );
        }
        tracing::info!(
            cell_tools = merged.cell_count,
            control_tools = merged.control_count,
            collisions = merged.collisions.len(),
            "merged tool catalog"
        );
        *self.routing.lock().expect("routing lock") = Some(merged.routes);
        Ok(merged.result)
    }

    async fn route_for(&self, name: &str) -> anyhow::Result<Option<Route>> {
        let known = self.routing.lock().expect("routing lock").clone();
        let routes = match known {
            Some(routes) => routes,
            None => {
                self.refresh_catalog(json!({})).await?;
                self.routing
                    .lock()
                    .expect("routing lock")
                    .clone()
                    .unwrap_or_default()
            }
        };
        Ok(routes.get(name).cloned())
    }
}

#[async_trait]
impl Backend for GatewayBackend {
    /// Answered locally: this backend is the union of two upstreams, so it
    /// presents itself as `sophia-mcp`. The agent's params are kept and
    /// replayed to each cell when proving routability. Never blocks on a
    /// dormant cell — the stdio handshake must not.
    async fn initialize(&self, client_params: Value) -> anyhow::Result<Value> {
        if client_params.is_object() {
            *self.client_init_params.lock().expect("init lock") = client_params;
        }
        Ok(json!({
            "protocolVersion": mcp::PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "sophia-mcp", "version": env!("CARGO_PKG_VERSION") },
        }))
    }

    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        self.refresh_catalog(params).await
    }

    async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("tools/call params.name must be a string"))?
            .to_string();
        match self.route_for(&name).await? {
            None => Err(ToolNotFound(name).into()),
            Some(Route::Control { upstream_name }) => {
                let mut upstream = params.clone();
                upstream["name"] = json!(upstream_name);
                self.control_rpc(method::TOOLS_CALL, upstream).await
            }
            Some(Route::Cell) => {
                let target = requested_graph(params.get("arguments"))
                    .unwrap_or_else(|| self.bound_graph.clone());
                if target != self.bound_graph {
                    self.assert_listed(&target).await?;
                    tracing::info!(tool = %name, graph = %target, "routing cell tool to sibling graph");
                }
                let session = self.session_for(&target);
                self.cell_rpc(&session, method::TOOLS_CALL, params).await
            }
        }
    }
}

// ------------------------------------------------------------ free helpers

fn default_client_init_params() -> Value {
    json!({
        "protocolVersion": mcp::PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": { "name": "sophia-mcp", "version": env!("CARGO_PKG_VERSION") },
    })
}

fn gateway_base(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('/');
    trimmed
        .find("/o/")
        .map(|index| trimmed[..index].to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

/// Probe the control plane: `/control/mcp` first; `/mcp` only when the
/// gateway 404s the former AND the fallback is enabled. Returns the control
/// URL plus this identity's `list_graphs` rows.
async fn probe_control(
    client: &reqwest::Client,
    base: &str,
    unified_fallback: bool,
) -> anyhow::Result<(String, Vec<Value>)> {
    let primary = format!("{base}/control/mcp");
    match control_list_graphs(client, &primary).await {
        Ok(graphs) => Ok((primary, graphs)),
        Err(ControlError::NotFound(body)) if unified_fallback => {
            let unified = format!("{base}/mcp");
            tracing::warn!(
                primary = %primary,
                body = %body,
                "control plane 404 at /control/mcp; trying unified /mcp"
            );
            let graphs = control_list_graphs(client, &unified)
                .await
                .with_context(|| format!("list accessible graph cells through {unified}"))?;
            Ok((unified, graphs))
        }
        Err(e) => Err(anyhow::Error::from(e))
            .with_context(|| format!("list accessible graph cells through {primary}")),
    }
}

#[derive(Debug, thiserror::Error)]
enum ControlError {
    #[error("control plane not found (HTTP 404): {0}")]
    NotFound(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

async fn control_list_graphs(
    client: &reqwest::Client,
    control_url: &str,
) -> Result<Vec<Value>, ControlError> {
    let reply = post_rpc(
        client,
        control_url,
        method::TOOLS_CALL,
        json!({ "name": "list_graphs", "arguments": {} }),
    )
    .await?;
    if reply.status == StatusCode::NOT_FOUND {
        return Err(ControlError::NotFound(reply.body.trim().to_string()));
    }
    let result = rpc_result(reply, "tools/call list_graphs")?;
    let graphs = result
        .get("structuredContent")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow!("control list_graphs returned no structuredContent array"))?;
    Ok(graphs)
}

/// A graph named by the tool arguments (`graph_id` or `graphId`, non-empty
/// string), else `None`.
fn requested_graph(arguments: Option<&Value>) -> Option<String> {
    let args = arguments?.as_object()?;
    ["graph_id", "graphId"]
        .iter()
        .find_map(|key| args.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Gateway 5xx that a later retry may clear (cold cell, capacity, lease
/// authority). `graph_repair_required` is typed non-retryable.
fn is_retryable(status: StatusCode, body: &Value) -> bool {
    if body.get("code").and_then(Value::as_str) == Some(CODE_REPAIR_REQUIRED) {
        return false;
    }
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

/// Render an `ActivationRecord` (or the 202 `activation` object) as one line:
/// `phase=<phase> — <last event detail>[; error: …]`.
fn describe_activation(record: &Value) -> String {
    let phase = record
        .get("phase")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let detail = record
        .get("events")
        .and_then(Value::as_array)
        .and_then(|events| events.last())
        .and_then(|event| event.get("detail"))
        .and_then(Value::as_str);
    let mut out = format!("phase={phase}");
    if let Some(detail) = detail {
        out.push_str(&format!(" — {detail}"));
    }
    if let Some(error) = record.get("error").and_then(Value::as_str) {
        out.push_str(&format!("; error: {error}"));
    }
    out
}

enum CellReply {
    Result(Value),
    RpcError { code: i64, message: String },
    /// HTTP 202 `graph_activating` from the owner-scoped cell path.
    Activating { poll_url: Option<String>, state: String },
    /// 502 `CellUnavailable` / 503 at-capacity etc. — retry after a wait.
    Unavailable { status: StatusCode, body: String },
    /// Any other non-2xx: the gateway's verdict, surfaced verbatim.
    Rejected { status: StatusCode, body: String },
}

fn classify_cell_reply(reply: HttpReply) -> CellReply {
    let status = reply.status;
    if status == StatusCode::ACCEPTED {
        let body = reply.json();
        let code = body.get("code").and_then(Value::as_str).unwrap_or("accepted");
        let phase = body.get("phase").and_then(Value::as_str).unwrap_or("unknown");
        return CellReply::Activating {
            poll_url: body.get("pollUrl").and_then(Value::as_str).map(str::to_string),
            state: format!("cell path answered 202 {code}, phase={phase}"),
        };
    }
    if !status.is_success() {
        let body_json = reply.json();
        return if is_retryable(status, &body_json) {
            CellReply::Unavailable {
                status,
                body: reply.body,
            }
        } else {
            CellReply::Rejected {
                status,
                body: reply.body,
            }
        };
    }
    match serde_json::from_str::<crate::mcp::JsonRpcResponse>(&reply.body) {
        Ok(resp) => match resp.error {
            Some(err) => CellReply::RpcError {
                code: err.code,
                message: err.message,
            },
            None => CellReply::Result(resp.result.unwrap_or(Value::Null)),
        },
        Err(e) => CellReply::RpcError {
            code: mcp::PARSE_ERROR,
            message: format!("unparseable JSON-RPC response: {e}: {}", reply.body),
        },
    }
}

struct Merged {
    result: Value,
    routes: HashMap<String, Route>,
    collisions: Vec<String>,
    cell_count: usize,
    control_count: usize,
}

/// Union of the cell catalog (first, augmented with `graph_id`/`graphId`) and
/// the control catalog (renamed with [`CONTROL_PREFIX`] only on collision).
/// `nextCursor` from the cell list, if any, is preserved.
fn merge_catalogs(cell: &Value, control: &Value, bound_graph: &str) -> Merged {
    let mut tools: Vec<Value> = Vec::new();
    let mut routes: HashMap<String, Route> = HashMap::new();
    let mut collisions = Vec::new();

    let cell_tools = cell
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for mut tool in cell_tools {
        if let Some(name) = tool.get("name").and_then(Value::as_str).map(str::to_string) {
            add_graph_argument(&mut tool, bound_graph);
            routes.insert(name, Route::Cell);
            tools.push(tool);
        }
    }
    let cell_count = tools.len();

    let control_tools = control
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut control_count = 0;
    for mut tool in control_tools {
        let Some(name) = tool.get("name").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        let exposed = if routes.contains_key(&name) {
            collisions.push(name.clone());
            let renamed = format!("{CONTROL_PREFIX}{name}");
            tool["name"] = json!(renamed);
            renamed
        } else {
            name.clone()
        };
        routes.insert(
            exposed,
            Route::Control {
                upstream_name: name,
            },
        );
        tools.push(tool);
        control_count += 1;
    }

    let mut result = json!({ "tools": tools });
    if let Some(cursor) = cell.get("nextCursor") {
        result["nextCursor"] = cursor.clone();
    }
    Merged {
        result,
        routes,
        collisions,
        cell_count,
        control_count,
    }
}

/// Add optional `graph_id` + `graphId` string properties to a cell tool's
/// input schema when absent, so the agent learns it can route the call to a
/// sibling graph of the same owner.
fn add_graph_argument(tool: &mut Value, bound_graph: &str) {
    let description = format!(
        "Optional. Route this call to another graph you have access to (same owner); \
         defaults to the bound graph '{bound_graph}'. Routing is by gateway path, so the \
         gateway's ACL decides — naming a graph never creates it."
    );
    let schema = tool
        .as_object_mut()
        .map(|t| t.entry("inputSchema").or_insert_with(|| json!({ "type": "object" })));
    let Some(schema) = schema.and_then(Value::as_object_mut) else {
        return;
    };
    let properties = schema
        .entry("properties")
        .or_insert_with(|| json!({}));
    let Some(properties) = properties.as_object_mut() else {
        return;
    };
    for key in ["graph_id", "graphId"] {
        properties.entry(key).or_insert_with(|| {
            json!({ "type": "string", "description": description })
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_prefixes_only_colliding_control_tools_and_routes_by_name() {
        let cell = json!({ "tools": [
            { "name": "search_documents", "inputSchema": { "type": "object", "properties": {} } },
            { "name": "list_graphs", "inputSchema": { "type": "object" } }
        ], "nextCursor": "c1" });
        let control = json!({ "tools": [
            { "name": "list_graphs", "inputSchema": { "type": "object" } },
            { "name": "create_graph", "inputSchema": { "type": "object" } }
        ]});
        let merged = merge_catalogs(&cell, &control, "notes");
        let names: Vec<&str> = merged.result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["search_documents", "list_graphs", "control_list_graphs", "create_graph"]
        );
        assert_eq!(merged.collisions, vec!["list_graphs".to_string()]);
        assert_eq!(merged.result["nextCursor"], json!("c1"));
        assert!(matches!(merged.routes["search_documents"], Route::Cell));
        assert!(matches!(merged.routes["list_graphs"], Route::Cell));
        assert!(matches!(
            &merged.routes["control_list_graphs"],
            Route::Control { upstream_name } if upstream_name == "list_graphs"
        ));
        assert!(matches!(
            &merged.routes["create_graph"],
            Route::Control { upstream_name } if upstream_name == "create_graph"
        ));
        // Cell tools gained the routing argument; control tools did not.
        assert_eq!(
            merged.result["tools"][0]["inputSchema"]["properties"]["graph_id"]["type"],
            json!("string")
        );
        assert_eq!(
            merged.result["tools"][1]["inputSchema"]["properties"]["graphId"]["type"],
            json!("string")
        );
        assert!(merged.result["tools"][3]["inputSchema"].get("properties").is_none());
    }

    #[test]
    fn graph_argument_does_not_overwrite_an_existing_schema_entry() {
        let mut tool = json!({ "name": "t", "inputSchema": { "type": "object",
            "properties": { "graph_id": { "type": "string", "description": "garden's own" } } } });
        add_graph_argument(&mut tool, "notes");
        assert_eq!(
            tool["inputSchema"]["properties"]["graph_id"]["description"],
            json!("garden's own")
        );
        assert!(tool["inputSchema"]["properties"]["graphId"].is_object());
    }

    #[test]
    fn requested_graph_reads_both_spellings_and_ignores_blank_or_non_string() {
        assert_eq!(
            requested_graph(Some(&json!({ "graph_id": "a" }))),
            Some("a".into())
        );
        assert_eq!(
            requested_graph(Some(&json!({ "graphId": " b " }))),
            Some("b".into())
        );
        assert_eq!(requested_graph(Some(&json!({ "graph_id": "" }))), None);
        assert_eq!(requested_graph(Some(&json!({ "graph_id": 7 }))), None);
        assert_eq!(requested_graph(Some(&json!({ "graph_id": null }))), None);
        assert_eq!(requested_graph(None), None);
    }

    #[test]
    fn describe_activation_uses_phase_last_detail_and_error() {
        let record = json!({
            "phase": "hydrating",
            "events": [
                { "phase": "scheduling", "detail": "cell placement requested" },
                { "phase": "hydrating", "detail": "cell is hydrating its registered generation" }
            ]
        });
        assert_eq!(
            describe_activation(&record),
            "phase=hydrating — cell is hydrating its registered generation"
        );
        let failed = json!({ "phase": "failed", "events": [], "error": "boom" });
        assert_eq!(describe_activation(&failed), "phase=failed; error: boom");
    }

    #[test]
    fn repair_required_is_not_retryable_but_plain_503_is() {
        assert!(is_retryable(StatusCode::SERVICE_UNAVAILABLE, &json!({ "code": "at_capacity" })));
        assert!(is_retryable(StatusCode::BAD_GATEWAY, &Value::Null));
        assert!(!is_retryable(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "code": "graph_repair_required" })
        ));
        assert!(!is_retryable(StatusCode::FORBIDDEN, &Value::Null));
        assert!(!is_retryable(StatusCode::NOT_FOUND, &Value::Null));
    }

    #[test]
    fn gateway_base_strips_owner_path() {
        assert_eq!(
            gateway_base("https://gw.example/o/user%3Ax/g/notes/mcp"),
            "https://gw.example"
        );
        assert_eq!(gateway_base("https://gw.example/"), "https://gw.example");
    }
}
