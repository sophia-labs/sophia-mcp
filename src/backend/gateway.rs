//! `GatewayBackend` — the multi-graph REMOTE backend for a platform-next
//! (cloud-2) gateway.
//!
//! One proxy, two upstreams, one merged tool surface:
//!
//!   * the gateway **control plane** at `POST {base}/control/mcp`
//!     (`list_graphs`, `create_graph`, `manage_access`, `control_job_status`, …),
//!     always exposed under the stable `control_` prefix;
//!   * the **bound graph's cell** at `POST {base}/o/{owner}/g/{graph}/mcp`
//!     (garden's own tools — `search_documents`, `sparql_query`, `remember`, …).
//!
//! `tools/list` is the union of both catalogs; `tools/call` routes by tool
//! name. Every cell tool additionally accepts an optional `graph_id` /
//! `graphId` argument: when it names a graph other than the bound one, the call
//! is routed to `/o/{listing-owner}/g/{that}/mcp` — **by path**, with the owner
//! taken from this identity's `list_graphs` (never from an argument), so the
//! gateway's ACL (the policy-enforcement point) decides and sophia-mcp never
//! invents or auto-creates a graph. The body is rewritten so the cell sees
//! exactly one graph argument, equal to the path. One upstream session is
//! cached per `(owner, graph)`.
//!
//! **Routable ≠ boot-ready** (CEL-AVAIL-001). A cell can have a running pod
//! and still not answer MCP. sophia-mcp therefore treats exactly two things
//! as proof of routability: the activation record's terminal `ready` phase
//! *followed by* a successful MCP `initialize` on the cell path. A 200 from any
//! non-MCP probe (`/health`, `/healthz`, `activate` → `ready:true`) is never
//! taken as readiness. Waits are bounded (`--activation-timeout`; polls every
//! `--activation-poll`, re-probes with backoff, at most one `activate` per
//! wait); every HTTP request is bounded by the remaining budget and a
//! per-request ceiling (`--request-timeout`), so a hung upstream cannot stretch
//! the wait. On expiry the error names the elapsed seconds and the last
//! observed activation state — truthfully, never "not found".

use std::collections::HashMap;
use std::future::Future;
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

/// `lifecycleState` values under which the gateway will route/activate a graph
/// (`LifecycleState::is_visible`: provisioning | active | repairing). Rows in
/// any other state (`tombstoned`, `purging`, `purged`) are listed but never
/// activated by sophia-mcp.
const ACTIVATABLE_STATES: [&str; 3] = ["provisioning", "active", "repairing"];

/// Prefix under which every control-plane tool is exposed. Always applied, so
/// the agent-facing names are stable across cell releases regardless of which
/// names a cell happens to serve.
pub const CONTROL_PREFIX: &str = "control_";

/// Re-probe backoff cap, as a multiple of `activation_poll`.
const BACKOFF_CAP_MULTIPLIER: u32 = 8;

#[derive(Debug, Clone)]
pub struct GatewayOptions {
    /// Bound wait for a cell to become routable (default 300 s).
    pub activation_timeout: Duration,
    /// Interval between activation polls; base of the re-probe backoff
    /// (default 2 s).
    pub activation_poll: Duration,
    /// Per-request ceiling for any single HTTP request (default 120 s). Wait
    /// steps are additionally bounded by the remaining activation budget.
    pub request_timeout: Duration,
    /// Also try `{base}/mcp` for control tools when `/control/mcp` is 404.
    pub unified_mcp_fallback: bool,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            activation_timeout: Duration::from_secs(300),
            activation_poll: Duration::from_secs(2),
            request_timeout: Duration::from_secs(120),
            unified_mcp_fallback: false,
        }
    }
}

/// A single HTTP request exceeded its bound. Surfaced by name so waits can
/// record it as the last observation instead of aborting.
#[derive(Debug, thiserror::Error)]
#[error("request timed out after {after:.1}s: {what}", after = .after.as_secs_f64())]
pub struct RequestTimedOut {
    pub what: String,
    pub after: Duration,
}

/// One cached upstream session per `(owner, graph)`.
struct GraphSession {
    owner: String,
    graph_id: String,
    mcp_url: String,
    activate_url: String,
    /// The cell's `initialize` result once it has been proven routable.
    /// `None` = not (or no longer) proven. Held across the whole wait so
    /// concurrent waiters serialize instead of racing the activation.
    init: tokio::sync::Mutex<Option<Value>>,
    /// A poll URL handed back by a 202 that has not been consumed yet.
    pending_poll: Mutex<Option<String>>,
    /// The connect-time kick already got `ready:true` from `activate`; the
    /// next wait may go straight to the MCP probe.
    kicked_ready: AtomicBool,
    /// Last observation made by any waiter on this session, so a waiter that
    /// times out queued behind another can still report a real state.
    last_state: Mutex<String>,
}

impl GraphSession {
    async fn invalidate(&self) {
        *self.init.lock().await = None;
    }

    fn label(&self) -> String {
        format!("{}/{}", self.owner, self.graph_id)
    }

    fn observe(&self, state: &str) {
        *self.last_state.lock().expect("last_state lock") = state.to_string();
    }

    fn last_state(&self) -> String {
        self.last_state.lock().expect("last_state lock").clone()
    }
}

#[derive(Debug, Clone)]
enum Route {
    Control { upstream_name: String },
    Cell,
}

/// Where a graph id resolved to, per this identity's listing.
#[derive(Default, Clone)]
struct Listing {
    /// graph id → owners under which it is activatable.
    activatable: HashMap<String, Vec<String>>,
    /// graph id → (owner, lifecycleState) rows that are listed but NOT
    /// activatable (tombstoned / purging / purged).
    dormant: HashMap<String, Vec<(String, String)>>,
}

pub struct GatewayBackend {
    client: reqwest::Client,
    base: String,
    owner: String,
    bound_graph: String,
    control_url: String,
    opts: GatewayOptions,
    sessions: Mutex<HashMap<(String, String), Arc<GraphSession>>>,
    /// Merged-name → upstream. Built by `tools/list`; built lazily by
    /// `tools/call` if the agent calls before listing.
    routing: Mutex<HashMap<String, Route>>,
    catalog_built: AtomicBool,
    /// This identity's `list_graphs`, replaced wholesale on every refresh.
    listing: Mutex<Listing>,
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
    /// it. Activation of the bound graph is *kicked* (one `POST …/activate`;
    /// a retryable 5xx there is logged, not fatal) but not awaited: call
    /// [`GatewayBackend::warm`] for the bounded wait, so the stdio
    /// `initialize` handshake never blocks on a dormant cell.
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
        let (control_url, graphs) = probe_control(
            &client,
            &base,
            opts.unified_mcp_fallback,
            opts.request_timeout,
        )
        .await?;
        let this = Self {
            client,
            base,
            owner: owner.to_string(),
            bound_graph: graph.to_string(),
            control_url,
            opts,
            sessions: Mutex::new(HashMap::new()),
            routing: Mutex::new(HashMap::new()),
            catalog_built: AtomicBool::new(false),
            listing: Mutex::new(Listing::default()),
            client_init_params: Mutex::new(default_client_init_params()),
            last_wait_polls: AtomicU64::new(0),
        };
        this.replace_listing(&graphs);
        this.assert_bound_listed()?;
        let session = this.session_for(owner, graph);
        this.kick_activation(&session).await?;
        Ok(this)
    }

    /// Bounded wait until the bound graph is routable. Progress goes to
    /// stderr (tracing); the error carries the last observed activation state
    /// and elapsed seconds.
    pub async fn warm(&self) -> anyhow::Result<()> {
        let session = self.session_for(&self.owner, &self.bound_graph);
        let started = Instant::now();
        self.ensure_routable(&session, started + self.opts.activation_timeout, started)
            .await
            .map(|_| ())
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
        self.mcp_url_of(&self.owner, graph)
    }

    fn mcp_url_of(&self, owner: &str, graph: &str) -> String {
        format!(
            "{}/o/{}/g/{}/mcp",
            self.base,
            urlencode_segment(owner),
            urlencode_segment(graph)
        )
    }

    /// Polls spent by the most recent wait-for-routable (0 if the session was
    /// already proven).
    pub fn last_wait_polls(&self) -> u64 {
        self.last_wait_polls.load(Ordering::SeqCst)
    }

    // ---------------------------------------------------------------- sessions

    fn session_for(&self, owner: &str, graph: &str) -> Arc<GraphSession> {
        let mut sessions = self.sessions.lock().expect("sessions lock");
        sessions
            .entry((owner.to_string(), graph.to_string()))
            .or_insert_with(|| {
                Arc::new(GraphSession {
                    owner: owner.to_string(),
                    graph_id: graph.to_string(),
                    mcp_url: self.mcp_url_of(owner, graph),
                    activate_url: format!(
                        "{}/o/{}/g/{}/activate",
                        self.base,
                        urlencode_segment(owner),
                        urlencode_segment(graph)
                    ),
                    init: tokio::sync::Mutex::new(None),
                    pending_poll: Mutex::new(None),
                    kicked_ready: AtomicBool::new(false),
                    last_state: Mutex::new("no activation observation yet".to_string()),
                })
            })
            .clone()
    }

    // ---------------------------------------------------------------- listing

    /// Replace (never union) the listing: a row that disappears is forgotten,
    /// a row whose `lifecycleState` is not activatable is remembered only to
    /// explain a refusal.
    fn replace_listing(&self, graphs: &[Value]) {
        let mut next = Listing::default();
        for item in graphs {
            let (Some(owner), Some(graph)) = (
                item.get("owner").and_then(Value::as_str),
                item.get("graphId").and_then(Value::as_str),
            ) else {
                continue;
            };
            let state = item
                .get("lifecycleState")
                .and_then(Value::as_str)
                .map(|s| s.to_ascii_lowercase());
            let activatable = state
                .as_deref()
                .map(|s| ACTIVATABLE_STATES.contains(&s))
                .unwrap_or(true);
            if activatable {
                next.activatable
                    .entry(graph.to_string())
                    .or_default()
                    .push(owner.to_string());
            } else {
                next.dormant.entry(graph.to_string()).or_default().push((
                    owner.to_string(),
                    state.unwrap_or_default(),
                ));
            }
        }
        tracing::debug!(
            activatable = next.activatable.len(),
            not_activatable = next.dormant.len(),
            "list_graphs absorbed"
        );
        *self.listing.lock().expect("listing lock") = next;
    }

    async fn refresh_listing(&self) -> anyhow::Result<()> {
        let graphs = self
            .bounded(
                &format!("POST {} (list_graphs)", self.control_url),
                None,
                async {
                    control_list_graphs(&self.client, &self.control_url)
                        .await
                        .map_err(anyhow::Error::from)
                },
            )
            .await
            .context("refresh list_graphs through the control plane")?;
        self.replace_listing(&graphs);
        Ok(())
    }

    fn owners_of(&self, graph: &str) -> Vec<String> {
        self.listing
            .lock()
            .expect("listing lock")
            .activatable
            .get(graph)
            .cloned()
            .unwrap_or_default()
    }

    fn not_listed_error(&self, graph: &str) -> anyhow::Error {
        let dormant = self
            .listing
            .lock()
            .expect("listing lock")
            .dormant
            .get(graph)
            .cloned()
            .unwrap_or_default();
        if dormant.is_empty() {
            anyhow!(
                "graph '{graph}' is not in this identity's list_graphs; \
                 sophia-mcp will not activate or create it"
            )
        } else {
            let rows: Vec<String> = dormant
                .iter()
                .map(|(owner, state)| format!("{owner} (lifecycleState '{state}')"))
                .collect();
            anyhow!(
                "graph '{graph}' is listed but not activatable — {}; \
                 sophia-mcp will not activate it",
                rows.join(", ")
            )
        }
    }

    /// The bound tuple must be listed under exactly the `--owner` given.
    fn assert_bound_listed(&self) -> anyhow::Result<()> {
        if self.owners_of(&self.bound_graph).iter().any(|o| o == &self.owner) {
            return Ok(());
        }
        let err = self.not_listed_error(&self.bound_graph);
        Err(anyhow!("{err:#} (owner {})", self.owner))
    }

    /// Resolve a `graph_id` argument to the `(owner, graph)` tuple this
    /// identity's listing names for it — never an argument's owner. Refreshes
    /// the listing once on a miss. Ambiguity (same id under two owners) is
    /// refused naming both.
    async fn resolve_sibling(&self, graph: &str) -> anyhow::Result<(String, String)> {
        let mut owners = self.owners_of(graph);
        if owners.is_empty() {
            self.refresh_listing().await?;
            owners = self.owners_of(graph);
        }
        match owners.as_slice() {
            [] => Err(self.not_listed_error(graph)),
            [owner] => Ok((owner.clone(), graph.to_string())),
            many => Err(anyhow!(
                "graph id '{graph}' is ambiguous in this identity's list_graphs — listed under {}; \
                 sophia-mcp will not guess an owner",
                many.join(" and ")
            )),
        }
    }

    // -------------------------------------------------------------- activation

    /// One `POST …/activate` to start waking the cell. Records the poll URL
    /// (202) or the ready claim (200) for the next wait; a retryable 5xx or a
    /// request timeout is logged and left to the first use; the gateway's own
    /// 4xx is surfaced verbatim.
    async fn kick_activation(&self, session: &GraphSession) -> anyhow::Result<()> {
        let reply = match self
            .bounded(
                &format!("POST {}", session.activate_url),
                None,
                self.post_activate(session),
            )
            .await
        {
            Ok(reply) => reply,
            Err(e) if e.is::<RequestTimedOut>() => {
                session.observe(&format!("{e:#}"));
                tracing::warn!(graph = %session.label(), "activation kick timed out; first use will wait: {e:#}");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let body = reply.json();
        match reply.status {
            StatusCode::ACCEPTED => {
                let state = describe_activation(body.get("activation").unwrap_or(&Value::Null));
                if let Some(poll) = body.get("pollUrl").and_then(Value::as_str) {
                    *session.pending_poll.lock().expect("pending lock") =
                        Some(self.same_origin(poll)?);
                }
                session.observe(&state);
                tracing::info!(
                    graph = %session.label(),
                    state = %state,
                    "activation started; will wait for routability on first use"
                );
                Ok(())
            }
            s if s.is_success() => {
                session.kicked_ready.store(true, Ordering::SeqCst);
                session.observe(READY_CLAIM);
                tracing::info!(
                    graph = %session.label(),
                    "activate reports a running cell (ready:true) — not yet proven routable"
                );
                Ok(())
            }
            s if is_retryable(s, &body) => {
                let state = format!("activate answered HTTP {s}: {}", reply.body.trim());
                session.observe(&state);
                tracing::warn!(
                    graph = %session.label(),
                    state = %state,
                    "activation kick answered a retryable status; first use will wait"
                );
                Ok(())
            }
            s => Err(self.gateway_rejection(&session.label(), "activate", s, &reply.body)),
        }
    }

    /// Cached `initialize` result if the session is proven routable, else run
    /// the bounded wait — against the CALLER's deadline, including the time
    /// spent queued behind another waiter on the same session.
    async fn ensure_routable(
        &self,
        session: &Arc<GraphSession>,
        deadline: Instant,
        started: Instant,
    ) -> anyhow::Result<Value> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let mut guard = match tokio::time::timeout(remaining, session.init.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                let state = format!("(queued behind another waiter) {}", session.last_state());
                return Err(self.timeout_error(&session.label(), started, 0, "polls", &state));
            }
        };
        if let Some(init) = guard.as_ref() {
            return Ok(init.clone());
        }
        let init = self.wait_for_routable(session, deadline, started).await?;
        *guard = Some(init.clone());
        Ok(init)
    }

    /// The wait state machine: activate (at most once per wait) → poll
    /// `/activations/{id}` until terminal → prove with MCP `initialize` on the
    /// cell path → re-probe with backoff on 202/502/503, all bounded by the
    /// caller's deadline.
    async fn wait_for_routable(
        &self,
        session: &GraphSession,
        deadline: Instant,
        started: Instant,
    ) -> anyhow::Result<Value> {
        enum Step {
            Activate,
            Poll(String),
            Probe,
        }
        let label = session.label();
        let mut polls: u64 = 0;
        let mut last_state = session.last_state();
        let mut step = if let Some(url) = session.pending_poll.lock().expect("pending lock").take()
        {
            Step::Poll(url)
        } else if session.kicked_ready.swap(false, Ordering::SeqCst) {
            Step::Probe
        } else {
            Step::Activate
        };
        // The connect-time kick already POSTed activate for Poll/Probe starts.
        let mut activated = !matches!(step, Step::Activate);
        let poll = self.opts.activation_poll;
        let backoff_cap = poll * BACKOFF_CAP_MULTIPLIER;
        let mut backoff = poll;
        loop {
            session.observe(&last_state);
            if Instant::now() >= deadline {
                self.last_wait_polls.store(polls, Ordering::SeqCst);
                return Err(self.timeout_error(&label, started, polls, "polls", &last_state));
            }
            let sleep_for = match step {
                Step::Activate => {
                    activated = true;
                    polls += 1;
                    let what = format!("POST {}", session.activate_url);
                    match self
                        .bounded(&what, Some(deadline), self.post_activate(session))
                        .await
                    {
                        Ok(reply) => {
                            let body = reply.json();
                            match reply.status {
                                StatusCode::ACCEPTED => {
                                    last_state = describe_activation(
                                        body.get("activation").unwrap_or(&Value::Null),
                                    );
                                    match body.get("pollUrl").and_then(Value::as_str) {
                                        Some(url) => {
                                            step = Step::Poll(self.same_origin(url)?);
                                            continue;
                                        }
                                        None => {
                                            last_state.push_str(" (202 without pollUrl)");
                                            step = Step::Probe;
                                        }
                                    }
                                }
                                s if s.is_success() => {
                                    last_state = READY_CLAIM.to_string();
                                    step = Step::Probe;
                                    continue;
                                }
                                s if is_retryable(s, &body) => {
                                    last_state = format!(
                                        "activate answered HTTP {s}: {}",
                                        reply.body.trim()
                                    );
                                    step = Step::Probe;
                                }
                                s => {
                                    return Err(self.gateway_rejection(
                                        &label,
                                        "activate",
                                        s,
                                        &reply.body,
                                    ))
                                }
                            }
                        }
                        Err(e) if e.is::<RequestTimedOut>() => {
                            last_state = format!("{e:#}");
                            step = Step::Probe;
                        }
                        Err(e) => return Err(e),
                    }
                    tracing::info!(graph = %label, polls, state = %last_state, "waiting for activation");
                    poll
                }
                Step::Poll(ref url) => {
                    polls += 1;
                    let what = format!("GET {url}");
                    match self.bounded(&what, Some(deadline), self.get(url)).await {
                        Ok(reply) => {
                            let body = reply.json();
                            match reply.status {
                                s if s.is_success() => {
                                    last_state = describe_activation(&body);
                                    match body.get("phase").and_then(Value::as_str) {
                                        Some(PHASE_READY) => {
                                            tracing::info!(
                                                graph = %label,
                                                polls,
                                                "activation reached phase=ready; probing MCP"
                                            );
                                            step = Step::Probe;
                                            continue;
                                        }
                                        Some(PHASE_FAILED) => {
                                            self.last_wait_polls.store(polls, Ordering::SeqCst);
                                            session.observe(&last_state);
                                            return Err(anyhow!(
                                                "graph '{label}' activation failed after {:.1}s: {}",
                                                started.elapsed().as_secs_f64(),
                                                body.get("error")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("no error detail")
                                            ));
                                        }
                                        _ => {}
                                    }
                                }
                                s if is_retryable(s, &body) => {
                                    last_state = format!(
                                        "activation poll answered HTTP {s}: {}",
                                        reply.body.trim()
                                    );
                                }
                                s => {
                                    return Err(self.gateway_rejection(
                                        &label,
                                        "activation poll",
                                        s,
                                        &reply.body,
                                    ))
                                }
                            }
                        }
                        Err(e) if e.is::<RequestTimedOut>() => {
                            last_state = format!("{e:#}");
                        }
                        Err(e) => return Err(e),
                    }
                    tracing::info!(graph = %label, polls, state = %last_state, "waiting for activation");
                    poll
                }
                Step::Probe => {
                    polls += 1;
                    let params = self.client_init_params.lock().expect("init lock").clone();
                    let what = format!("POST {} (initialize)", session.mcp_url);
                    match self
                        .bounded(
                            &what,
                            Some(deadline),
                            post_rpc(&self.client, &session.mcp_url, method::INITIALIZE, params),
                        )
                        .await
                    {
                        Ok(reply) => match classify_cell_reply(reply) {
                            CellReply::Result(init) => {
                                self.last_wait_polls.store(polls, Ordering::SeqCst);
                                session.observe("routable (MCP initialize succeeded)");
                                tracing::info!(
                                    graph = %label,
                                    polls,
                                    elapsed_s = format!("{:.1}", started.elapsed().as_secs_f64()),
                                    "cell routable (MCP initialize succeeded)"
                                );
                                return Ok(init);
                            }
                            CellReply::RpcError { code, message } => {
                                self.last_wait_polls.store(polls, Ordering::SeqCst);
                                return Err(anyhow!(
                                    "cell for graph '{label}' answered initialize with JSON-RPC error {code}: {message}"
                                ));
                            }
                            CellReply::Activating { poll_url, state } => {
                                last_state = state;
                                if let Some(url) = poll_url {
                                    step = Step::Poll(self.same_origin(&url)?);
                                    continue;
                                }
                                if !activated {
                                    step = Step::Activate;
                                    continue;
                                }
                            }
                            CellReply::Unavailable { status, body } => {
                                last_state =
                                    format!("cell path answered HTTP {status}: {}", body.trim());
                                if !activated {
                                    step = Step::Activate;
                                    continue;
                                }
                            }
                            CellReply::Rejected { status, body } => {
                                self.last_wait_polls.store(polls, Ordering::SeqCst);
                                return Err(self.gateway_rejection(&label, "mcp", status, &body));
                            }
                        },
                        Err(e) if e.is::<RequestTimedOut>() => {
                            last_state = format!("{e:#}");
                        }
                        Err(e) => return Err(e),
                    }
                    tracing::info!(graph = %label, polls, state = %last_state, "cell not routable yet");
                    let this = backoff;
                    backoff = (backoff * 2).min(backoff_cap);
                    this
                }
            };
            session.observe(&last_state);
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(sleep_for.min(remaining)).await;
        }
    }

    fn timeout_error(
        &self,
        label: &str,
        started: Instant,
        count: u64,
        count_kind: &str,
        last_state: &str,
    ) -> anyhow::Error {
        anyhow!(
            "graph '{label}' is not routable after {:.1}s (activation budget {:.1}s, {count} {count_kind}); \
             last observed activation state: {last_state}",
            started.elapsed().as_secs_f64(),
            self.opts.activation_timeout.as_secs_f64()
        )
    }

    /// The gateway said no (403/404/400/…): surface status + body verbatim.
    /// The gateway is the policy-enforcement point; sophia-mcp does not
    /// reinterpret its verdicts.
    fn gateway_rejection(
        &self,
        label: &str,
        what: &str,
        status: StatusCode,
        body: &str,
    ) -> anyhow::Error {
        anyhow!(
            "gateway returned HTTP {status} for {what} of graph '{label}': {}",
            body.trim()
        )
    }

    /// Bound one request by the per-request ceiling and, when given, the
    /// caller's remaining activation budget.
    async fn bounded<T, F>(
        &self,
        what: &str,
        deadline: Option<Instant>,
        fut: F,
    ) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let mut limit = self.opts.request_timeout;
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RequestTimedOut {
                    what: what.to_string(),
                    after: Duration::ZERO,
                }
                .into());
            }
            limit = limit.min(remaining);
        }
        match tokio::time::timeout(limit, fut).await {
            Ok(result) => result,
            Err(_) => Err(RequestTimedOut {
                what: what.to_string(),
                after: limit,
            }
            .into()),
        }
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
        // Redirects are never followed (see `build_client`); name the target
        // so the refusal is legible, then let the 3xx surface as a rejection.
        let location = status.is_redirection().then(|| {
            http.headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("<no Location header>")
                .to_string()
        });
        let mut body = http.text().await.context("read activation poll body")?;
        if let Some(location) = location {
            body = format!("redirect to '{location}' not followed; {}", body.trim());
        }
        Ok(HttpReply { status, body })
    }

    /// A poll URL is followed only on the gateway's own origin: relative, or
    /// absolute under `self.base`. Anything else would carry the bearer +
    /// on-behalf-of headers off-origin, so it is refused.
    fn same_origin(&self, url: &str) -> anyhow::Result<String> {
        if url.starts_with('/') {
            return Ok(format!("{}{url}", self.base));
        }
        if url == self.base || url.starts_with(&format!("{}/", self.base)) {
            return Ok(url.to_string());
        }
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Ok(format!("{}/{url}", self.base));
        }
        Err(anyhow!(
            "refusing to follow off-origin pollUrl '{url}' (gateway base is {}); credentials stay on-origin",
            self.base
        ))
    }

    // ------------------------------------------------------------------- rpc

    async fn control_rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let what = format!("POST {} ({method})", self.control_url);
        let reply = self
            .bounded(&what, None, post_rpc(&self.client, &self.control_url, method, params))
            .await?;
        rpc_result(reply, method).with_context(|| format!("control plane {}", self.control_url))
    }

    /// One MCP call against a graph cell, with wait-for-routable before and
    /// retry-after-wait on any 202/502/503 answer, bounded by the activation
    /// budget measured from this call's start. The call itself is bounded by
    /// the per-request ceiling only (a legitimate long tool call is not an
    /// activation problem).
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
            self.ensure_routable(session, deadline, started).await?;
            let what = format!("POST {} ({method})", session.mcp_url);
            let reply = self
                .bounded(
                    &what,
                    None,
                    post_rpc(&self.client, &session.mcp_url, method, params.clone()),
                )
                .await?;
            let state = match classify_cell_reply(reply) {
                CellReply::Result(value) => return Ok(value),
                CellReply::RpcError { code, message } => {
                    return Err(anyhow!(
                        "backend JSON-RPC error {code} for {method}: {message}"
                    ))
                }
                CellReply::Rejected { status, body } => {
                    return Err(self.gateway_rejection(&session.label(), method, status, &body))
                }
                CellReply::Activating { poll_url, state } => {
                    if let Some(url) = poll_url {
                        *session.pending_poll.lock().expect("pending lock") =
                            Some(self.same_origin(&url)?);
                    }
                    state
                }
                CellReply::Unavailable { status, body } => {
                    format!("cell path answered HTTP {status}: {}", body.trim())
                }
            };
            session.invalidate().await;
            session.observe(&state);
            retries += 1;
            tracing::warn!(
                graph = %session.label(),
                method,
                retries,
                state = %state,
                "cell answered not-routable mid-session; re-waiting for activation"
            );
            if Instant::now() >= deadline {
                return Err(self.timeout_error(&session.label(), started, retries, "retries", &state));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(self.opts.activation_poll.min(remaining)).await;
        }
    }

    // --------------------------------------------------------------- routing

    /// Fetch both catalogs, merge them, and (re)build the routing table. A
    /// paginated request (`cursor` present) extends the table instead of
    /// replacing it; control tools are appended only on the last page.
    async fn refresh_catalog(&self, params: Value) -> anyhow::Result<Value> {
        let paged = params.get("cursor").is_some_and(|c| !c.is_null());
        let control = self.control_rpc(method::TOOLS_LIST, json!({})).await?;
        let session = self.session_for(&self.owner, &self.bound_graph);
        let cell = self.cell_rpc(&session, method::TOOLS_LIST, params).await?;
        let merged = merge_catalogs(&cell, &control, &self.bound_graph);
        tracing::info!(
            cell_tools = merged.cell_count,
            control_tools = merged.control_count,
            control_appended = merged.control_appended,
            paged,
            "merged tool catalog"
        );
        {
            let mut routing = self.routing.lock().expect("routing lock");
            if !paged {
                routing.clear();
            }
            routing.extend(merged.routes);
        }
        self.catalog_built.store(true, Ordering::SeqCst);
        Ok(merged.result)
    }

    async fn route_for(&self, name: &str) -> anyhow::Result<Option<Route>> {
        if !self.catalog_built.load(Ordering::SeqCst) {
            self.refresh_catalog(json!({})).await?;
        }
        Ok(self.routing.lock().expect("routing lock").get(name).cloned())
    }
}

/// `activate` answered 200 `ready:true`: a running pod, which per
/// CEL-AVAIL-001 is not proof of routability.
const READY_CLAIM: &str = "activate reports ready:true (a running pod — not yet proven routable)";

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

    async fn call_tool(&self, mut params: Value) -> anyhow::Result<Value> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("tools/call params.name must be a string"))?
            .to_string();
        match self.route_for(&name).await? {
            None => Err(ToolNotFound(name).into()),
            Some(Route::Control { upstream_name }) => {
                params["name"] = json!(upstream_name);
                self.control_rpc(method::TOOLS_CALL, params).await
            }
            Some(Route::Cell) => {
                // Refused before any request when the two spellings disagree.
                let requested = requested_graph(params.get("arguments"))?;
                let (owner, graph) = match requested.as_deref() {
                    None => (self.owner.clone(), self.bound_graph.clone()),
                    Some(g) if g == self.bound_graph => {
                        (self.owner.clone(), self.bound_graph.clone())
                    }
                    Some(g) => {
                        let target = self.resolve_sibling(g).await?;
                        tracing::info!(tool = %name, owner = %target.0, graph = %target.1, "routing cell tool to sibling graph");
                        target
                    }
                };
                // The cell sees exactly one graph argument, equal to the path.
                normalize_graph_arguments(&mut params, &graph);
                let session = self.session_for(&owner, &graph);
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
    request_timeout: Duration,
) -> anyhow::Result<(String, Vec<Value>)> {
    let primary = format!("{base}/control/mcp");
    let attempt = |url: String| async move {
        match tokio::time::timeout(request_timeout, control_list_graphs(client, &url)).await {
            Ok(result) => result,
            Err(_) => Err(ControlError::Other(
                RequestTimedOut {
                    what: format!("POST {url} (list_graphs)"),
                    after: request_timeout,
                }
                .into(),
            )),
        }
    };
    match attempt(primary.clone()).await {
        Ok(graphs) => Ok((primary, graphs)),
        Err(ControlError::NotFound(body)) if unified_fallback => {
            let unified = format!("{base}/mcp");
            tracing::warn!(
                primary = %primary,
                body = %body,
                "control plane 404 at /control/mcp; trying unified /mcp"
            );
            let graphs = attempt(unified.clone())
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

/// The graph named by the tool arguments via `graph_id` and/or `graphId`.
/// Both may be present only if they agree; a non-string value is refused;
/// `null` / blank counts as absent.
fn requested_graph(arguments: Option<&Value>) -> anyhow::Result<Option<String>> {
    let Some(args) = arguments.and_then(Value::as_object) else {
        return Ok(None);
    };
    let mut found: Option<(&str, String)> = None;
    for key in ["graph_id", "graphId"] {
        match args.get(key) {
            None | Some(Value::Null) => {}
            Some(Value::String(raw)) => {
                let value = raw.trim();
                if value.is_empty() {
                    continue;
                }
                match &found {
                    Some((prev_key, prev)) if prev != value => {
                        return Err(anyhow!(
                            "{prev_key} ('{prev}') and {key} ('{value}') disagree; name exactly one graph"
                        ));
                    }
                    _ => found = Some((key, value.to_string())),
                }
            }
            Some(other) => {
                return Err(anyhow!(
                    "{key} must be a string naming one of your listed graphs, got {other}"
                ));
            }
        }
    }
    Ok(found.map(|(_, value)| value))
}

/// Rewrite the call body so the target cell sees exactly one graph argument,
/// equal to the routed path: whichever spelling the agent used is kept (both
/// → `graph_id`) and set to `graph`; the other spelling is removed. A call
/// with neither spelling is left untouched (the cell answers about itself).
fn normalize_graph_arguments(params: &mut Value, graph: &str) {
    let Some(args) = params.get_mut("arguments").and_then(Value::as_object_mut) else {
        return;
    };
    let has_snake = args.contains_key("graph_id");
    let has_camel = args.contains_key("graphId");
    if has_snake {
        args.insert("graph_id".into(), json!(graph));
        args.remove("graphId");
    } else if has_camel {
        args.insert("graphId".into(), json!(graph));
    }
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
    cell_count: usize,
    control_count: usize,
    control_appended: bool,
}

/// Union of the cell catalog (first, augmented with `graph_id`/`graphId`) and
/// the control catalog (always under [`CONTROL_PREFIX`]). Control tools are
/// appended only when the cell list has no `nextCursor` (i.e. this is the
/// last page); their routes are registered regardless. `nextCursor` from the
/// cell list, if any, is preserved.
fn merge_catalogs(cell: &Value, control: &Value, bound_graph: &str) -> Merged {
    let mut tools: Vec<Value> = Vec::new();
    let mut routes: HashMap<String, Route> = HashMap::new();

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
    let next_cursor = cell.get("nextCursor").filter(|c| !c.is_null()).cloned();
    let control_appended = next_cursor.is_none();

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
        let exposed = format!("{CONTROL_PREFIX}{name}");
        tool["name"] = json!(exposed);
        routes.insert(
            exposed,
            Route::Control {
                upstream_name: name,
            },
        );
        if control_appended {
            tools.push(tool);
        }
        control_count += 1;
    }

    let mut result = json!({ "tools": tools });
    if let Some(cursor) = next_cursor {
        result["nextCursor"] = cursor;
    }
    Merged {
        result,
        routes,
        cell_count,
        control_count,
        control_appended,
    }
}

/// Add optional `graph_id` + `graphId` string properties to a cell tool's
/// input schema when absent, so the agent learns it can route the call to
/// another listed graph.
fn add_graph_argument(tool: &mut Value, bound_graph: &str) {
    let description = format!(
        "Optional. Route this call to one of your listed graphs instead of the bound graph \
         '{bound_graph}'. Naming a graph wakes it if it is dormant — a node may be provisioned \
         and the call may block for up to --activation-timeout (default 300 s) while it becomes \
         routable. Never creates a graph: the name must already be in list_graphs, and the \
         gateway's ACL decides. Use either graph_id or graphId, not both."
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
    fn merge_always_prefixes_control_tools_and_routes_by_name() {
        let cell = json!({ "tools": [
            { "name": "search_documents", "inputSchema": { "type": "object", "properties": {} } },
            { "name": "list_graphs", "inputSchema": { "type": "object" } }
        ]});
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
            vec!["search_documents", "list_graphs", "control_list_graphs", "control_create_graph"]
        );
        assert!(merged.control_appended);
        assert!(merged.result.get("nextCursor").is_none());
        assert!(matches!(merged.routes["search_documents"], Route::Cell));
        assert!(matches!(merged.routes["list_graphs"], Route::Cell));
        assert!(matches!(
            &merged.routes["control_list_graphs"],
            Route::Control { upstream_name } if upstream_name == "list_graphs"
        ));
        assert!(matches!(
            &merged.routes["control_create_graph"],
            Route::Control { upstream_name } if upstream_name == "create_graph"
        ));
        assert!(!merged.routes.contains_key("create_graph"), "no unprefixed control route");
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
    fn merge_with_a_cursor_defers_control_tools_but_still_routes_them() {
        let cell = json!({ "tools": [ { "name": "a" } ], "nextCursor": "c1" });
        let control = json!({ "tools": [ { "name": "list_graphs" } ] });
        let merged = merge_catalogs(&cell, &control, "notes");
        let names: Vec<&str> = merged.result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["a"]);
        assert!(!merged.control_appended);
        assert_eq!(merged.result["nextCursor"], json!("c1"));
        assert!(matches!(&merged.routes["control_list_graphs"], Route::Control { .. }));
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
        let added = tool["inputSchema"]["properties"]["graphId"]["description"]
            .as_str()
            .unwrap();
        assert!(added.contains("wakes it if it is dormant"), "{added}");
        assert!(added.contains("--activation-timeout"), "{added}");
        assert!(added.contains("Never creates"), "{added}");
    }

    #[test]
    fn requested_graph_reads_both_spellings_and_ignores_blank_or_null() {
        assert_eq!(
            requested_graph(Some(&json!({ "graph_id": "a" }))).unwrap(),
            Some("a".into())
        );
        assert_eq!(
            requested_graph(Some(&json!({ "graphId": " b " }))).unwrap(),
            Some("b".into())
        );
        assert_eq!(requested_graph(Some(&json!({ "graph_id": "" }))).unwrap(), None);
        assert_eq!(requested_graph(Some(&json!({ "graph_id": null }))).unwrap(), None);
        assert_eq!(requested_graph(None).unwrap(), None);
        assert_eq!(
            requested_graph(Some(&json!({ "graph_id": "a", "graphId": "a" }))).unwrap(),
            Some("a".into())
        );
    }

    #[test]
    fn requested_graph_refuses_disagreement_and_non_strings() {
        let err = requested_graph(Some(&json!({ "graph_id": "notes", "graphId": "scratch" })))
            .unwrap_err();
        assert!(format!("{err}").contains("disagree"), "{err}");
        let err = requested_graph(Some(&json!({ "graph_id": 7 }))).unwrap_err();
        assert!(format!("{err}").contains("must be a string"), "{err}");
    }

    #[test]
    fn normalize_leaves_exactly_one_argument_equal_to_the_path() {
        let mut p = json!({ "name": "t", "arguments": { "graph_id": "x", "graphId": "x", "q": 1 } });
        normalize_graph_arguments(&mut p, "x");
        assert_eq!(p["arguments"], json!({ "graph_id": "x", "q": 1 }));

        let mut p = json!({ "name": "t", "arguments": { "graphId": "" } });
        normalize_graph_arguments(&mut p, "bound");
        assert_eq!(p["arguments"], json!({ "graphId": "bound" }));

        let mut p = json!({ "name": "t", "arguments": { "q": 1 } });
        normalize_graph_arguments(&mut p, "bound");
        assert_eq!(p["arguments"], json!({ "q": 1 }));
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
