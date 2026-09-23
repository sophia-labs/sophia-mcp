//! Agent declaration and live, graph-defined MCP modes.
//!
//! This wrapper sits above every backend (hosted gateway, direct Garden MCP,
//! local gardend, composed sub-MCPs) whenever the process is bound to a graph.
//! Without a declared agent it is a pass-through that only adds the three
//! `sophia_agent_*` tools. A caller declares an agent at runtime with
//! `sophia_agent_declare` (or at start with `--agent-id`); from then on the
//! bound graph owns the mode definitions and the proxy owns the selection and
//! enforces it on both discovery and calls.
//!
//! Definitions are live: the agent's assignments and modes are re-read from
//! the graph's user RDF partition before `tools/list` and `tools/call` (cached
//! for a short TTL) and by a background poller, and every change to the
//! effective tool set emits `notifications/tools/list_changed`. A call is only
//! ever admitted against a definition read within the TTL; when the graph
//! cannot be read the call fails closed with a retryable error while the
//! control tools stay available. No mode bytes come from tool arguments.
//!
//! A declared agent is a claim made by the MCP caller. The underlying
//! credential and its gateway/Garden ACLs remain the authority ceiling;
//! binding agent identity to credentials is future work.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};

use super::{Backend, ToolNotFound};

const AGT: &str = "http://mnemosyne.dev/agent#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const MAX_MODE_ROWS: usize = 5000;
const MAX_CATALOG_PAGES: usize = 20;
const MAX_CATALOG_TOOLS: usize = 2000;

pub const AGENT_DECLARE: &str = "sophia_agent_declare";
pub const AGENT_STATUS: &str = "sophia_agent_status";
pub const AGENT_CLEAR: &str = "sophia_agent_clear";
pub const MODE_STATUS: &str = "sophia_mode_status";
pub const MODE_LIST: &str = "sophia_mode_list";
pub const MODE_SET: &str = "sophia_mode_set";

/// Refresh cadence for live mode definitions.
#[derive(Clone, Debug)]
pub struct ModeOptions {
    /// How long a successful read of the agent's modes counts as current.
    /// `tools/list` and `tools/call` re-read the graph once it is older.
    pub cache_ttl: Duration,
    /// Background re-read interval while an agent is declared, so edits show
    /// up (as `list_changed`) without a call. Zero disables the poller.
    pub poll_interval: Duration,
}

impl Default for ModeOptions {
    fn default() -> Self {
        Self {
            cache_ttl: Duration::from_secs(3),
            poll_interval: Duration::from_secs(15),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Mode {
    iri: String,
    label: String,
    access: String,
    allows: BTreeSet<String>,
    graphs: BTreeSet<String>,
    approvals: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct Catalog {
    default: Option<String>,
    permitted: BTreeSet<String>,
    modes: BTreeMap<String, Mode>,
}

#[derive(Debug, Default)]
struct Session {
    /// The declared agent. `None` = undeclared: full pass-through catalogue.
    agent: Option<String>,
    /// The selected mode definition as of the last successful read.
    active: Option<Mode>,
    /// The last successful read of the agent's modes.
    catalog: Option<Catalog>,
    fetched_at: Option<Instant>,
    last_error: Option<String>,
    /// Told to the caller on its next call (e.g. the active mode was revoked).
    notice: Option<String>,
}

/// What `tools/list` shows depends only on this; a change emits list_changed.
#[derive(PartialEq, Eq)]
struct Visible(bool, Option<Mode>);

impl Session {
    fn visible(&self) -> Visible {
        Visible(self.agent.is_some(), self.active.clone())
    }

    fn fresh(&self, ttl: Duration) -> bool {
        self.last_error.is_none() && self.fetched_at.is_some_and(|at| at.elapsed() < ttl)
    }

    /// Adopt a new read of the graph. The selection follows its live
    /// definition; a revoked selection falls back to the default mode (or to
    /// controls only), and an empty selection adopts the default.
    fn install(&mut self, catalog: Catalog) {
        self.fetched_at = Some(Instant::now());
        self.last_error = None;
        let default = catalog
            .default
            .as_ref()
            .and_then(|iri| catalog.modes.get(iri))
            .cloned();
        match self.active.take() {
            None => self.active = default,
            Some(old) => match catalog.modes.get(&old.iri) {
                Some(mode) if catalog.permitted.contains(&old.iri) => {
                    self.active = Some(mode.clone())
                }
                _ => {
                    let fallback = default
                        .as_ref()
                        .map_or_else(|| "controls only".to_owned(), |m| format!("<{}>", m.iri));
                    self.notice = Some(format!(
                        "mode <{}> was revoked or unassigned for {}; this MCP process fell back to {fallback}",
                        old.iri,
                        self.agent.as_deref().unwrap_or("the agent")
                    ));
                    self.active = default;
                }
            },
        }
        self.catalog = Some(catalog);
    }
}

/// The selection belongs to this MCP process (connection) only; restarting
/// forgets the declaration unless `--agent-id` presets it.
pub struct ModeBackend {
    inner: Arc<dyn Backend>,
    graph_id: String,
    options: ModeOptions,
    session: Mutex<Session>,
    notify_tx: mpsc::UnboundedSender<Value>,
    notify_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<Value>>>,
}

impl ModeBackend {
    /// Wrap `inner` for a graph-bound process. `preset_agent` is the optional
    /// `--agent-id`, equivalent to declaring it before the first request.
    pub fn new(
        inner: Arc<dyn Backend>,
        graph_id: &str,
        preset_agent: Option<&str>,
        options: ModeOptions,
    ) -> anyhow::Result<Arc<Self>> {
        if !valid_graph_id(graph_id) {
            bail!("--graph must be a simple graph id for MCP modes");
        }
        if preset_agent.is_some_and(|agent| !valid_agent_id(agent)) {
            bail!("--agent-id must be a canonical agent-<hex> id for MCP modes");
        }
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();
        let this = Arc::new(Self {
            inner,
            graph_id: graph_id.to_owned(),
            options,
            session: Mutex::new(Session {
                agent: preset_agent.map(str::to_owned),
                ..Session::default()
            }),
            notify_tx,
            notify_rx: std::sync::Mutex::new(Some(notify_rx)),
        });
        if !this.options.poll_interval.is_zero() {
            tokio::spawn(poll(Arc::downgrade(&this), this.options.poll_interval));
        }
        Ok(this)
    }

    fn query(&self, agent_id: &str) -> String {
        let graph = format!("urn:mnemosyne:local:graph:{}:user:rdf", self.graph_id);
        let agent = format!("urn:sophia:agent:{agent_id}");
        // Only the user RDF partition that the editor writes is authoritative.
        // An unrelated named graph may not inject an assignment or mode.
        format!(
            "SELECT ?rel ?mode ?p ?o WHERE {{\n  GRAPH <{graph}> {{\n    <{agent}> ?rel ?mode .\n    FILTER(?rel IN (<{AGT}defaultMode>, <{AGT}mayUseMode>))\n    OPTIONAL {{ ?mode ?p ?o . FILTER(?p IN (<{RDF_TYPE}>, <{RDFS_LABEL}>, <{AGT}access>, <{AGT}allowsTool>, <{AGT}graphScope>, <{AGT}requiresApproval>)) }}\n  }}\n}}\nLIMIT {}",
            MAX_MODE_ROWS + 1
        )
    }

    async fn fetch(&self, agent_id: &str) -> anyhow::Result<Catalog> {
        let result = self
            .inner
            .call_tool(json!({"name":"sparql_query", "arguments": {"graphId":self.graph_id, "query":self.query(agent_id)}}))
            .await
            .context("read agent mode catalog through the bound graph MCP")?;
        if result["isError"] == true {
            bail!("the graph MCP refused the agent mode catalog query");
        }
        parse_catalog(&result)
    }

    /// Re-read the declared agent's modes unless the last read is within the
    /// TTL. On failure the last good read is kept for discovery only; the
    /// error is returned so calls can fail closed.
    async fn ensure_current(&self, session: &mut Session) -> anyhow::Result<()> {
        let Some(agent) = session.agent.clone() else {
            return Ok(());
        };
        if session.fresh(self.options.cache_ttl) {
            return Ok(());
        }
        match self.fetch(&agent).await {
            Ok(catalog) => {
                session.install(catalog);
                Ok(())
            }
            Err(error) => {
                session.last_error = Some(format!("{error:#}"));
                Err(error)
            }
        }
    }

    fn notify_if_changed(&self, before: &Visible, session: &Session) {
        if *before != session.visible() {
            // The receiver lives as long as the stdio server; a send after it
            // stopped has nobody to tell.
            let _ = self
                .notify_tx
                .send(json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"}));
        }
    }

    async fn full_tools(&self) -> anyhow::Result<Vec<Value>> {
        let mut tools = Vec::new();
        let mut names = BTreeSet::new();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        for _ in 0..MAX_CATALOG_PAGES {
            let params = cursor
                .as_ref()
                .map_or_else(|| json!({}), |c| json!({"cursor": c}));
            let page = self.inner.list_tools(params).await?;
            let entries = page["tools"]
                .as_array()
                .context("MCP tools/list returned no tools array")?;
            for entry in entries {
                let name = entry["name"]
                    .as_str()
                    .context("MCP tool descriptor has no name")?;
                if !names.insert(name.to_owned()) {
                    bail!("MCP tools/list has duplicate tool '{name}'");
                }
                if is_control_tool(name) {
                    bail!("upstream MCP tool '{name}' conflicts with proxy mode control");
                }
                tools.push(entry.clone());
            }
            if tools.len() > MAX_CATALOG_TOOLS {
                bail!("MCP tool catalogue exceeds {MAX_CATALOG_TOOLS} tools");
            }
            match page["nextCursor"].as_str() {
                None => return Ok(tools),
                Some(next) if seen_cursors.insert(next.to_owned()) => {
                    cursor = Some(next.to_owned())
                }
                Some(_) => bail!("MCP tools/list repeated a cursor"),
            }
        }
        bail!("MCP tools/list exceeds {MAX_CATALOG_PAGES} pages")
    }

    fn agent_tools() -> Vec<Value> {
        let empty = json!({"type":"object","properties":{},"additionalProperties":false});
        vec![
            json!({"name":AGENT_DECLARE,"description":"Declare which agent this MCP connection acts as. Selects the agent's default mode from the bound graph and narrows the tool list to it (clients get notifications/tools/list_changed). Mode definitions are re-read live from the graph. The declaration is a claim by the caller; the credential's ACL stays the authority ceiling.","inputSchema":{"type":"object","properties":{"agentId":{"type":"string","description":"Canonical agent id, agent-<lowercase hex>"}},"required":["agentId"],"additionalProperties":false}}),
            json!({"name":AGENT_STATUS,"description":"Show the agent declared on this MCP connection (if any), its active mode, and whether the mode definitions were read from the graph recently.","inputSchema":empty}),
            json!({"name":AGENT_CLEAR,"description":"Forget the declared agent and return this MCP connection to the full tool catalogue.","inputSchema":empty}),
        ]
    }

    fn mode_tools() -> Vec<Value> {
        let empty = json!({"type":"object","properties":{},"additionalProperties":false});
        vec![
            json!({"name":MODE_STATUS,"description":"Show this MCP connection's selected mode and whether its graph definition is current.","inputSchema":empty}),
            json!({"name":MODE_LIST,"description":"List the modes the bound graph currently assigns to the declared agent. A mode switch affects this MCP connection only.","inputSchema":empty}),
            json!({"name":MODE_SET,"description":"Switch this MCP connection to one of the declared agent's assigned modes. Tool availability changes immediately.","inputSchema":{"type":"object","properties":{"modeIri":{"type":"string","description":"Exact urn:sophia:mode:{slug} IRI from sophia_mode_list"}},"required":["modeIri"],"additionalProperties":false}}),
        ]
    }

    fn tool_result(value: Value) -> Value {
        json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value})
    }

    fn status(&self, session: &Session, current: bool) -> Value {
        let active = session.active.as_ref();
        json!({
            "declared": session.agent.is_some(),
            "agentId": session.agent,
            "graphId": self.graph_id,
            "activeMode": active.map(|mode| &mode.iri),
            "label": active.map(|mode| &mode.label),
            "access": active.map(|mode| &mode.access),
            "current": current,
            "lastError": session.last_error,
            "cacheTtlMs": u64::try_from(self.options.cache_ttl.as_millis()).unwrap_or(u64::MAX),
        })
    }

    fn mode_summary(mode: &Mode) -> Value {
        json!({"iri":mode.iri,"label":mode.label,"access":mode.access,
            "allowsTools":mode.allows,"graphScopes":mode.graphs,"requiresApproval":mode.approvals})
    }

    fn assigned_modes(session: &Session) -> Vec<Value> {
        session.catalog.as_ref().map_or_else(Vec::new, |catalog| {
            catalog
                .modes
                .values()
                .filter(|mode| catalog.permitted.contains(&mode.iri))
                .map(Self::mode_summary)
                .collect()
        })
    }

    async fn declare(&self, params: &Value) -> anyhow::Result<Value> {
        let args = params["arguments"]
            .as_object()
            .context("sophia_agent_declare arguments must be an object")?;
        if args.len() != 1 {
            bail!("sophia_agent_declare accepts only agentId");
        }
        let agent = args
            .get("agentId")
            .and_then(Value::as_str)
            .context("sophia_agent_declare requires agentId")?;
        if !valid_agent_id(agent) {
            bail!("agentId must be a canonical agent-<lowercase hex> id");
        }
        let mut session = self.session.lock().await;
        // Read first: a failed read leaves the previous declaration intact.
        let catalog = self.fetch(agent).await.with_context(|| {
            format!(
                "could not read the modes of {agent} from graph '{}' (retryable)",
                self.graph_id
            )
        })?;
        let before = session.visible();
        *session = Session {
            agent: Some(agent.to_owned()),
            ..Session::default()
        };
        session.install(catalog);
        self.notify_if_changed(&before, &session);
        let mut result = self.status(&session, true);
        result["modes"] = json!(Self::assigned_modes(&session));
        if session.active.is_none() {
            result["note"] = json!("no default mode is assigned; only the agent and mode controls are available until sophia_mode_set selects one");
        }
        Ok(Self::tool_result(result))
    }

    async fn clear(&self) -> Value {
        let mut session = self.session.lock().await;
        let before = session.visible();
        let previous = session.agent.take();
        *session = Session::default();
        self.notify_if_changed(&before, &session);
        Self::tool_result(
            json!({"cleared": previous.is_some(), "previousAgentId": previous, "graphId": self.graph_id}),
        )
    }

    async fn agent_status(&self) -> Value {
        let mut session = self.session.lock().await;
        let before = session.visible();
        let current = session.agent.is_some() && self.ensure_current(&mut session).await.is_ok();
        self.notify_if_changed(&before, &session);
        Self::tool_result(self.status(&session, current))
    }

    async fn refresh_poll(&self) {
        let mut session = self.session.lock().await;
        let Some(agent) = session.agent.clone() else {
            return;
        };
        let before = session.visible();
        match self.fetch(&agent).await {
            Ok(catalog) => session.install(catalog),
            Err(error) => {
                tracing::warn!(agent, "background mode refresh failed: {error:#}");
                session.last_error = Some(format!("{error:#}"));
            }
        }
        self.notify_if_changed(&before, &session);
    }
}

async fn poll(this: Weak<ModeBackend>, every: Duration) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(this) = this.upgrade() else {
            return;
        };
        this.refresh_poll().await;
    }
}

#[async_trait]
impl Backend for ModeBackend {
    async fn initialize(&self, params: Value) -> anyhow::Result<Value> {
        let mut result = self.inner.initialize(params).await?;
        if !result.is_object() {
            result = json!({});
        }
        result["capabilities"]["tools"]["listChanged"] = json!(true);
        Ok(result)
    }

    async fn list_tools(&self, params: Value) -> anyhow::Result<Value> {
        let mut session = self.session.lock().await;
        let before = session.visible();
        let refreshed = self.ensure_current(&mut session).await;
        self.notify_if_changed(&before, &session);
        if session.agent.is_none() {
            drop(session);
            let mut page = self.inner.list_tools(params.clone()).await?;
            let tools = page["tools"]
                .as_array_mut()
                .context("MCP tools/list returned no tools array")?;
            if let Some(name) = tools
                .iter()
                .filter_map(|tool| tool["name"].as_str())
                .find(|name| is_control_tool(name))
            {
                bail!("upstream MCP tool '{name}' conflicts with proxy agent control");
            }
            if params.get("cursor").is_none_or(Value::is_null) {
                tools.extend(Self::agent_tools());
            }
            return Ok(page);
        }
        if let Err(error) = refreshed {
            if session.catalog.is_none() {
                return Err(error.context("reading the declared agent's modes (retryable)"));
            }
            tracing::warn!("listing tools from the last good mode read: {error:#}");
        }
        let active = session.active.clone();
        drop(session);
        let mut tools = Self::agent_tools();
        tools.extend(Self::mode_tools());
        if let Some(mode) = active {
            for tool in self.full_tools().await? {
                if mode_allows(&mode, &tool, &self.graph_id) {
                    tools.push(tool);
                }
            }
        }
        Ok(json!({"tools":tools}))
    }

    async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
        let name = params["name"]
            .as_str()
            .context("tools/call params.name must be a string")?;
        match name {
            AGENT_DECLARE => return self.declare(&params).await,
            AGENT_CLEAR => return Ok(self.clear().await),
            AGENT_STATUS => return Ok(self.agent_status().await),
            _ => {}
        }
        let mut session = self.session.lock().await;
        let Some(agent) = session.agent.clone() else {
            if is_control_tool(name) {
                bail!("no agent is declared on this MCP connection; call {AGENT_DECLARE} first");
            }
            drop(session);
            return self.inner.call_tool(params).await;
        };
        let before = session.visible();
        let refreshed = self.ensure_current(&mut session).await;
        self.notify_if_changed(&before, &session);

        if name == MODE_STATUS {
            let mut result = self.status(&session, refreshed.is_ok());
            if let Some(notice) = session.notice.take() {
                result["notice"] = json!(notice);
            }
            return Ok(Self::tool_result(result));
        }
        if name == MODE_LIST {
            let mut result = json!({
                "modes": Self::assigned_modes(&session),
                "activeMode": session.active.as_ref().map(|m| &m.iri),
                "current": refreshed.is_ok(),
            });
            if let Some(notice) = session.notice.take() {
                result["notice"] = json!(notice);
            }
            return Ok(Self::tool_result(result));
        }
        if let Err(error) = refreshed {
            return Err(anyhow!(
                "retryable: could not re-read the modes of {agent} from graph '{}', so '{name}' was denied (fail closed); agent and mode controls remain available: {error:#}",
                self.graph_id
            ));
        }
        if name == MODE_SET {
            let args = params["arguments"]
                .as_object()
                .context("mode_set arguments must be an object")?;
            if args.len() != 1 {
                bail!("mode_set accepts only modeIri");
            }
            let iri = args
                .get("modeIri")
                .and_then(Value::as_str)
                .context("mode_set requires modeIri")?;
            if !valid_mode_iri(iri) {
                bail!("modeIri must be urn:sophia:mode:<slug>");
            }
            let catalog = session.catalog.as_ref().context("no mode catalogue read")?;
            let mode = catalog
                .modes
                .get(iri)
                .filter(|_| catalog.permitted.contains(iri))
                .with_context(|| format!("mode <{iri}> is not assigned to {agent}"))?
                .clone();
            let changed = session.active.as_ref() != Some(&mode);
            session.active = Some(mode.clone());
            session.notice = None;
            self.notify_if_changed(&before, &session);
            return Ok(Self::tool_result(
                json!({"activeMode":iri,"changed":changed,"access":mode.access,"graphId":self.graph_id}),
            ));
        }
        if let Some(notice) = session.notice.take() {
            bail!("{notice}. '{name}' was not called; refresh tools/list and retry");
        }
        let mode = session
            .active
            .clone()
            .with_context(|| format!("{agent} has no active mode; select one with {MODE_SET}"))?;
        drop(session);
        let tools = self.full_tools().await?;
        let descriptor = tools
            .iter()
            .find(|tool| tool["name"] == name)
            .ok_or_else(|| ToolNotFound(name.to_owned()))?;
        if !mode_allows(&mode, descriptor, &self.graph_id) {
            bail!("mode <{}> denies tool '{name}'", mode.iri);
        }
        if mode.approvals.contains(name) {
            bail!("approval_required: tool '{name}' requires human approval in mode <{}>; no approval queue is configured", mode.iri);
        }
        let args = params["arguments"]
            .as_object()
            .context("tools/call arguments must be an object")?;
        for key in ["graph_id", "graphId"] {
            if let Some(target) = args.get(key) {
                let target = target.as_str().context("graph argument must be a string")?;
                if !mode.graphs.is_empty() && !mode.graphs.contains(target) {
                    bail!("mode <{}> denies graph '{target}'", mode.iri);
                }
            }
        }
        self.inner.call_tool(params).await
    }

    fn take_notifications(&self) -> Option<mpsc::UnboundedReceiver<Value>> {
        self.notify_rx.lock().ok()?.take()
    }
}

fn is_control_tool(name: &str) -> bool {
    matches!(
        name,
        AGENT_DECLARE | AGENT_STATUS | AGENT_CLEAR | MODE_STATUS | MODE_LIST | MODE_SET
    )
}

fn mode_allows(mode: &Mode, tool: &Value, bound_graph: &str) -> bool {
    let Some(name) = tool["name"].as_str() else {
        return false;
    };
    mode.allows.contains(name)
        // A scoped mode can only admit tools from a graph-aware catalogue.
        // Composed or control tools without scope metadata may route elsewhere
        // through arguments this proxy does not understand.
        && (mode.graphs.is_empty()
            || (mode.graphs.contains(bound_graph)
                && tool["_meta"]["sophia.local.requiredScopes"].is_array()))
        && (mode.access == "write" || read_effect(tool))
}

fn read_effect(tool: &Value) -> bool {
    let Some(scopes) = tool["_meta"]["sophia.local.requiredScopes"].as_array() else {
        return false;
    };
    !scopes.is_empty()
        && scopes.iter().all(|scope| {
            scope
                .as_str()
                .is_some_and(|s| s.ends_with(".read") || matches!(s, "rdf.query" | "rdf.dump"))
        })
}

fn valid_graph_id(value: &str) -> bool {
    value.len() <= 128
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}
fn valid_agent_id(value: &str) -> bool {
    value.strip_prefix("agent-").is_some_and(|tail| {
        !tail.is_empty()
            && tail
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn valid_mode_iri(value: &str) -> bool {
    value.strip_prefix("urn:sophia:mode:").is_some_and(|tail| {
        !tail.is_empty()
            && tail.len() <= 64
            && (tail.as_bytes()[0].is_ascii_lowercase() || tail.as_bytes()[0].is_ascii_digit())
            && tail
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    })
}

fn parse_catalog(result: &Value) -> anyhow::Result<Catalog> {
    let body = if result["structuredContent"].is_object() {
        &result["structuredContent"]
    } else if result["rows"].is_array() {
        result
    } else {
        bail!("mode catalog query returned no structured solutions")
    };
    if body["resultType"]
        .as_str()
        .is_some_and(|kind| kind != "solutions")
    {
        bail!("mode catalog query did not return solutions");
    }
    if body["warnings"]
        .as_array()
        .is_some_and(|warnings| !warnings.is_empty())
    {
        bail!("mode catalog query returned warnings; refusing a possibly incomplete result");
    }
    let rows = body["rows"]
        .as_array()
        .context("mode catalog query returned no rows")?;
    if rows.len() >= MAX_MODE_ROWS {
        bail!("mode catalog query reached its row limit");
    }
    let mut defaults = BTreeSet::new();
    let mut permitted = BTreeSet::new();
    let mut props: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> = BTreeMap::new();
    for row in rows {
        let rel = iri_term(&row["rel"]).context("malformed mode relation")?;
        let iri = iri_term(&row["mode"]).context("malformed mode IRI")?;
        if !valid_mode_iri(&iri) {
            bail!("invalid mode IRI <{iri}>");
        }
        if rel == format!("{AGT}defaultMode") {
            defaults.insert(iri.clone());
        } else if rel != format!("{AGT}mayUseMode") {
            bail!("unexpected agent mode relation <{rel}>");
        }
        permitted.insert(iri.clone());
        if row["p"].is_null() || row["o"].is_null() {
            continue;
        }
        let p = iri_term(&row["p"]).context("malformed mode predicate")?;
        let value = if p == RDF_TYPE || p == format!("{AGT}graphScope") {
            iri_term(&row["o"]).context("malformed mode IRI property")?
        } else {
            literal_term(&row["o"]).context("malformed mode literal property")?
        };
        props
            .entry(iri)
            .or_default()
            .entry(p)
            .or_default()
            .insert(value);
    }
    if defaults.len() > 1 {
        bail!("agent declares more than one default mode");
    }
    let mut modes = BTreeMap::new();
    for iri in &permitted {
        let p = props.get(iri).context("assigned mode has no definition")?;
        if !p
            .get(RDF_TYPE)
            .is_some_and(|types| types.contains(&format!("{AGT}Mode")))
        {
            bail!("assigned mode <{iri}> is not agt:Mode");
        }
        let access = match p.get(&format!("{AGT}access")) {
            Some(values) if values.len() == 1 && values.contains("write") => "write",
            _ => "read",
        };
        let allows = p
            .get(&format!("{AGT}allowsTool"))
            .cloned()
            .unwrap_or_default();
        let approvals = p
            .get(&format!("{AGT}requiresApproval"))
            .cloned()
            .unwrap_or_default();
        if !allows
            .iter()
            .chain(approvals.iter())
            .all(|name| valid_tool_name(name))
        {
            bail!("mode <{iri}> contains a non-exact tool name");
        }
        if !approvals.is_subset(&allows) {
            bail!("mode <{iri}> requires approval for a tool it does not allow");
        }
        let graphs = p
            .get(&format!("{AGT}graphScope"))
            .into_iter()
            .flat_map(|v| v.iter())
            .map(|scope| {
                let graph = scope
                    .strip_prefix("urn:graph:")
                    .context("invalid mode graph scope")?;
                if !valid_graph_id(graph) {
                    bail!("invalid mode graph scope");
                }
                Ok(graph.to_owned())
            })
            .collect::<anyhow::Result<BTreeSet<_>>>()?;
        let label = p
            .get(RDFS_LABEL)
            .and_then(|v| v.iter().next())
            .cloned()
            .unwrap_or_default();
        modes.insert(
            iri.clone(),
            Mode {
                iri: iri.clone(),
                label,
                access: access.to_owned(),
                allows,
                graphs,
                approvals,
            },
        );
    }
    Ok(Catalog {
        default: defaults.into_iter().next(),
        permitted,
        modes,
    })
}

fn valid_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

fn iri_term(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        if s.starts_with('<') && s.ends_with('>') {
            return Some(s[1..s.len() - 1].to_owned());
        }
        if s.starts_with("urn:") || s.starts_with("http://") || s.starts_with("https://") {
            return Some(s.to_owned());
        }
    }
    if matches!(value["type"].as_str(), Some("iri" | "uri")) {
        return value["value"].as_str().map(str::to_owned);
    }
    None
}

fn literal_term(value: &Value) -> Option<String> {
    if value["type"] == "literal" {
        return value["value"].as_str().map(str::to_owned);
    }
    let s = value.as_str()?;
    if !s.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (index, ch) in s.char_indices().skip(1) {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '"' {
            let suffix = &s[index + 1..];
            if !suffix.is_empty() && !suffix.starts_with('@') && !suffix.starts_with("^^<") {
                return None;
            }
            return serde_json::from_str(&s[..=index]).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    const AGENT: &str = "agent-deadbeef";
    const READER: &str = "urn:sophia:mode:reader";
    const WRITER: &str = "urn:sophia:mode:writer";

    /// A Garden-shaped cell whose mode RDF can be edited between requests.
    struct Cell {
        revoked: AtomicBool,
        expanded: AtomicBool,
        duplicate: AtomicBool,
        unreadable: AtomicBool,
        calls: Mutex<Vec<String>>,
    }

    impl Cell {
        fn new() -> Self {
            Self {
                revoked: AtomicBool::new(false),
                expanded: AtomicBool::new(false),
                duplicate: AtomicBool::new(false),
                unreadable: AtomicBool::new(false),
                calls: Mutex::new(Vec::new()),
            }
        }
        async fn forwarded(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }
    }

    fn row(rel: &str, mode: &str, predicate: &str, value: &str, iri: bool) -> Value {
        json!({"rel":format!("<{AGT}{rel}>"),"mode":format!("<{mode}>"),
            "p":format!("<{predicate}>"),"o":if iri {format!("<{value}>")} else {json!(value).to_string()}})
    }

    fn rows(revoked: bool, expanded: bool) -> Vec<Value> {
        let mut result = Vec::new();
        let mut definitions = vec![
            (
                "reader",
                "read",
                vec!["read_document", "write_document", "mystery"],
                vec![],
            ),
            (
                "writer",
                "write",
                vec!["read_document", "write_document", "mystery"],
                vec!["write_document"],
            ),
        ];
        if expanded {
            definitions.push((
                "admin",
                "write",
                vec!["read_document", "write_document"],
                vec![],
            ));
        }
        for (slug, access, tools, approvals) in definitions {
            if revoked && slug == "writer" {
                continue;
            }
            let mode = format!("urn:sophia:mode:{slug}");
            let rel = if slug == "reader" {
                "defaultMode"
            } else {
                "mayUseMode"
            };
            result.push(row(rel, &mode, RDF_TYPE, &format!("{AGT}Mode"), true));
            result.push(row(rel, &mode, RDFS_LABEL, slug, false));
            result.push(row(rel, &mode, &format!("{AGT}access"), access, false));
            result.push(row(
                rel,
                &mode,
                &format!("{AGT}graphScope"),
                "urn:graph:lab",
                true,
            ));
            for tool in tools {
                result.push(row(rel, &mode, &format!("{AGT}allowsTool"), tool, false));
            }
            for tool in approvals {
                result.push(row(
                    rel,
                    &mode,
                    &format!("{AGT}requiresApproval"),
                    tool,
                    false,
                ));
            }
        }
        result
    }

    #[async_trait]
    impl Backend for Cell {
        async fn initialize(&self, _params: Value) -> anyhow::Result<Value> {
            Ok(json!({"capabilities":{"tools":{}}}))
        }
        async fn list_tools(&self, _params: Value) -> anyhow::Result<Value> {
            let mut tools = vec![
                json!({"name":"read_document","_meta":{"sophia.local.requiredScopes":["documents.read"]}}),
                json!({"name":"write_document","_meta":{"sophia.local.requiredScopes":["documents.write"]}}),
                json!({"name":"mystery"}),
            ];
            if self.duplicate.load(Ordering::SeqCst) {
                tools.push(json!({"name":"read_document","_meta":{"sophia.local.requiredScopes":["documents.write"]}}));
            }
            Ok(json!({"tools":tools}))
        }
        async fn call_tool(&self, params: Value) -> anyhow::Result<Value> {
            let name = params["name"].as_str().unwrap_or_default();
            if name == "sparql_query" {
                if self.unreadable.load(Ordering::SeqCst) {
                    bail!("upstream HTTP 503");
                }
                let query = params["arguments"]["query"].as_str().unwrap_or_default();
                assert!(query.contains("GRAPH <urn:mnemosyne:local:graph:lab:user:rdf>"));
                assert!(query.contains("<urn:sophia:agent:agent-deadbeef>"));
                return Ok(
                    json!({"structuredContent":{"resultType":"solutions","rows":rows(self.revoked.load(Ordering::SeqCst), self.expanded.load(Ordering::SeqCst))}}),
                );
            }
            self.calls.lock().await.push(name.to_owned());
            Ok(json!({"structuredContent":{"forwarded":name}}))
        }
    }

    fn live() -> ModeOptions {
        ModeOptions {
            cache_ttl: Duration::ZERO,
            poll_interval: Duration::ZERO,
        }
    }

    fn names(result: Value) -> Vec<String> {
        result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect()
    }
    fn call(name: &str, arguments: Value) -> Value {
        json!({"name":name,"arguments":arguments})
    }
    fn controls() -> Vec<&'static str> {
        vec![
            AGENT_DECLARE,
            AGENT_STATUS,
            AGENT_CLEAR,
            MODE_STATUS,
            MODE_LIST,
            MODE_SET,
        ]
    }
    fn with(extra: &[&'static str]) -> Vec<&'static str> {
        let mut all = controls();
        all.extend_from_slice(extra);
        all
    }
    fn drain(rx: &mut mpsc::UnboundedReceiver<Value>) -> usize {
        let mut count = 0;
        while let Ok(note) = rx.try_recv() {
            assert_eq!(note["method"], "notifications/tools/list_changed");
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn undeclared_is_a_pass_through_plus_agent_tools() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", None, live()).unwrap();
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            vec![
                "read_document",
                "write_document",
                "mystery",
                AGENT_DECLARE,
                AGENT_STATUS,
                AGENT_CLEAR
            ]
        );
        proxy.call_tool(call("mystery", json!({}))).await.unwrap();
        assert!(proxy
            .call_tool(call(MODE_SET, json!({"modeIri":READER})))
            .await
            .is_err());
        assert_eq!(cell.forwarded().await, vec!["mystery"]);
        assert!(proxy
            .call_tool(call(AGENT_DECLARE, json!({"agentId":"scout"})))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn declare_switch_and_clear_change_discovery_and_calls() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", None, live()).unwrap();
        let mut notes = proxy.take_notifications().unwrap();
        assert_eq!(
            proxy.initialize(json!({})).await.unwrap()["capabilities"]["tools"]["listChanged"],
            true
        );
        let declared = proxy
            .call_tool(call(AGENT_DECLARE, json!({"agentId":AGENT})))
            .await
            .unwrap();
        assert_eq!(declared["structuredContent"]["activeMode"], READER);
        assert_eq!(drain(&mut notes), 1);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            with(&["read_document"])
        );
        assert!(proxy
            .call_tool(call("write_document", json!({})))
            .await
            .unwrap_err()
            .to_string()
            .contains("denies"));
        assert!(proxy.call_tool(call("mystery", json!({}))).await.is_err());
        assert!(proxy
            .call_tool(call("not_listed", json!({})))
            .await
            .is_err());
        assert!(cell.forwarded().await.is_empty());

        assert!(proxy
            .call_tool(call(MODE_SET, json!({"modeIri":"urn:sophia:mode:other"})))
            .await
            .is_err());
        let switched = proxy
            .call_tool(call(MODE_SET, json!({"modeIri":WRITER})))
            .await
            .unwrap();
        assert_eq!(switched["structuredContent"]["changed"], true);
        assert_eq!(drain(&mut notes), 1);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            with(&["read_document", "write_document"])
        );
        assert!(proxy
            .call_tool(call("write_document", json!({})))
            .await
            .unwrap_err()
            .to_string()
            .contains("approval_required"));
        assert!(proxy
            .call_tool(call("read_document", json!({"graphId":"elsewhere"})))
            .await
            .is_err());
        proxy
            .call_tool(call("read_document", json!({"graphId":"lab"})))
            .await
            .unwrap();
        assert_eq!(cell.forwarded().await, vec!["read_document"]);

        proxy.call_tool(call(AGENT_CLEAR, json!({}))).await.unwrap();
        assert_eq!(drain(&mut notes), 1);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()).len(),
            6,
            "full catalogue again"
        );
    }

    #[tokio::test]
    async fn revoking_the_active_mode_falls_back_to_the_default_with_a_notice() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", Some(AGENT), live()).unwrap();
        let mut notes = proxy.take_notifications().unwrap();
        proxy
            .call_tool(call(MODE_SET, json!({"modeIri":WRITER})))
            .await
            .unwrap();
        drain(&mut notes);
        cell.revoked.store(true, Ordering::SeqCst);
        let refused = proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("revoked") && refused.contains(READER),
            "{refused}"
        );
        assert_eq!(drain(&mut notes), 1);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            with(&["read_document"])
        );
        proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap();
        assert!(proxy
            .call_tool(call(MODE_SET, json!({"modeIri":WRITER})))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn graph_edits_are_live_and_new_modes_selectable_immediately() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", Some(AGENT), live()).unwrap();
        proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap();
        cell.expanded.store(true, Ordering::SeqCst);
        assert_eq!(
            proxy.call_tool(call(MODE_LIST, json!({}))).await.unwrap()["structuredContent"]
                ["modes"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        proxy
            .call_tool(call(MODE_SET, json!({"modeIri":"urn:sophia:mode:admin"})))
            .await
            .unwrap();
        proxy
            .call_tool(call("write_document", json!({})))
            .await
            .unwrap();
        assert_eq!(cell.forwarded().await, vec!["write_document"]);
    }

    #[tokio::test]
    async fn an_unreadable_graph_fails_calls_closed_but_keeps_controls() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", Some(AGENT), live()).unwrap();
        proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap();
        cell.unreadable.store(true, Ordering::SeqCst);
        let refused = proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("retryable"), "{refused}");
        let status = proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap();
        assert_eq!(status["structuredContent"]["current"], false);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            with(&["read_document"]),
            "discovery keeps the last good read"
        );
        cell.unreadable.store(false, Ordering::SeqCst);
        proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap();
        assert_eq!(
            cell.forwarded().await,
            vec!["read_document", "read_document"]
        );
    }

    #[tokio::test]
    async fn conflicting_tool_descriptors_fail_before_any_call_is_forwarded() {
        let cell = Arc::new(Cell::new());
        cell.duplicate.store(true, Ordering::SeqCst);
        let proxy = ModeBackend::new(cell.clone(), "lab", Some(AGENT), live()).unwrap();
        assert!(proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        assert!(cell.forwarded().await.is_empty());
    }

    #[test]
    fn parser_rejects_incomplete_or_invented_mode_authority() {
        assert!(!valid_agent_id("scout"));
        assert!(!valid_agent_id("agent-DEADBEEF"));
        assert!(!valid_mode_iri("urn:sophia:mode:a>"));
        assert!(valid_mode_iri(READER));
        let mut incomplete = rows(false, false);
        incomplete.retain(|row| row["o"] != format!("<{AGT}Mode>"));
        assert!(parse_catalog(&json!({"structuredContent":{"rows":incomplete}})).is_err());
        assert!(parse_catalog(
            &json!({"structuredContent":{"rows":rows(false, false),"warnings":["truncated"]}})
        )
        .is_err());
    }
}
