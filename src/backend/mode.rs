//! Process-scoped MCP modes. This wrapper works above the hosted gateway,
//! direct Garden MCP, local gardend, and composed sub-MCPs. The backing graph
//! owns mode definitions; the proxy owns the current selection and enforces
//! it on both discovery and calls. No mode bytes come from tool arguments.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use super::{Backend, ToolNotFound};

const AGT: &str = "http://mnemosyne.dev/agent#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const MAX_MODE_ROWS: usize = 5000;
const MAX_CATALOG_PAGES: usize = 20;
const MAX_CATALOG_TOOLS: usize = 2000;

pub const MODE_STATUS: &str = "sophia_mode_status";
pub const MODE_LIST: &str = "sophia_mode_list";
pub const MODE_SET: &str = "sophia_mode_set";

#[derive(Clone, Debug, PartialEq, Eq)]
struct Mode {
    iri: String,
    label: String,
    access: String,
    allows: BTreeSet<String>,
    graphs: BTreeSet<String>,
    approvals: BTreeSet<String>,
}

#[derive(Debug)]
struct Catalog {
    default: Option<String>,
    permitted: BTreeSet<String>,
    modes: BTreeMap<String, Mode>,
}

#[derive(Debug, Default)]
struct Selection {
    initialized: bool,
    active: Option<Mode>,
    /// The process's initial authority envelope. Graph edits can revoke or
    /// invalidate it, but cannot add a mode or widen a mode until restart.
    pinned_modes: BTreeMap<String, Mode>,
}

/// A mode switch affects this MCP process only. Restarting selects the graph's
/// default again. The lock serializes selection with calls, so a successful
/// switch cannot race a call under the old tool set.
pub struct ModeBackend {
    inner: Arc<dyn Backend>,
    graph_id: String,
    agent_id: String,
    selection: Mutex<Selection>,
}

impl ModeBackend {
    pub fn new(inner: Arc<dyn Backend>, graph_id: &str, agent_id: &str) -> anyhow::Result<Self> {
        if !valid_graph_id(graph_id) {
            bail!("--graph must be a simple graph id for MCP modes");
        }
        if !valid_agent_id(agent_id) {
            bail!("--agent-id must be a canonical agent-<hex> id for MCP modes");
        }
        Ok(Self {
            inner,
            graph_id: graph_id.to_owned(),
            agent_id: agent_id.to_owned(),
            selection: Mutex::new(Selection::default()),
        })
    }

    fn query(&self) -> String {
        let graph = format!("urn:mnemosyne:local:graph:{}:user:rdf", self.graph_id);
        let agent = format!("urn:sophia:agent:{}", self.agent_id);
        // Only the user RDF partition that the editor writes is authoritative.
        // An unrelated named graph may not inject an assignment or mode.
        format!(
            "SELECT ?rel ?mode ?p ?o WHERE {{\n  GRAPH <{graph}> {{\n    <{agent}> ?rel ?mode .\n    FILTER(?rel IN (<{AGT}defaultMode>, <{AGT}mayUseMode>))\n    OPTIONAL {{ ?mode ?p ?o . FILTER(?p IN (<{RDF_TYPE}>, <{RDFS_LABEL}>, <{AGT}access>, <{AGT}allowsTool>, <{AGT}graphScope>, <{AGT}requiresApproval>)) }}\n  }}\n}}\nLIMIT {}",
            MAX_MODE_ROWS + 1
        )
    }

    async fn catalog(&self) -> anyhow::Result<Catalog> {
        let result = self
            .inner
            .call_tool(json!({"name":"sparql_query", "arguments": {"graphId":self.graph_id, "query":self.query()}}))
            .await
            .context("read agent mode catalog through the bound graph MCP")?;
        if result["isError"] == true {
            bail!("the graph MCP refused the agent mode catalog query");
        }
        parse_catalog(&result)
    }

    fn active<'a>(
        &self,
        state: &'a mut Selection,
        catalog: &Catalog,
    ) -> anyhow::Result<Option<&'a Mode>> {
        if !state.initialized {
            state.initialized = true;
            state.pinned_modes = catalog.modes.clone();
            state.active = catalog
                .default
                .as_ref()
                .and_then(|iri| catalog.modes.get(iri))
                .cloned();
        }
        let Some(active) = &state.active else {
            return Ok(None);
        };
        if !catalog.permitted.contains(&active.iri)
            || state.pinned_modes.get(&active.iri) != Some(active)
            || catalog.modes.get(&active.iri) != Some(active)
        {
            // Preserve the previous selection as evidence, but never use it
            // after an assignment was revoked or the definition was edited.
            return Err(anyhow!(
                "active mode changed or was revoked; select an assigned mode with {MODE_SET}"
            ));
        }
        Ok(state.active.as_ref())
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

    fn control_tools() -> Vec<Value> {
        vec![
            json!({"name":MODE_STATUS,"description":"Show this MCP process's selected mode and whether its graph assignment is still current.","inputSchema":{"type":"object","properties":{},"additionalProperties":false}}),
            json!({"name":MODE_LIST,"description":"List modes assigned to this agent in the bound graph. A mode switch affects this MCP process only.","inputSchema":{"type":"object","properties":{},"additionalProperties":false}}),
            json!({"name":MODE_SET,"description":"Switch this MCP process to one of this agent's assigned modes. Tool availability changes immediately.","inputSchema":{"type":"object","properties":{"modeIri":{"type":"string","description":"Exact urn:sophia:mode:{slug} IRI from sophia_mode_list"}},"required":["modeIri"],"additionalProperties":false}}),
        ]
    }

    fn tool_result(value: Value) -> Value {
        json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value})
    }

    fn summary(&self, active: Option<&Mode>, current: bool) -> Value {
        json!({
            "agentId": self.agent_id,
            "graphId": self.graph_id,
            "activeMode": active.map(|mode| &mode.iri),
            "label": active.map(|mode| &mode.label),
            "access": active.map(|mode| &mode.access),
            "current": current,
        })
    }

    fn mode_summary(mode: &Mode) -> Value {
        json!({"iri":mode.iri,"label":mode.label,"access":mode.access,
            "allowsTools":mode.allows,"graphScopes":mode.graphs,"requiresApproval":mode.approvals})
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

    async fn list_tools(&self, _params: Value) -> anyhow::Result<Value> {
        let mut state = self.selection.lock().await;
        let catalog = self.catalog().await?;
        let active = self.active(&mut state, &catalog).ok().flatten();
        let mut tools = Self::control_tools();
        if let Some(mode) = active {
            let upstream = self.full_tools().await?;
            for tool in upstream {
                if mode_allows(mode, &tool, &self.graph_id) {
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
        let mut state = self.selection.lock().await;
        let catalog = self.catalog().await?;
        self.active(&mut state, &catalog).ok();
        if name == MODE_LIST {
            let modes: Vec<Value> = state
                .pinned_modes
                .iter()
                .filter(|(iri, mode)| {
                    catalog.permitted.contains(*iri) && catalog.modes.get(*iri) == Some(*mode)
                })
                .map(|(_, mode)| Self::mode_summary(mode))
                .collect();
            let current = matches!(self.active(&mut state, &catalog), Ok(Some(_)));
            return Ok(Self::tool_result(
                json!({"modes":modes,"activeMode":state.active.as_ref().map(|m| &m.iri),"current":current}),
            ));
        }
        if name == MODE_STATUS {
            let current = matches!(self.active(&mut state, &catalog), Ok(Some(_)));
            return Ok(Self::tool_result(
                self.summary(state.active.as_ref(), current),
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
            let mode = state
                .pinned_modes
                .get(iri)
                .context("mode was not assigned when this MCP process started")?
                .clone();
            if !catalog.permitted.contains(iri) || catalog.modes.get(iri) != Some(&mode) {
                bail!("mode <{iri}> changed or is no longer assigned to this agent");
            }
            let changed = state.active.as_ref() != Some(&mode);
            state.initialized = true;
            state.active = Some(mode.clone());
            return Ok(Self::tool_result(
                json!({"activeMode":iri,"changed":changed,"access":mode.access,"graphId":self.graph_id}),
            ));
        }
        let mode = self
            .active(&mut state, &catalog)?
            .context("no active mode; select one with sophia_mode_set")?;
        let tools = self.full_tools().await?;
        let descriptor = tools
            .iter()
            .find(|tool| tool["name"] == name)
            .ok_or_else(|| ToolNotFound(name.to_owned()))?;
        if !mode_allows(mode, descriptor, &self.graph_id) {
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
}

fn is_control_tool(name: &str) -> bool {
    matches!(name, MODE_STATUS | MODE_LIST | MODE_SET)
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

    struct Cell {
        revoked: AtomicBool,
        expanded: AtomicBool,
        duplicate: AtomicBool,
        calls: Mutex<Vec<String>>,
    }

    impl Cell {
        fn new() -> Self {
            Self {
                revoked: AtomicBool::new(false),
                expanded: AtomicBool::new(false),
                duplicate: AtomicBool::new(false),
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
            if revoked && slug == "reader" {
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

    #[tokio::test]
    async fn mode_switch_changes_discovery_and_call_enforcement() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", AGENT).unwrap();
        assert_eq!(
            proxy.initialize(json!({})).await.unwrap()["capabilities"]["tools"]["listChanged"],
            true
        );
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            vec![MODE_STATUS, MODE_LIST, MODE_SET, "read_document"]
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

        let choices = proxy.call_tool(call(MODE_LIST, json!({}))).await.unwrap();
        assert_eq!(
            choices["structuredContent"]["modes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(proxy
            .call_tool(call(MODE_SET, json!({"modeIri":"urn:sophia:mode:other"})))
            .await
            .is_err());
        let switched = proxy
            .call_tool(call(MODE_SET, json!({"modeIri":WRITER})))
            .await
            .unwrap();
        assert_eq!(switched["structuredContent"]["changed"], true);
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            vec![
                MODE_STATUS,
                MODE_LIST,
                MODE_SET,
                "read_document",
                "write_document",
            ]
        );
        assert!(proxy
            .call_tool(call("write_document", json!({})))
            .await
            .unwrap_err()
            .to_string()
            .contains("approval_required"));
        assert!(proxy.call_tool(call("mystery", json!({}))).await.is_err());
        assert!(proxy
            .call_tool(call("read_document", json!({"graphId":"elsewhere"})))
            .await
            .is_err());
        proxy
            .call_tool(call("read_document", json!({"graphId":"lab"})))
            .await
            .unwrap();
        assert_eq!(cell.forwarded().await, vec!["read_document"]);
    }

    #[tokio::test]
    async fn revoked_selection_fails_closed_but_can_switch_to_remaining_mode() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", AGENT).unwrap();
        proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap();
        cell.revoked.store(true, Ordering::SeqCst);
        assert!(proxy
            .call_tool(call("read_document", json!({})))
            .await
            .unwrap_err()
            .to_string()
            .contains("revoked"));
        assert_eq!(
            names(proxy.list_tools(json!({})).await.unwrap()),
            vec![MODE_STATUS, MODE_LIST, MODE_SET]
        );
        assert_eq!(
            proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap()["structuredContent"]
                ["current"],
            false
        );
        proxy
            .call_tool(call(MODE_SET, json!({"modeIri":WRITER})))
            .await
            .unwrap();
        assert_eq!(
            proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap()["structuredContent"]
                ["current"],
            true
        );
    }

    #[tokio::test]
    async fn graph_edits_cannot_widen_a_running_processs_mode_choices() {
        let cell = Arc::new(Cell::new());
        let proxy = ModeBackend::new(cell.clone(), "lab", AGENT).unwrap();
        proxy.call_tool(call(MODE_STATUS, json!({}))).await.unwrap();
        cell.expanded.store(true, Ordering::SeqCst);
        assert!(proxy
            .call_tool(call(MODE_SET, json!({"modeIri":"urn:sophia:mode:admin"})))
            .await
            .unwrap_err()
            .to_string()
            .contains("when this MCP process started"));
        assert_eq!(
            proxy.call_tool(call(MODE_LIST, json!({}))).await.unwrap()["structuredContent"]
                ["modes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let restarted = ModeBackend::new(cell, "lab", AGENT).unwrap();
        assert_eq!(
            restarted
                .call_tool(call(MODE_LIST, json!({})))
                .await
                .unwrap()["structuredContent"]["modes"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn conflicting_tool_descriptors_fail_before_any_call_is_forwarded() {
        let cell = Arc::new(Cell::new());
        cell.duplicate.store(true, Ordering::SeqCst);
        let proxy = ModeBackend::new(cell.clone(), "lab", AGENT).unwrap();
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
