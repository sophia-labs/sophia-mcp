//! sophia-mcp — a stdio MCP server that proxies Claude Code (and other MCP clients) to
//! a Mnemosyne/garden backend. Tools are autopopulated from the backend; sophia-mcp
//! never hardcodes them. Garden owns the tools; sophia-mcp owns who-you-are,
//! which-graph, and which-backend.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;

use sophia_mcp::backend::{
    self, AuthHeaders, Backend, ComposedBackend, GatewayBackend, GatewayOptions, LocalGarden,
    RemoteHttp,
};
use sophia_mcp::config::Cli;
use sophia_mcp::server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs to stderr ONLY — stdout is the MCP channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SOPHIA_MCP_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let sub_specs = cli
        .resolved_subs()
        .context("parsing --sub / --sub-token")?;
    let mut backend = build_backend(&cli).await?;
    if !sub_specs.is_empty() {
        let prefixes: Vec<&str> = sub_specs.iter().map(|s| s.prefix.as_str()).collect();
        tracing::info!(subs = ?prefixes, "composing sub-MCPs above the primary backend");
        let composed = ComposedBackend::compose(backend, sub_specs).await?;
        tracing::info!(subs = ?composed.sub_prefixes(), "sub-MCPs composed (see above for which are up)");
        backend = Arc::new(composed);
    }

    tracing::info!("sophia-mcp proxy ready; serving MCP over stdio");
    server::serve_stdio(backend).await?;
    Ok(())
}

async fn build_backend(cli: &Cli) -> anyhow::Result<Arc<dyn Backend>> {
    if cli.is_local() {
        let opts = backend::local::LocalGardenOptions {
            profile_dir: cli.resolved_profile_dir(),
            garden_bin: cli.garden_bin.clone(),
            port: cli.local_port,
            health_timeout: Duration::from_secs(cli.local_health_timeout),
        };
        tracing::info!(
            profile = %opts.profile_dir.display(),
            "backend: LOCAL headless garden"
        );
        let local = LocalGarden::start(opts)
            .await
            .context("starting local headless garden backend")?;
        return Ok(Arc::new(local));
    }

    // REMOTE backend: a URL to an existing backend's MCP endpoint.
    let auth = AuthHeaders {
        bearer: cli.token.clone(),
        on_behalf_of: cli.on_behalf_of.clone(),
        user_id: cli.user_id.clone(),
        // Remote endpoints (gateway / hosted) don't enforce a loopback Origin.
        origin: None,
    };
    if cli.token.as_deref().is_some_and(|token| token.starts_with("sph_ak")) {
        return Ok(Arc::new(backend::ScopedMcp::new(
            &cli.backend, cli.owner.as_deref(), cli.graph.as_deref(), auth,
            Duration::from_secs(cli.activation_timeout.max(1)),
        )?));
    }
    if let (Some(owner), Some(graph)) = (cli.owner.as_deref(), cli.graph.as_deref()) {
        let opts = GatewayOptions {
            activation_timeout: Duration::from_secs(cli.activation_timeout),
            activation_poll: Duration::from_secs(cli.activation_poll.max(1)),
            request_timeout: Duration::from_secs(cli.request_timeout.max(1)),
            unified_mcp_fallback: cli.unified_mcp_fallback,
        };
        let gateway = Arc::new(
            GatewayBackend::connect(&cli.backend, owner, graph, auth, opts)
                .await
                .context("discovering owner-scoped remote graph through the gateway")?,
        );
        tracing::info!(
            url = %gateway.mcp_url_for(graph),
            control = %gateway.control_url(),
            owner,
            graph,
            "backend: REMOTE gateway (control tools + bound graph, graph_id routing)"
        );
        // Wake the bound cell in the background so the stdio `initialize`
        // handshake answers immediately; tools/list and tools/call do their own
        // bounded wait and surface a truthful error if the cell stays cold.
        let warm = Arc::clone(&gateway);
        tokio::spawn(async move {
            if let Err(e) = warm.warm().await {
                tracing::warn!("bound graph not routable yet: {e:#}");
            }
        });
        return Ok(gateway);
    }

    // Preserve direct Garden loopback/sidecar interoperability. A cloud-2
    // gateway base is never allowed through this compatibility branch: it
    // must bind an explicit owner tuple and prove discovery first.
    let direct = cli.backend.trim_end_matches('/');
    if direct.ends_with("/mcp") && !direct.contains("/g/") {
        let remote = RemoteHttp::new(direct, auth).context("connecting to direct Garden MCP")?;
        tracing::info!(url = %remote.mcp_url(), "backend: REMOTE direct Garden MCP");
        return Ok(Arc::new(remote));
    }
    Err(anyhow::anyhow!(
        "remote cloud-2 gateways require both --owner user:<subject> and --graph; only an explicit direct /mcp endpoint may omit them"
    ))
}
