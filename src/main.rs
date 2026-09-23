//! sophia-mcp — a stdio MCP server that proxies Claude Code (and other MCP clients) to
//! a Mnemosyne/garden backend. Tools are autopopulated from the backend; sophia-mcp
//! never hardcodes graph tools. Garden owns those tools; sophia-mcp owns
//! identity, graph routing, and optional process-local mode controls.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;

use sophia_mcp::backend::{
    self, AuthHeaders, Backend, ComposedBackend, GatewayBackend, GatewayOptions, LocalGarden,
    ModeBackend, ModeOptions, RemoteHttp,
};
use sophia_mcp::config::{Cli, Command};
use sophia_mcp::server;
use sophia_mcp::update;

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
    if let Some(Command::Update { check }) = cli.command {
        return run_update(check).await;
    }
    // Non-blocking, at most once a day, stderr only (stdout is the MCP channel).
    tokio::spawn(async {
        update::startup_check().await;
    });
    let sub_specs = cli.resolved_subs().context("parsing --sub / --sub-token")?;
    let mut backend = build_backend(&cli).await?;
    if !sub_specs.is_empty() {
        let prefixes: Vec<&str> = sub_specs.iter().map(|s| s.prefix.as_str()).collect();
        tracing::info!(subs = ?prefixes, "composing sub-MCPs above the primary backend");
        let composed = ComposedBackend::compose(
            backend,
            sub_specs,
            Duration::from_secs(cli.request_timeout.max(1)),
            cli.allow_insecure_http,
        )
        .await?;
        tracing::info!(subs = ?composed.sub_prefixes(), "sub-MCPs composed (see above for which are up)");
        backend = Arc::new(composed);
    }

    if let Some(graph_id) = cli.graph.as_deref() {
        let options = ModeOptions {
            cache_ttl: Duration::from_millis(cli.mode_cache_ttl_ms),
            poll_interval: Duration::from_millis(cli.mode_poll_ms),
        };
        match ModeBackend::new(backend.clone(), graph_id, cli.agent_id.as_deref(), options) {
            Ok(modes) => {
                tracing::info!(
                    graph_id,
                    preset_agent = cli.agent_id.as_deref(),
                    "agent declaration + live MCP modes available"
                );
                backend = modes;
            }
            Err(e) if cli.agent_id.is_some() => return Err(e),
            Err(e) => tracing::warn!("agent declaration unavailable: {e:#}"),
        }
    } else if cli.agent_id.is_some() {
        anyhow::bail!("--agent-id requires --graph");
    }

    tracing::info!("sophia-mcp proxy ready; serving MCP over stdio");
    serve_until_shutdown(backend).await?;
    Ok(())
}

/// `sophia-mcp update [--check]`. Not an MCP session, so stdout is free.
async fn run_update(check_only: bool) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locating the running sophia-mcp binary")?;
    let base = update::release_base();
    let current = update::CURRENT_VERSION;
    match update::run_update(&base, &exe, current, check_only).await? {
        update::UpdateOutcome::UpToDate { latest } => {
            println!("sophia-mcp {current} is up to date (latest release: {latest})");
        }
        update::UpdateOutcome::Available { latest } => {
            println!("{}", update::notice(&latest, current));
        }
        update::UpdateOutcome::Installed { latest, path } => {
            println!(
                "sophia-mcp updated {current} -> {latest} at {} (restart MCP clients to pick it up)",
                path.display()
            );
        }
    }
    Ok(())
}

/// MCP clients commonly terminate stdio servers with SIGTERM rather than
/// closing stdin. Exit through Rust so LocalGarden's child handle is dropped
/// and kill_on_drop stops gardend before the next proxy opens the profile.
async fn serve_until_shutdown(backend: Arc<dyn Backend>) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = server::serve_stdio(backend) => result,
            result = tokio::signal::ctrl_c() => result.map_err(Into::into),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            result = server::serve_stdio(backend) => result,
            result = tokio::signal::ctrl_c() => result.map_err(Into::into),
        }
    }
}

async fn build_backend(cli: &Cli) -> anyhow::Result<Arc<dyn Backend>> {
    if cli.is_local() {
        let opts = backend::local::LocalGardenOptions {
            profile_dir: cli.resolved_profile_dir(),
            garden_bin: cli.garden_bin.clone(),
            port: cli.local_port,
            health_timeout: Duration::from_secs(cli.local_health_timeout),
            request_timeout: Duration::from_secs(cli.request_timeout.max(1)),
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
    if let (Some(owner), Some(graph)) = (cli.owner.as_deref(), cli.graph.as_deref()) {
        let opts = GatewayOptions {
            activation_timeout: Duration::from_secs(cli.activation_timeout),
            activation_poll: Duration::from_secs(cli.activation_poll.max(1)),
            request_timeout: Duration::from_secs(cli.request_timeout.max(1)),
            unified_mcp_fallback: cli.unified_mcp_fallback,
            allow_insecure_http: cli.allow_insecure_http,
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
        let remote = RemoteHttp::new(
            direct,
            auth,
            Duration::from_secs(cli.request_timeout.max(1)),
            cli.allow_insecure_http,
        )
        .context("connecting to direct Garden MCP")?;
        tracing::info!(url = %remote.mcp_url(), "backend: REMOTE direct Garden MCP");
        return Ok(Arc::new(remote));
    }
    Err(anyhow::anyhow!(
        "remote cloud-2 gateways require both --owner user:<subject> and --graph; only an explicit direct /mcp endpoint may omit them"
    ))
}
