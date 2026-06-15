//! neem — a stdio MCP server that proxies Claude Code (and other MCP clients) to
//! a Mnemosyne/garden backend. Tools are autopopulated from the backend; neem
//! never hardcodes them. Garden owns the tools; neem owns who-you-are,
//! which-graph, and which-backend.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;

use neem::backend::{self, AuthHeaders, Backend, LocalGarden, RemoteHttp};
use neem::config::Cli;
use neem::server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs to stderr ONLY — stdout is the MCP channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("NEEM_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let backend = build_backend(&cli).await?;

    tracing::info!("neem proxy ready; serving MCP over stdio");
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
    let mcp_url = RemoteHttp::resolve_mcp_url(&cli.backend, cli.graph.as_deref());
    tracing::info!(url = %mcp_url, "backend: REMOTE");

    let auth = AuthHeaders {
        bearer: cli.token.clone(),
        on_behalf_of: cli.on_behalf_of.clone(),
        user_id: cli.user_id.clone(),
        // Remote endpoints (gateway / hosted) don't enforce a loopback Origin.
        origin: None,
    };
    let remote = RemoteHttp::new(mcp_url, auth).context("building remote backend client")?;
    Ok(Arc::new(remote))
}
