//! Self-update against GitHub Releases.
//!
//! Crude on purpose: every release carries three kinds of asset, all static
//! files, so the whole protocol works against any static file server:
//!
//! * `release.json` — `{"name","version","assets":{"<target triple>":"<file>"}}`
//! * `SHA256SUMS` — `sha256sum` output over every tarball
//! * `sophia-mcp-v<version>-<target>.tar.gz` — contains the `sophia-mcp` binary
//!
//! The newest release's manifest is fetched from
//! `{base}/releases/latest/download/release.json` (GitHub redirects that to the
//! newest non-prerelease), assets from `{base}/releases/download/v{version}/…`.
//! No GitHub API, so no API rate limit and no token.
//!
//! The startup check never writes to stdout: stdout is the MCP JSON-RPC channel.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The public release repository.
pub const DEFAULT_RELEASE_BASE: &str = "https://github.com/sophia-labs/sophia-mcp";
/// Name of the binary inside every release tarball.
pub const BIN_NAME: &str = "sophia-mcp";
/// The running version.
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
/// The startup check runs at most once per this interval.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// A release's manifest (`release.json`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseManifest {
    pub name: String,
    pub version: String,
    /// Target triple -> tarball file name.
    #[serde(default)]
    pub assets: BTreeMap<String, String>,
}

/// Where releases come from. `SOPHIA_MCP_UPDATE_URL` overrides the default
/// (a fork, a mirror, or a local test server).
pub fn release_base() -> String {
    std::env::var("SOPHIA_MCP_UPDATE_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_RELEASE_BASE.to_string())
}

/// The release target triple this binary was built for, if releases carry one.
pub fn current_target() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Some("aarch64-unknown-linux-gnu"),
        ("macos", "x86_64") => Some("x86_64-apple-darwin"),
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        _ => None,
    }
}

/// `x.y.z` (an optional leading `v`, any `-pre`/`+build` suffix ignored).
pub fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let v = v.trim().trim_start_matches('v');
    let core = v.split(['-', '+']).next()?;
    let mut parts = core.split('.').map(|p| p.parse::<u64>().ok());
    let out = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(out)
}

/// Whether `candidate` is strictly newer than `current`.
pub fn is_newer(candidate: &str, current: &str) -> bool {
    matches!((parse_version(candidate), parse_version(current)), (Some(a), Some(b)) if a > b)
}

/// Parse `sha256sum` output: `<hex>  <file>` (or `<hex> *<file>`).
pub fn parse_sha256sums(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let (hash, file) = line.trim().split_once(char::is_whitespace)?;
            let file = file.trim().trim_start_matches('*');
            (hash.len() == 64 && !file.is_empty())
                .then(|| (file.to_string(), hash.to_ascii_lowercase()))
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn client(timeout: Duration) -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(timeout)
        .user_agent(concat!("sophia-mcp/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building update HTTP client")
}

async fn get_bytes(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<u8>> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        bail!("GET {url}: HTTP {status}");
    }
    Ok(resp.bytes().await?.to_vec())
}

/// Fetch the newest release's manifest.
pub async fn fetch_latest(base: &str, timeout: Duration) -> anyhow::Result<ReleaseManifest> {
    let url = format!("{base}/releases/latest/download/release.json");
    let body = get_bytes(&client(timeout)?, &url).await?;
    let manifest: ReleaseManifest =
        serde_json::from_slice(&body).with_context(|| format!("parsing {url}"))?;
    if parse_version(&manifest.version).is_none() {
        bail!(
            "release manifest carries an unparseable version {:?}",
            manifest.version
        );
    }
    Ok(manifest)
}

/// Download the release's tarball for `target`, verify it against the
/// release's SHA256SUMS, and return the extracted binary's bytes.
pub async fn download_verified(
    base: &str,
    manifest: &ReleaseManifest,
    target: &str,
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    let asset = manifest
        .assets
        .get(target)
        .ok_or_else(|| anyhow!("release {} has no build for {target}", manifest.version))?;
    if asset.contains('/') || asset.contains("..") {
        bail!("refusing suspicious asset name {asset:?}");
    }
    let dir = format!("{base}/releases/download/v{}", manifest.version);
    let client = client(timeout)?;
    let sums = String::from_utf8(get_bytes(&client, &format!("{dir}/SHA256SUMS")).await?)
        .context("SHA256SUMS is not UTF-8")?;
    let expected = parse_sha256sums(&sums)
        .remove(asset)
        .ok_or_else(|| anyhow!("SHA256SUMS lists no checksum for {asset}"))?;
    let tarball = get_bytes(&client, &format!("{dir}/{asset}")).await?;
    let actual = hex(&Sha256::digest(&tarball));
    if actual != expected {
        bail!("checksum mismatch for {asset}: expected {expected}, got {actual}");
    }
    extract_binary(&tarball, BIN_NAME)
}

/// Pull the file named `bin_name` (at any depth) out of a `.tar.gz`.
pub fn extract_binary(tarball: &[u8], bin_name: &str) -> anyhow::Result<Vec<u8>> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(tarball));
    for entry in archive.entries().context("reading tarball")? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if entry.header().entry_type().is_file()
            && path.file_name().and_then(|n| n.to_str()) == Some(bin_name)
        {
            let mut out = Vec::new();
            entry.read_to_end(&mut out)?;
            if out.is_empty() {
                bail!("{bin_name} in the tarball is empty");
            }
            return Ok(out);
        }
    }
    bail!("tarball does not contain {bin_name}")
}

/// Atomically replace the executable at `exe` with `bytes`, after proving the
/// new binary runs here and reports `expected_version` from `--version`.
pub fn replace_executable(exe: &Path, bytes: &[u8], expected_version: &str) -> anyhow::Result<()> {
    let exe = std::fs::canonicalize(exe).unwrap_or_else(|_| exe.to_path_buf());
    let dir = exe
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", exe.display()))?;
    let staged = dir.join(format!(".{BIN_NAME}.update-{}", std::process::id()));
    std::fs::write(&staged, bytes).with_context(|| {
        format!(
            "writing {} (is the install dir writable?)",
            staged.display()
        )
    })?;
    let result = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))?;
        }
        let out = std::process::Command::new(&staged)
            .arg("--version")
            .output()
            .context("running the downloaded binary")?;
        let reported = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() || !reported.contains(expected_version) {
            bail!(
                "downloaded binary did not report version {expected_version} (got {:?})",
                reported.trim()
            );
        }
        std::fs::rename(&staged, &exe).with_context(|| format!("replacing {}", exe.display()))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&staged);
    }
    result
}

/// Outcome of `sophia-mcp update [--check]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateOutcome {
    UpToDate { latest: String },
    Available { latest: String },
    Installed { latest: String, path: PathBuf },
}

/// `sophia-mcp update [--check]`: check (and unless `check_only`, install).
pub async fn run_update(
    base: &str,
    exe: &Path,
    current: &str,
    check_only: bool,
) -> anyhow::Result<UpdateOutcome> {
    let timeout = Duration::from_secs(120);
    let manifest = fetch_latest(base, Duration::from_secs(15)).await?;
    if !is_newer(&manifest.version, current) {
        return Ok(UpdateOutcome::UpToDate {
            latest: manifest.version,
        });
    }
    if check_only {
        return Ok(UpdateOutcome::Available {
            latest: manifest.version,
        });
    }
    let target = current_target()
        .ok_or_else(|| anyhow!("no release builds for this platform; build from source"))?;
    let bytes = download_verified(base, &manifest, target, timeout).await?;
    replace_executable(exe, &bytes, &manifest.version)?;
    Ok(UpdateOutcome::Installed {
        latest: manifest.version,
        path: exe.to_path_buf(),
    })
}

// ---------------------------------------------------------------- startup check

/// Why the startup check is (not) running. Disabled by
/// `SOPHIA_MCP_NO_UPDATE_CHECK=1` or any non-empty `CI`.
pub fn startup_check_disabled(get: impl Fn(&str) -> Option<String>) -> bool {
    let set = |k: &str| {
        get(k)
            .map(|v| !v.trim().is_empty() && v.trim() != "0" && v.trim() != "false")
            .unwrap_or(false)
    };
    set("SOPHIA_MCP_NO_UPDATE_CHECK") || set("CI")
}

/// The per-user cache directory (`SOPHIA_MCP_CACHE_DIR` overrides).
pub fn cache_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("SOPHIA_MCP_CACHE_DIR").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(dir));
    }
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(xdg).join("sophia-mcp"));
    }
    let home = PathBuf::from(std::env::var_os("HOME").filter(|v| !v.is_empty())?);
    if cfg!(target_os = "macos") {
        Some(home.join("Library/Caches/sophia-mcp"))
    } else {
        Some(home.join(".cache/sophia-mcp"))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CheckStamp {
    checked_at: u64,
    #[serde(default)]
    latest: Option<String>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether a stamp written at `checked_at` is still fresh at `now`.
pub fn stamp_is_fresh(checked_at: u64, now: u64) -> bool {
    now >= checked_at && now - checked_at < CHECK_INTERVAL.as_secs()
}

/// The notice line (stderr only).
pub fn notice(latest: &str, current: &str) -> String {
    format!("sophia-mcp {latest} available (you have {current}): run `sophia-mcp update`")
}

/// Non-blocking-friendly startup check: at most once per day, never on
/// stdout, silent on any failure. Returns the notice it printed, if any.
pub async fn startup_check() -> Option<String> {
    if startup_check_disabled(|k| std::env::var(k).ok()) {
        return None;
    }
    let stamp_path = cache_dir()?.join("update-check.json");
    if let Ok(raw) = std::fs::read(&stamp_path) {
        if let Ok(stamp) = serde_json::from_slice::<CheckStamp>(&raw) {
            if stamp_is_fresh(stamp.checked_at, now_secs()) {
                return None;
            }
        }
    }
    let latest = fetch_latest(&release_base(), Duration::from_secs(5)).await;
    let stamp = CheckStamp {
        checked_at: now_secs(),
        latest: latest.as_ref().ok().map(|m| m.version.clone()),
    };
    if let Some(parent) = stamp_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&stamp_path, serde_json::to_vec(&stamp).unwrap_or_default());
    match latest {
        Ok(m) if is_newer(&m.version, CURRENT_VERSION) => {
            let line = notice(&m.version, CURRENT_VERSION);
            eprintln!("{line}");
            Some(line)
        }
        Ok(_) => None,
        Err(e) => {
            tracing::debug!("update check failed: {e:#}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        assert_eq!(parse_version("v0.10.2"), Some((0, 10, 2)));
        assert_eq!(parse_version("1.2.3-rc.1"), Some((1, 2, 3)));
        assert_eq!(parse_version("1.2"), None);
        assert_eq!(parse_version("1.2.3.4"), None);
        assert!(is_newer("0.10.0", "0.9.9"));
        assert!(is_newer("v0.5.0", "0.4.0"));
        assert!(!is_newer("0.4.0", "0.4.0"));
        assert!(!is_newer("garbage", "0.4.0"));
    }

    #[test]
    fn sha256sums_parse_both_modes() {
        let a = "a".repeat(64);
        let b = "B".repeat(64);
        let sums = parse_sha256sums(&format!("{a}  x.tar.gz\n{b} *y.tar.gz\nnot a line\n"));
        assert_eq!(sums.get("x.tar.gz"), Some(&a));
        assert_eq!(sums.get("y.tar.gz"), Some(&"b".repeat(64)));
        assert_eq!(sums.len(), 2);
    }

    #[test]
    fn disabled_by_env_or_ci() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(startup_check_disabled(env(&[(
            "SOPHIA_MCP_NO_UPDATE_CHECK",
            "1"
        )])));
        assert!(startup_check_disabled(env(&[("CI", "true")])));
        assert!(!startup_check_disabled(env(&[("CI", "")])));
        assert!(!startup_check_disabled(env(&[(
            "SOPHIA_MCP_NO_UPDATE_CHECK",
            "0"
        )])));
        assert!(!startup_check_disabled(env(&[])));
    }

    #[test]
    fn stamp_freshness_is_one_day() {
        assert!(stamp_is_fresh(1_000, 1_000 + 3_600));
        assert!(!stamp_is_fresh(1_000, 1_000 + 86_400));
        assert!(!stamp_is_fresh(2_000, 1_000)); // clock went backwards: re-check
    }
}
