//! Self-update against a real local HTTP server serving a real release: a
//! gzipped tarball, a SHA256SUMS file, and a release.json — the same static
//! layout GitHub Releases serves. Also proves the startup check never touches
//! stdout (the MCP JSON-RPC channel).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use sha2::{Digest, Sha256};
use sophia_mcp::update::{self, UpdateOutcome};

/// A minimal static file server: GET serves files under `root`; POST /mcp
/// answers JSON-RPC `initialize` like a Garden backend would.
fn serve(root: PathBuf) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let root = root.clone();
            std::thread::spawn(move || handle(stream, &root));
        }
    });
    format!("http://{addr}")
}

fn handle(mut stream: TcpStream, root: &Path) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0u8; content_length];
    let _ = reader.read_exact(&mut body);
    let mut parts = request_line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or("/"));
    let (status, payload, ctype) = if method == "POST" && path == "/mcp" {
        let req: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let reply = serde_json::json!({"jsonrpc":"2.0","id":req["id"],"result":{"capabilities":{"tools":{}}}});
        ("200 OK", reply.to_string().into_bytes(), "application/json")
    } else {
        match std::fs::read(root.join(path.trim_start_matches('/'))) {
            Ok(bytes) if !path.contains("..") => ("200 OK", bytes, "application/octet-stream"),
            _ => ("404 Not Found", b"not found".to_vec(), "text/plain"),
        }
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: {ctype}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&payload);
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sophia-mcp-update-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Publish a fake-but-real release `version` under `root` whose binary is a
/// shell script printing `sophia-mcp <version>`. `corrupt` publishes a wrong checksum.
fn publish_release(root: &Path, version: &str, corrupt: bool) {
    let target = update::current_target().expect("test platform has release builds");
    let asset = format!("sophia-mcp-v{version}-{target}.tar.gz");
    let script = format!("#!/bin/sh\necho 'sophia-mcp {version}'\n");
    let mut tar = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::default(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_size(script.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    tar.append_data(
        &mut header,
        format!("sophia-mcp-v{version}-{target}/sophia-mcp"),
        script.as_bytes(),
    )
    .unwrap();
    let tarball = tar.into_inner().unwrap().finish().unwrap();
    let mut digest: String = Sha256::digest(&tarball)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if corrupt {
        digest = "0".repeat(64);
    }
    let dir = root.join(format!("releases/download/v{version}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(&asset), &tarball).unwrap();
    std::fs::write(dir.join("SHA256SUMS"), format!("{digest}  {asset}\n")).unwrap();
    let manifest = serde_json::json!({
        "name": "sophia-mcp", "version": version, "assets": { target: asset }
    });
    let latest = root.join("releases/latest/download");
    std::fs::create_dir_all(&latest).unwrap();
    std::fs::write(latest.join("release.json"), manifest.to_string()).unwrap();
}

fn fake_installed_exe(dir: &Path) -> PathBuf {
    let exe = dir.join("sophia-mcp");
    std::fs::write(&exe, "#!/bin/sh\necho 'sophia-mcp 0.0.1'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    exe
}

#[cfg(unix)]
#[tokio::test]
async fn check_then_install_replaces_the_binary_after_verifying_its_checksum() {
    let root = tempdir("release");
    publish_release(&root, "9.9.9", false);
    let base = serve(root);
    let install = tempdir("install");
    let exe = fake_installed_exe(&install);

    let checked = update::run_update(&base, &exe, "0.5.0", true)
        .await
        .unwrap();
    assert_eq!(
        checked,
        UpdateOutcome::Available {
            latest: "9.9.9".into()
        }
    );
    assert!(std::fs::read_to_string(&exe).unwrap().contains("0.0.1"));

    let installed = update::run_update(&base, &exe, "0.5.0", false)
        .await
        .unwrap();
    assert!(matches!(installed, UpdateOutcome::Installed { ref latest, .. } if latest == "9.9.9"));
    let out = Command::new(&exe).output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "sophia-mcp 9.9.9"
    );

    let again = update::run_update(&base, &exe, "9.9.9", false)
        .await
        .unwrap();
    assert_eq!(
        again,
        UpdateOutcome::UpToDate {
            latest: "9.9.9".into()
        }
    );
    // No staging debris left next to the binary.
    assert_eq!(std::fs::read_dir(&install).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn checksum_mismatch_refuses_and_leaves_the_binary_alone() {
    let root = tempdir("corrupt");
    publish_release(&root, "9.9.9", true);
    let base = serve(root);
    let install = tempdir("install-corrupt");
    let exe = fake_installed_exe(&install);
    let err = update::run_update(&base, &exe, "0.5.0", false)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("checksum mismatch"), "{err:#}");
    assert!(std::fs::read_to_string(&exe).unwrap().contains("0.0.1"));
    assert_eq!(std::fs::read_dir(&install).unwrap().count(), 1);
}

#[test]
fn update_check_subcommand_reports_a_newer_release() {
    let root = tempdir("cli");
    publish_release(&root, "9.9.9", false);
    let base = serve(root);
    let out = Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
        .args(["update", "--check"])
        .env("SOPHIA_MCP_UPDATE_URL", &base)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("sophia-mcp 9.9.9 available"),
        "stdout: {stdout}"
    );
}

/// The startup notice goes to stderr; stdout carries only JSON-RPC; the check
/// runs at most once per day (a second start with the same cache is silent).
#[test]
fn startup_check_writes_only_to_stderr_and_only_once_a_day() {
    let root = tempdir("stdout");
    publish_release(&root, "9.9.9", false);
    let base = serve(root);
    let cache = tempdir("cache");

    let run = |expect_notice: bool| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sophia-mcp"))
            .args(["--backend", &format!("{base}/mcp")])
            .env("SOPHIA_MCP_UPDATE_URL", &base)
            .env("SOPHIA_MCP_CACHE_DIR", &cache)
            .env_remove("SOPHIA_MCP_NO_UPDATE_CHECK")
            .env_remove("CI")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n")
            .unwrap();
        // Keep stdin open until the check has had its chance to speak.
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut all = String::new();
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if line.contains("available (you have") {
                    let _ = tx.send(());
                }
                all.push_str(&line);
                all.push('\n');
            }
            all
        });
        let saw_notice = rx.recv_timeout(Duration::from_secs(if expect_notice { 15 } else { 3 }));
        assert_eq!(saw_notice.is_ok(), expect_notice);
        drop(stdin);
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        child.wait().unwrap();
        assert!(!stdout.trim().is_empty(), "initialize must be answered");
        for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("non-JSON on stdout ({e}): {line:?}"));
            assert_eq!(v["jsonrpc"], "2.0", "stdout line is not JSON-RPC: {line}");
        }
        assert!(!stdout.contains("available"));
    };
    run(true);
    assert!(cache.join("update-check.json").exists());
    run(false);
}
