//! Liveness and readiness through sentinel files in a local directory, so
//! probes never touch the (possibly hung) PVC.
//!
//! ```text
//!   controller, after each successful statvfs ──► touch HEALTH_DIR/alive
//!   first successful statvfs, workers running  ──► create HEALTH_DIR/ready
//!   SIGTERM                                     ──► remove HEALTH_DIR/ready
//!
//!   kvreap healthcheck --ready   ready exists
//!   kvreap healthcheck --live    alive mtime within --max-age
//! ```

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::config::{HealthConfig, parse_max_age};

const READY: &str = "ready";
const ALIVE: &str = "alive";

/// Writes the sentinel files for the running evictor.
#[derive(Debug)]
pub struct Sentinels {
    dir: PathBuf,
}

impl Sentinels {
    /// Creates the directory and clears a `ready` left by a previous run.
    pub fn new(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let s = Self {
            dir: dir.to_path_buf(),
        };
        s.clear_ready()?;
        Ok(s)
    }

    pub fn mark_ready(&self) -> io::Result<()> {
        fs::File::create(self.dir.join(READY)).map(drop)
    }

    pub fn clear_ready(&self) -> io::Result<()> {
        match fs::remove_file(self.dir.join(READY)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Sets the mtime of `alive` to now, creating it if needed.
    pub fn heartbeat(&self) -> io::Result<()> {
        fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.dir.join(ALIVE))?
            .set_modified(SystemTime::now())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    Live,
    Ready,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProbeFailure {
    NotReady(PathBuf),
    NoHeartbeat(PathBuf),
    Stale {
        path: PathBuf,
        age: Duration,
        max_age: Duration,
    },
    Io(PathBuf, io::ErrorKind),
}

impl fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady(p) => write!(f, "not ready: {} does not exist", p.display()),
            Self::NoHeartbeat(p) => write!(f, "not live: {} does not exist", p.display()),
            Self::Stale { path, age, max_age } => write!(
                f,
                "not live: {} is {}s old, max {}s (controller stalled, likely a hung mount)",
                path.display(),
                age.as_secs(),
                max_age.as_secs()
            ),
            Self::Io(p, kind) => write!(f, "cannot stat {}: {kind}", p.display()),
        }
    }
}

/// Checks one probe against the files in `dir`; only stats files there.
pub fn check(dir: &Path, probe: Probe, max_age: Duration) -> Result<(), ProbeFailure> {
    let (path, missing): (PathBuf, fn(PathBuf) -> ProbeFailure) = match probe {
        Probe::Ready => (dir.join(READY), ProbeFailure::NotReady),
        Probe::Live => (dir.join(ALIVE), ProbeFailure::NoHeartbeat),
    };
    let meta = match fs::metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(missing(path)),
        Err(e) => return Err(ProbeFailure::Io(path, e.kind())),
    };
    if probe == Probe::Ready {
        return Ok(());
    }
    let modified = meta
        .modified()
        .map_err(|e| ProbeFailure::Io(path.clone(), e.kind()))?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::ZERO);
    if age > max_age {
        return Err(ProbeFailure::Stale { path, age, max_age });
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Args {
    probe: Probe,
    max_age: Option<Duration>,
}

const USAGE: &str = "usage: kvreap healthcheck --live|--ready [--max-age SECONDS]";

fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut probe = None;
    let mut max_age = None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let next = match arg.as_str() {
            "--live" => Some(Probe::Live),
            "--ready" => Some(Probe::Ready),
            "--max-age" => {
                let raw = it.next().ok_or("--max-age needs a value")?;
                max_age = Some(parse_max_age("--max-age", raw.clone()).map_err(|e| e.to_string())?);
                None
            }
            other => return Err(format!("unknown argument {other:?}")),
        };
        if let Some(p) = next {
            if probe.is_some_and(|q| q != p) {
                return Err("pass only one of --live and --ready".into());
            }
            probe = Some(p);
        }
    }
    let probe = probe.ok_or("pass --live or --ready")?;
    Ok(Args { probe, max_age })
}

/// `kvreap healthcheck ...`: exit 0 healthy, 1 unhealthy, 2 bad usage.
pub fn run_cli(args: &[String], lookup: impl Fn(&str) -> Option<String>) -> u8 {
    let parsed = parse_args(args);
    let config = HealthConfig::from_lookup(lookup);
    let (args, config) = match (parsed, config) {
        (Ok(a), Ok(c)) => (a, c),
        (Err(e), _) => {
            eprintln!("healthcheck: {e}\n{USAGE}");
            return 2;
        }
        (_, Err(e)) => {
            eprintln!("healthcheck: {e}");
            return 2;
        }
    };
    match check(
        &config.dir,
        args.probe,
        args.max_age.unwrap_or(config.max_age),
    ) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("healthcheck: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    use crate::health::{Args, Probe, ProbeFailure, Sentinels, check, parse_args, run_cli};

    fn backdate(path: &Path, secs: u64) {
        let t = SystemTime::now() - Duration::from_secs(secs);
        fs::File::options()
            .write(true)
            .open(path)
            .and_then(|f| f.set_modified(t))
            .expect("set mtime");
    }

    const MAX: Duration = Duration::from_secs(30);

    #[test]
    fn ready_follows_mark_and_clear() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("health");
        let s = Sentinels::new(&dir).expect("sentinels");
        assert_eq!(
            check(&dir, Probe::Ready, MAX),
            Err(ProbeFailure::NotReady(dir.join("ready")))
        );
        s.mark_ready().expect("ready");
        assert_eq!(check(&dir, Probe::Ready, MAX), Ok(()));
        s.clear_ready().expect("clear");
        s.clear_ready().expect("clearing twice is fine");
        assert!(check(&dir, Probe::Ready, MAX).is_err());
    }

    #[test]
    fn new_clears_a_stale_ready_from_a_previous_run() {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::write(tmp.path().join("ready"), b"").expect("write");
        Sentinels::new(tmp.path()).expect("sentinels");
        assert!(!tmp.path().join("ready").exists());
    }

    #[test]
    fn live_checks_heartbeat_age() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let s = Sentinels::new(tmp.path()).expect("sentinels");
        let alive = tmp.path().join("alive");
        assert_eq!(
            check(tmp.path(), Probe::Live, MAX),
            Err(ProbeFailure::NoHeartbeat(alive.clone()))
        );
        s.heartbeat().expect("heartbeat");
        assert_eq!(check(tmp.path(), Probe::Live, MAX), Ok(()));

        backdate(&alive, 60);
        match check(tmp.path(), Probe::Live, MAX) {
            Err(ProbeFailure::Stale { age, max_age, .. }) => {
                assert!(age >= Duration::from_secs(60));
                assert_eq!(max_age, MAX);
            }
            other => panic!("expected stale, got {other:?}"),
        }
        assert_eq!(
            check(tmp.path(), Probe::Live, Duration::from_secs(120)),
            Ok(())
        );

        s.heartbeat().expect("heartbeat refreshes mtime");
        assert_eq!(check(tmp.path(), Probe::Live, MAX), Ok(()));
    }

    #[test]
    fn heartbeat_from_the_future_is_live() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let s = Sentinels::new(tmp.path()).expect("sentinels");
        s.heartbeat().expect("heartbeat");
        fs::File::options()
            .write(true)
            .open(tmp.path().join("alive"))
            .and_then(|f| f.set_modified(SystemTime::now() + Duration::from_secs(60)))
            .expect("set mtime");
        assert_eq!(check(tmp.path(), Probe::Live, MAX), Ok(()));
    }

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_cli_args() {
        assert_eq!(
            parse_args(&args(&["--live"])),
            Ok(Args {
                probe: Probe::Live,
                max_age: None
            })
        );
        assert_eq!(
            parse_args(&args(&["--max-age", "7.5", "--ready"])),
            Ok(Args {
                probe: Probe::Ready,
                max_age: Some(Duration::from_secs(7))
            })
        );
        for bad in [
            &[][..],
            &["--live", "--ready"][..],
            &["--live", "--max-age"][..],
            &["--live", "--max-age", "0"][..],
            &["--live", "--verbose"][..],
        ] {
            assert!(parse_args(&args(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn run_cli_exit_codes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().to_str().expect("utf8").to_string();
        let lookup = |k: &str| match k {
            "HEALTH_DIR" => Some(dir.clone()),
            "CLEANUP_THRESHOLD" => Some("lots".into()),
            _ => None,
        };
        assert_eq!(run_cli(&args(&["--ready"]), lookup), 1);
        assert_eq!(run_cli(&args(&["--bogus"]), lookup), 2);
        let s = Sentinels::new(tmp.path()).expect("sentinels");
        s.mark_ready().expect("ready");
        s.heartbeat().expect("heartbeat");
        assert_eq!(run_cli(&args(&["--ready"]), lookup), 0);
        assert_eq!(run_cli(&args(&["--live"]), lookup), 0);
        backdate(&tmp.path().join("alive"), 45);
        assert_eq!(run_cli(&args(&["--live"]), lookup), 1, "default 30s");
        assert_eq!(run_cli(&args(&["--live", "--max-age", "60"]), lookup), 0);
        let bad_env = |k: &str| (k == "HEALTH_MAX_AGE_SECONDS").then(|| "0".to_string());
        assert_eq!(run_cli(&args(&["--live"]), bad_env), 2);
    }
}
