//! End-to-end tests: the real `kvreap` binary against real directory trees.
//!
//! Most tests force eviction on with `CLEANUP_THRESHOLD` near zero, so they
//! run on any filesystem. The `threshold_*` tests need a small filesystem that
//! nothing else writes to (they fill it to 90%); they are `#[ignore]`d and read
//! its path from `KVREAP_E2E_DIR`:
//!
//! ```text
//! docker run --rm --tmpfs /e2e:size=64m,exec -e KVREAP_E2E_DIR=/e2e \
//!   -v "$PWD":/src -w /src docker.io/library/rust:1-bookworm \
//!   cargo test --test e2e -- --include-ignored
//! ```

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io::Read;
#[cfg(feature = "events")]
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
#[cfg(feature = "events")]
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(feature = "events")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

const CACHE_DIR: &str = "kv/model-cache/models";
const BASE: &str = "org-model_abcdef012345";
const MODEL_NAME: &str = "org/model";
const COLD_AGE: Duration = Duration::from_secs(7200);
const BIN: &str = env!("CARGO_BIN_EXE_kvreap");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Block {
    hash: u64,
    path: PathBuf,
}

fn hash_for(i: u64) -> u64 {
    // Golden-ratio multiplier spreads consecutive indices across buckets.
    i.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn set_atime(path: &Path, age: Duration) {
    let t = SystemTime::now() - age;
    fs::File::options()
        .write(true)
        .open(path)
        .and_then(|f| f.set_times(fs::FileTimes::new().set_accessed(t).set_modified(t)))
        .expect("set file times");
}

struct Cache {
    mount: PathBuf,
}

impl Cache {
    fn new(mount: &Path) -> Self {
        let base = mount.join(CACHE_DIR).join(BASE);
        fs::create_dir_all(&base).expect("mkdir base");
        fs::write(
            base.join("config.json"),
            format!(r#"{{"model_name": "{MODEL_NAME}"}}"#),
        )
        .expect("write config.json");
        Self {
            mount: mount.to_path_buf(),
        }
    }

    fn root(&self) -> PathBuf {
        self.mount.join(CACHE_DIR)
    }

    fn rank(&self) -> PathBuf {
        self.root().join(format!("{BASE}_r0"))
    }

    fn block_path(&self, hash: u64) -> PathBuf {
        let hex = format!("{hash:016x}");
        self.rank()
            .join(&hex[..3])
            .join(format!("{}_g0", &hex[3..5]))
            .join(format!("{hex}.bin"))
    }

    fn write_block(&self, hash: u64, size: usize, age: Option<Duration>) -> Block {
        let path = self.block_path(hash);
        fs::create_dir_all(path.parent().expect("leaf")).expect("mkdir leaf");
        fs::write(&path, vec![0u8; size]).expect("write block");
        if let Some(age) = age {
            set_atime(&path, age);
        }
        Block { hash, path }
    }

    /// `count` cold blocks spread across buckets; block i is `COLD_AGE + i` seconds old.
    fn cold_blocks(&self, count: u64, size: usize) -> Vec<Block> {
        (0..count)
            .map(|i| self.write_block(hash_for(i), size, Some(COLD_AGE + Duration::from_secs(i))))
            .collect()
    }

    fn hot_blocks(&self, count: u64, size: usize) -> Vec<Block> {
        (0..count)
            .map(|i| self.write_block(hash_for(1_000_000 + i), size, None))
            .collect()
    }
}

fn existing(blocks: &[Block]) -> Vec<&Block> {
    blocks.iter().filter(|b| b.path.exists()).collect()
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

fn always_evicting() -> Vec<(&'static str, String)> {
    vec![
        ("CLEANUP_THRESHOLD", "0.001".into()),
        ("TARGET_THRESHOLD", "0".into()),
        ("DIR_CLEANUP_TTL_SECONDS", "0".into()),
    ]
}

fn usage_percent(path: &Path) -> f64 {
    let st = rustix::fs::statvfs(path).expect("statvfs");
    let total = st.f_blocks * st.f_frsize;
    let free = st.f_bfree * st.f_frsize;
    (total - free) as f64 / total as f64 * 100.0
}

/// Tests that rely on pacing would be invalidated by the >=97% emergency band.
fn assert_not_in_emergency_band(path: &Path) {
    let usage = usage_percent(path);
    assert!(
        usage < 95.0,
        "{} is {usage:.1}% full; free space before running e2e tests",
        path.display()
    );
}

struct Evictor {
    child: Option<Child>,
    stdout: PathBuf,
    _logs: tempfile::TempDir,
}

impl Evictor {
    fn start(mount: &Path, env: &[(&str, String)]) -> Self {
        let logs = tempfile::tempdir().expect("log dir");
        let stdout = logs.path().join("stdout.log");
        let child = Command::new(BIN)
            .env_clear()
            .env("PVC_MOUNT_PATH", mount)
            .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::from(fs::File::create(&stdout).expect("stdout file")))
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn kvreap");
        Self {
            child: Some(child),
            stdout,
            _logs: logs,
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.stdout).unwrap_or_default()
    }

    fn wait_for_log(&self, needle: &str, timeout: Duration) -> bool {
        wait_until(timeout, || self.log().contains(needle))
    }

    fn sigterm(&mut self) -> ExitStatus {
        let mut child = self.child.take().expect("still running");
        let pid = rustix::process::Pid::from_child(&child);
        rustix::process::kill_process(pid, rustix::process::Signal::TERM).expect("send SIGTERM");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("kvreap did not exit within 10s of SIGTERM\n{}", self.log());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Evictor {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn run_to_exit(env: &[(&str, &str)]) -> (ExitStatus, String) {
    let mut child = Command::new(BIN)
        .env_clear()
        .envs(env.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kvreap");
    let status = child.wait().expect("wait");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    (status, stderr)
}

#[test]
fn evicts_strictly_oldest_first_within_a_bucket() {
    let tmp = tempfile::tempdir().expect("tempdir");
    assert_not_in_emergency_band(tmp.path());
    let cache = Cache::new(tmp.path());
    // All blocks share bucket "abc" and leaf "de_g0", so one worker sees all of them
    // in every sample and must always unlink the oldest remaining one.
    let mut blocks: Vec<(Duration, Block)> = (0..20u64)
        .map(|i| {
            let hash = 0xABCD_E000_0000_0000 | i;
            let age = COLD_AGE + Duration::from_secs((i * 7919) % 101);
            (age, cache.write_block(hash, 64, Some(age)))
        })
        .collect();
    let hot = cache.write_block(0xABCD_E000_0000_FFFF, 64, None);
    blocks.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
    let oldest_first: Vec<Block> = blocks.into_iter().map(|(_, b)| b).collect();

    let mut env = always_evicting();
    env.push(("DELETION_MAX_FILES_PER_SECOND", "20".into()));
    let mut ev = Evictor::start(tmp.path(), &env);

    let mut seen_states = BTreeSet::new();
    let done = wait_until(Duration::from_secs(30), || {
        let alive: Vec<bool> = oldest_first.iter().map(|b| b.path.exists()).collect();
        let deleted = alive.iter().take_while(|a| !**a).count();
        assert!(
            alive[deleted..].iter().all(|a| *a),
            "deletions are not a prefix of the age order: {alive:?}"
        );
        seen_states.insert(deleted);
        deleted == oldest_first.len()
    });
    assert!(done, "not all cold blocks were deleted\n{}", ev.log());
    assert!(
        seen_states.len() > 3,
        "pacing should expose intermediate states: {seen_states:?}"
    );
    assert!(hot.path.exists(), "hot block must survive");
    assert!(
        hot.path.parent().expect("leaf").is_dir(),
        "leaf with a hot block must stay"
    );
    assert!(ev.sigterm().success());
}

#[test]
fn deletes_cold_blocks_spares_hot_and_foreign_files_and_reaps_dirs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    assert_not_in_emergency_band(tmp.path());
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(60, 128);
    let hot = cache.hot_blocks(10, 128);
    let leaf = cold[0].path.parent().expect("leaf").to_path_buf();
    let foreign = [
        leaf.join(format!("{:016x}.bin_12345.tmp", cold[0].hash)),
        leaf.join("notes.txt"),
        cache.root().join(BASE).join("config.json"),
    ];
    for f in &foreign[..2] {
        fs::write(f, b"keep").expect("write");
        set_atime(f, COLD_AGE);
    }

    let hot_leaves: HashSet<PathBuf> = hot
        .iter()
        .filter_map(|b| b.path.parent().map(Path::to_path_buf))
        .collect();
    let emptied_leaves: Vec<PathBuf> = cold[1..]
        .iter()
        .filter_map(|b| b.path.parent().map(Path::to_path_buf))
        .filter(|l| !hot_leaves.contains(l))
        .collect();

    let mut ev = Evictor::start(tmp.path(), &always_evicting());
    let converged = wait_until(Duration::from_secs(60), || {
        existing(&cold).is_empty() && emptied_leaves.iter().all(|l| !l.exists())
    });
    assert!(
        converged,
        "cold blocks left: {}, emptied leaves left: {:?}\n{}",
        existing(&cold).len(),
        emptied_leaves
            .iter()
            .filter(|l| l.exists())
            .collect::<Vec<_>>(),
        ev.log()
    );
    assert!(ev.sigterm().success());

    assert_eq!(existing(&hot).len(), hot.len(), "hot blocks must survive");
    for f in &foreign {
        assert!(f.exists(), "{} must not be touched", f.display());
    }
    assert!(leaf.is_dir(), "leaf with foreign files must stay");
    assert!(cache.rank().is_dir(), "rank dir is never removed");
}

#[test]
fn respects_max_files_per_second() {
    let tmp = tempfile::tempdir().expect("tempdir");
    assert_not_in_emergency_band(tmp.path());
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(200, 64);

    let mut env = always_evicting();
    env.push(("DELETION_MAX_FILES_PER_SECOND", "10".into()));
    let mut ev = Evictor::start(tmp.path(), &env);
    let start = Instant::now();
    assert!(
        wait_until(Duration::from_secs(10), || existing(&cold).len()
            < cold.len()),
        "no progress\n{}",
        ev.log()
    );
    std::thread::sleep(Duration::from_secs(3).saturating_sub(start.elapsed()));
    let deleted = cold.len() - existing(&cold).len();
    let elapsed = start.elapsed().as_secs_f64();
    // 10 files/s caps all metadata ops at 20/s, and each deletion costs at least a statx and an unlink.
    assert!(
        (deleted as f64) <= 10.0 * elapsed + 2.0,
        "{deleted} deletions in {elapsed:.2}s exceeds 10 files/s"
    );
    assert!(ev.sigterm().success());
}

#[cfg(feature = "events")]
struct Subscriber {
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<()>,
    frames: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

#[cfg(feature = "events")]
impl Subscriber {
    fn connect(endpoint: &str) -> Self {
        let ctx = zmq::Context::new();
        let sub = ctx.socket(zmq::SUB).expect("sub socket");
        sub.set_rcvtimeo(100).expect("rcvtimeo");
        sub.set_subscribe(b"kv@").expect("subscribe");
        sub.connect(endpoint).expect("connect");
        let stop = Arc::new(AtomicBool::new(false));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let handle = {
            let (stop, frames) = (Arc::clone(&stop), Arc::clone(&frames));
            std::thread::spawn(move || {
                let _ctx = ctx;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(msg) = sub.recv_multipart(0) {
                        frames.lock().expect("lock").push(msg);
                    }
                }
            })
        };
        Self {
            stop,
            handle,
            frames,
        }
    }

    fn finish(self) -> Vec<Vec<Vec<u8>>> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().expect("join subscriber");
        Arc::try_unwrap(self.frames)
            .expect("sole owner")
            .into_inner()
            .expect("lock")
    }
}

#[cfg(feature = "events")]
/// Decodes a payload `[ts, [bin(event)...]]` into the hashes of its BlockRemoved events.
fn decode_removed(payload: &[u8]) -> Vec<u64> {
    let mut rd = payload;
    assert_eq!(
        rmp::decode::read_array_len(&mut rd).expect("payload array"),
        2
    );
    let ts = rmp::decode::read_f64(&mut rd).expect("timestamp");
    assert!(ts > 1.7e9, "timestamp {ts} is not a unix time");
    let n = rmp::decode::read_array_len(&mut rd).expect("events array");
    let mut hashes = Vec::new();
    for _ in 0..n {
        let len = rmp::decode::read_bin_len(&mut rd).expect("bin event") as usize;
        let (mut event, rest) = rd.split_at(len);
        rd = rest;
        assert_eq!(
            rmp::decode::read_array_len(&mut event).expect("event array"),
            3
        );
        let mut tag = [0u8; 32];
        assert_eq!(
            rmp::decode::read_str(&mut event, &mut tag).expect("tag"),
            "BlockRemoved"
        );
        let count = rmp::decode::read_array_len(&mut event).expect("hashes array");
        for _ in 0..count {
            hashes.push(rmp::decode::read_int(&mut event).expect("hash"));
        }
        let mut medium = [0u8; 32];
        assert_eq!(
            rmp::decode::read_str(&mut event, &mut medium).expect("medium"),
            "SHARED_STORAGE"
        );
    }
    hashes
}

#[cfg(feature = "events")]
#[test]
fn publishes_block_removed_events_for_every_deletion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    assert_not_in_emergency_band(tmp.path());
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(40, 64);

    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    let sub = Subscriber::connect(&format!("tcp://127.0.0.1:{port}"));
    let mut env = always_evicting();
    env.push(("STORAGE_EVENTS_ENDPOINT", format!("tcp://127.0.0.1:{port}")));
    env.push(("DELETION_BATCH_SIZE", "1000".into()));
    env.push(("DELETION_MAX_FILES_PER_SECOND", "40".into()));
    let mut ev = Evictor::start(tmp.path(), &env);

    assert!(
        wait_until(Duration::from_secs(60), || existing(&cold).is_empty()),
        "cold blocks left: {}\n{}",
        existing(&cold).len(),
        ev.log()
    );
    assert!(ev.sigterm().success());
    std::thread::sleep(Duration::from_millis(300));
    let frames = sub.finish();

    assert!(!frames.is_empty(), "no events received\n{}", ev.log());
    let mut seqs = Vec::new();
    let mut hashes = Vec::new();
    for msg in &frames {
        assert_eq!(msg.len(), 3, "expected [topic, seq, payload]");
        assert_eq!(msg[0], format!("kv@SHARED_STORAGE@{MODEL_NAME}").as_bytes());
        seqs.push(u64::from_be_bytes(
            msg[1].as_slice().try_into().expect("8-byte seq"),
        ));
        hashes.extend(decode_removed(&msg[2]));
    }
    assert!(
        seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "sequence not contiguous: {seqs:?}"
    );
    let got: BTreeSet<u64> = hashes.iter().copied().collect();
    let want: BTreeSet<u64> = cold.iter().map(|b| b.hash).collect();
    assert_eq!(got.len(), hashes.len(), "duplicate hashes in events");
    assert_eq!(got, want);
}

#[test]
fn dry_run_deletes_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(20, 64);
    let mut env = always_evicting();
    env.push(("DRY_RUN", "true".into()));
    env.push(("LOG_LEVEL", "DEBUG".into()));
    let mut ev = Evictor::start(tmp.path(), &env);
    assert!(
        ev.wait_for_log("[DRY RUN] would delete", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    std::thread::sleep(Duration::from_millis(500));
    assert!(ev.sigterm().success());
    assert_eq!(existing(&cold).len(), cold.len());
}

#[test]
fn below_cleanup_threshold_nothing_is_deleted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(20, 64);
    let env = vec![
        ("CLEANUP_THRESHOLD", "100".to_string()),
        ("TARGET_THRESHOLD", "99.99".to_string()),
    ];
    let mut ev = Evictor::start(tmp.path(), &env);
    assert!(
        ev.wait_for_log("worker started", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    std::thread::sleep(Duration::from_secs(2));
    let status = ev.sigterm();
    assert!(status.success());
    assert_eq!(existing(&cold).len(), cold.len());
    assert!(!ev.log().contains("DELETION_START"));
}

#[test]
fn sigterm_while_evicting_exits_cleanly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = Cache::new(tmp.path());
    cache.cold_blocks(100, 64);
    let mut env = always_evicting();
    env.push(("DELETION_MAX_FILES_PER_SECOND", "2".into()));
    let mut ev = Evictor::start(tmp.path(), &env);
    assert!(
        ev.wait_for_log("DELETION_START", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    std::thread::sleep(Duration::from_millis(500));
    let start = Instant::now();
    let status = ev.sigterm();
    assert!(status.success(), "exit status {status:?}\n{}", ev.log());
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "shutdown took {:?}",
        start.elapsed()
    );
    let log = ev.log();
    assert!(log.contains("shutting down"), "{log}");
    assert!(log.contains("all threads stopped"), "{log}");
}

#[test]
fn logs_to_log_file_path_and_reports_compat_vars() {
    let tmp = tempfile::tempdir().expect("tempdir");
    Cache::new(tmp.path());
    let log_file = tmp.path().join("evictor_all_logs.txt");
    let env = vec![
        ("LOG_FILE_PATH", log_file.to_string_lossy().into_owned()),
        ("FILE_QUEUE_MAXSIZE", "10000".into()),
        ("FILE_QUEUE_MIN_SIZE", "1000".into()),
    ];
    let mut ev = Evictor::start(tmp.path(), &env);
    assert!(
        ev.wait_for_log("worker started", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    assert!(ev.sigterm().success());
    let file_log = fs::read_to_string(&log_file).expect("log file written");
    assert!(file_log.contains("kvreap starting"), "{file_log}");
    for var in ["FILE_QUEUE_MAXSIZE", "FILE_QUEUE_MIN_SIZE"] {
        assert!(
            file_log.contains(var),
            "{var} should be reported as ignored\n{file_log}"
        );
    }
}

#[test]
fn invalid_config_exits_nonzero_with_reason() {
    let cases: [(&[(&str, &str)], &str); 4] = [
        (
            &[("NUM_CRAWLER_PROCESSES", "3")],
            "NUM_CRAWLER_PROCESSES must be a power of 2",
        ),
        (
            &[("CLEANUP_THRESHOLD", "70"), ("TARGET_THRESHOLD", "80")],
            "TARGET_THRESHOLD",
        ),
        (&[("CLEANUP_THRESHOLD", "lots")], "CLEANUP_THRESHOLD"),
        (
            &[("DELETION_MAX_FILES_PER_SECOND", "-5")],
            "DELETION_MAX_FILES_PER_SECOND",
        ),
    ];
    for (env, needle) in cases {
        let (status, stderr) = run_to_exit(env);
        assert_eq!(status.code(), Some(1), "{env:?}: {stderr}");
        assert!(stderr.contains(needle), "{env:?}: {stderr}");
    }
}

static E2E_DIR_LOCK: Mutex<()> = Mutex::new(());

/// Holds the lock for the shared `KVREAP_E2E_DIR` and empties it on drop.
struct E2eDir {
    path: PathBuf,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl Drop for E2eDir {
    fn drop(&mut self) {
        if let Ok(entries) = fs::read_dir(&self.path) {
            for e in entries.flatten() {
                let _ = fs::remove_dir_all(e.path());
            }
        }
    }
}

fn e2e_dir() -> E2eDir {
    let guard = E2E_DIR_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = std::env::var_os("KVREAP_E2E_DIR").map(PathBuf::from).expect(
        "KVREAP_E2E_DIR must point at a small dedicated filesystem (e.g. a 64m tmpfs); see tests/e2e.rs",
    );
    assert!(
        fs::read_dir(&dir)
            .expect("read KVREAP_E2E_DIR")
            .next()
            .is_none(),
        "{} must be empty",
        dir.display()
    );
    let usage = usage_percent(&dir);
    assert!(
        usage < 5.0,
        "{} is already {usage:.1}% used; it must be a dedicated filesystem",
        dir.display()
    );
    E2eDir {
        path: dir,
        _guard: guard,
    }
}

#[test]
#[ignore = "needs KVREAP_E2E_DIR, a small dedicated filesystem"]
fn threshold_evicts_from_cleanup_down_to_target_then_idles() {
    let e2e = e2e_dir();
    let dir = e2e.path.clone();
    let cache = Cache::new(&dir);
    let st = rustix::fs::statvfs(&dir).expect("statvfs");
    let total = st.f_blocks * st.f_frsize;
    let block_size = 128 * 1024;

    let mut cold = Vec::new();
    let mut i = 0;
    while usage_percent(&dir) < 80.0 {
        cold.push(cache.write_block(
            hash_for(i),
            block_size,
            Some(COLD_AGE + Duration::from_secs(i)),
        ));
        i += 1;
    }
    let hot = cache.hot_blocks(
        ((total as f64 * 0.10) / block_size as f64) as u64,
        block_size,
    );
    let start_usage = usage_percent(&dir);
    assert!(
        (85.0..97.0).contains(&start_usage),
        "seeded to {start_usage:.1}%"
    );

    let env = vec![
        ("CLEANUP_THRESHOLD", "85".to_string()),
        ("TARGET_THRESHOLD", "70".to_string()),
        ("LOGGER_INTERVAL_SECONDS", "0.05".to_string()),
        ("DELETION_MAX_FILES_PER_SECOND", "200".to_string()),
    ];
    let mut ev = Evictor::start(&dir, &env);
    assert!(
        ev.wait_for_log("DELETION_START", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    assert!(
        ev.wait_for_log("DELETION_END", Duration::from_secs(60)),
        "{}",
        ev.log()
    );

    let end_usage = usage_percent(&dir);
    assert!(
        (60.0..=70.0).contains(&end_usage),
        "stopped at {end_usage:.1}%"
    );
    std::thread::sleep(Duration::from_millis(200));
    let after_end = existing(&cold).len();
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        existing(&cold).len(),
        after_end,
        "deletions continued after DELETION_END"
    );
    assert!(ev.sigterm().success());

    assert_eq!(existing(&hot).len(), hot.len(), "hot blocks must survive");

    // Approximate LRU: what was deleted skews old. `cold` is ordered youngest -> oldest.
    let deleted: Vec<usize> = (0..cold.len())
        .filter(|&i| !cold[i].path.exists())
        .collect();
    let survivors: Vec<usize> = (0..cold.len()).filter(|&i| cold[i].path.exists()).collect();
    let mean = |v: &[usize]| v.iter().sum::<usize>() as f64 / v.len().max(1) as f64;
    let youngest_tenth = cold.len() / 10;
    let young_deleted = deleted.iter().filter(|&&i| i < youngest_tenth).count();
    eprintln!(
        "cold={} deleted={} mean_age_rank deleted={:.1} survivors={:.1} youngest-10% deleted={young_deleted}/{youngest_tenth}",
        cold.len(),
        deleted.len(),
        mean(&deleted),
        mean(&survivors)
    );
    assert!(
        mean(&deleted) - mean(&survivors) >= 0.15 * cold.len() as f64,
        "deleted files should skew clearly older than survivors"
    );
    assert!(
        young_deleted * 20 <= youngest_tenth.max(1),
        "{young_deleted} of the youngest {youngest_tenth} cold files were deleted"
    );
}

#[test]
#[ignore = "needs KVREAP_E2E_DIR, a small dedicated filesystem"]
fn threshold_never_deletes_hot_files_even_if_target_is_unreachable() {
    let e2e = e2e_dir();
    let dir = e2e.path.clone();
    let cache = Cache::new(&dir);
    let block_size = 128 * 1024;
    let mut hot = Vec::new();
    let mut i = 0;
    while usage_percent(&dir) < 88.0 {
        hot.push(cache.write_block(hash_for(2_000_000 + i), block_size, None));
        i += 1;
    }

    let env = vec![
        ("CLEANUP_THRESHOLD", "85".to_string()),
        ("TARGET_THRESHOLD", "70".to_string()),
        ("LOGGER_INTERVAL_SECONDS", "0.05".to_string()),
    ];
    let mut ev = Evictor::start(&dir, &env);
    assert!(
        ev.wait_for_log("DELETION_START", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    std::thread::sleep(Duration::from_secs(3));
    assert!(ev.sigterm().success());
    assert_eq!(existing(&hot).len(), hot.len());
    assert!(!ev.log().contains("DELETION_END"));
}

#[cfg(not(feature = "events"))]
#[test]
fn storage_events_endpoint_without_events_feature_warns_and_still_evicts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = Cache::new(tmp.path());
    let cold = cache.cold_blocks(10, 64);
    let mut env = always_evicting();
    env.push(("STORAGE_EVENTS_ENDPOINT", "tcp://127.0.0.1:5559".into()));
    let mut ev = Evictor::start(tmp.path(), &env);
    assert!(
        ev.wait_for_log("this build has no events support", Duration::from_secs(10)),
        "{}",
        ev.log()
    );
    assert!(
        wait_until(Duration::from_secs(30), || existing(&cold).is_empty()),
        "{}",
        ev.log()
    );
    assert!(ev.sigterm().success());
}

/// The default build must not link any cryptographic code (FIPS).
#[cfg(not(feature = "events"))]
#[test]
fn default_build_links_no_crypto() {
    let bin = fs::read(BIN).expect("read kvreap binary");
    let needles: [&[u8]; 9] = [
        b"sha1_init",
        b"sha1_loop",
        b"tweetnacl",
        b"crypto_box",
        b"curve_client",
        b"chacha",
        b"zmq_ctx_new",
        b"sodium_init",
        b"EVP_",
    ];
    let found: Vec<String> = needles
        .iter()
        .filter(|n| bin.windows(n.len()).any(|w| w == **n))
        .map(|n| String::from_utf8_lossy(n).into_owned())
        .collect();
    assert!(
        found.is_empty(),
        "crypto or zmq symbols in the default build: {found:?}"
    );
}
