//! Fixtures shared by the worker modules' tests.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::budget::Budget;
use crate::capacity::Samples;
use crate::chains::decode::KvEvent;
use crate::chains::{Chains, Tier};
use crate::config::Config;
use crate::controller::SharedState;
use crate::layout::{BlockHash, Shard};
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::worker::{Context, Worker};

pub(crate) struct Fixture {
    pub(crate) _tmp: tempfile::TempDir,
    pub(crate) cache: PathBuf,
}

pub(crate) fn age(path: &Path, secs: u64) {
    let t = SystemTime::now() - Duration::from_secs(secs);
    fs::File::options()
        .write(true)
        .open(path)
        .and_then(|f| f.set_times(fs::FileTimes::new().set_accessed(t).set_modified(t)))
        .expect("set times");
}

/// `cold` files aged `cold_age + i` seconds (so i=0 is youngest), `hot` files fresh.
pub(crate) fn fixture(cold: usize, hot: usize) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = tmp.path().join("cache");
    let base = cache.join("org-model_abcdef012345");
    fs::create_dir_all(&base).expect("mkdir");
    fs::write(base.join("config.json"), r#"{"model_name": "org/model"}"#).expect("write");
    for i in 0..cold + hot {
        let hash = 0x1000_0000_0000_0000u64 + i as u64 * 0x0001_0000_0000_0000;
        let hex = format!("{hash:016x}");
        let leaf = cache.join(format!(
            "org-model_abcdef012345_r0/{}/{}_g0",
            &hex[..3],
            &hex[3..5]
        ));
        fs::create_dir_all(&leaf).expect("mkdir");
        let f = leaf.join(format!("{hex}.bin"));
        fs::write(&f, vec![0u8; 100]).expect("write");
        if i < cold {
            age(&f, 7200 + i as u64);
        }
    }
    Fixture { _tmp: tmp, cache }
}

pub(crate) fn config(cache: &Path, extra: &[(&str, &str)]) -> Config {
    let mut env: HashMap<String, String> = [
        ("PVC_MOUNT_PATH", cache.to_str().expect("utf8")),
        ("CACHE_DIRECTORY", "."),
        ("NUM_CRAWLER_PROCESSES", "1"),
        ("DIR_CLEANUP_TTL_SECONDS", "0"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    Config::from_lookup(|k| env.get(k).cloned()).expect("config")
}

pub(crate) struct Harness {
    pub(crate) worker: Worker,
    pub(crate) shared: Arc<SharedState>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) samples: Arc<Samples>,
}

pub(crate) fn harness(cfg: Config) -> Harness {
    let shared = Arc::new(SharedState::default());
    let stats = Arc::new(Stats::default());
    let samples = Arc::new(Samples::default());
    let ctx = Context {
        budget: Arc::new(Budget::new(cfg.max_files_per_second)),
        config: Arc::new(cfg),
        shared: Arc::clone(&shared),
        stats: Arc::clone(&stats),
        samples: Arc::clone(&samples),
        shutdown: Arc::new(Shutdown::default()),
        chains: None,
    };
    Harness {
        worker: Worker::new(0, Shard::split(1)[0], ctx),
        shared,
        stats,
        samples,
    }
}

pub(crate) fn bins(cache: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![cache.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).expect("read_dir").flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "bin") {
                out.push(p.file_name().expect("name").to_string_lossy().into_owned());
            }
        }
    }
    out.sort();
    out
}

pub(crate) fn run_rounds(h: &mut Harness, n: usize) {
    for _ in 0..n {
        h.worker.round().expect("round");
    }
}

pub(crate) const BASE: u64 = 0xabcd_e000_0000_0000;

pub(crate) fn hash(n: u64) -> BlockHash {
    BlockHash(BASE + n)
}

/// One leaf dir of blocks `BASE + n`; `Some(secs)` ages a block, `None` keeps it hot.
pub(crate) fn bucket(blocks: &[(u64, Option<u64>)]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = tmp.path().join("cache");
    let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
    fs::create_dir_all(&leaf).expect("mkdir");
    for &(n, secs) in blocks {
        let f = leaf.join(format!("{:016x}.bin", BASE + n));
        fs::write(&f, b"x").expect("write");
        if let Some(secs) = secs {
            age(&f, secs);
        }
    }
    (tmp, cache)
}

pub(crate) fn names(ns: &[u64]) -> Vec<String> {
    ns.iter()
        .map(|n| format!("{:016x}.bin", BASE + n))
        .collect()
}

pub(crate) fn chain_harness(cache: &Path, chain: &[u64]) -> Harness {
    let mut h = harness(config(cache, &[]));
    let chains = Arc::new(Chains::new(1000, Tier::default()));
    chains.apply(&[stored(None, chain)]);
    h.worker.ctx.chains = Some(chains);
    h
}

/// A GPU `BlockStored` whose digests are the 8-byte hashes, so the
/// digest names the fixture's 16-hex-digit files.
pub(crate) fn stored(parent: Option<u64>, chain: &[u64]) -> KvEvent {
    KvEvent::Stored {
        parent: parent.map(hash),
        hashes: chain.iter().map(|&n| hash(n)).collect(),
        digests: chain
            .iter()
            .map(|&n| hash(n).0.to_be_bytes().to_vec())
            .collect(),
        medium: Some("GPU".into()),
        locality: None,
    }
}

pub(crate) fn chains(h: &Harness) -> &Chains {
    h.worker.ctx.chains.as_deref().expect("chains")
}
