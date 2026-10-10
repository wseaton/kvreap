mod atime;
mod budget;
mod capacity;
mod chains;
mod config;
mod controller;
#[cfg(feature = "events")]
mod events;
mod fsops;
mod layout;
mod shutdown;
mod stats;
mod worker;

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use signal_hook::consts::{SIGINT, SIGTERM};
use tracing_subscriber::fmt::writer::MakeWriterExt;

use crate::atime::AtimeBehavior;
use crate::budget::Budget;
use crate::capacity::{Sampler, Samples, statvfs_overreports};
use crate::config::Config;
use crate::controller::{
    GRANT_MARGIN_PERCENT, Hysteresis, Mode, RecentFrees, SharedState, USAGE_LAG, UsageSource,
    disk_usage, next_budget,
};
use crate::layout::Shard;
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::worker::{Context, Removed, Worker};

const MOUNT_WAIT: Duration = Duration::from_secs(60);
const MOUNT_POLL: Duration = Duration::from_secs(2);
const STATUS_INTERVAL: Duration = Duration::from_secs(30);

fn init_logging(config: &Config) {
    let filter = tracing_subscriber::EnvFilter::new(config.log_level.as_filter());
    let file = config.log_file_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .ok()
    });
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(false);
    match file {
        Some(f) => builder.with_writer(std::io::stdout.and(Arc::new(f))).init(),
        None => builder.with_writer(std::io::stdout).init(),
    }
}

fn wait_for_mount(path: &Path, shutdown: &Shutdown) -> bool {
    let deadline = Instant::now() + MOUNT_WAIT;
    while Instant::now() < deadline {
        if path.exists() {
            tracing::info!(path = %path.display(), "PVC mount path is ready");
            return true;
        }
        if shutdown.wait(MOUNT_POLL) {
            return false;
        }
        tracing::info!(path = %path.display(), "still waiting for mount");
    }
    false
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

fn controller_loop(
    config: Arc<Config>,
    source: UsageSource,
    shared: Arc<SharedState>,
    shutdown: Arc<Shutdown>,
) {
    let hysteresis = Hysteresis::new(config.cleanup_threshold, config.target_threshold);
    let mut mode = Mode::Idle;
    let mut have_usage = false;
    let mut recent_frees = RecentFrees::default();
    loop {
        match source.read() {
            Ok(Some(usage)) => {
                let pct = usage.percent();
                if !have_usage {
                    have_usage = true;
                    if let UsageSource::Sampled { .. } = source {
                        tracing::info!(
                            usage = format!("{pct:.2}%"),
                            used_gb = format!("{:.2}", usage.used_bytes as f64 / GIB),
                            capacity_gb = format!("{:.2}", usage.total_bytes as f64 / GIB),
                            "sampled usage estimate ready"
                        );
                    }
                }
                shared.set_usage(pct);
                let recent = recent_frees.observe(Instant::now(), shared.freed_total(), USAGE_LAG);
                let next = hysteresis.next(mode, pct);
                shared.set_to_free(next_budget(
                    next.is_evicting(),
                    shared.to_free(),
                    usage.above(config.target_threshold),
                    recent,
                    usage.total_bytes / 100 * GRANT_MARGIN_PERCENT,
                ));
                if next != mode {
                    let previous = mode;
                    mode = next;
                    shared.set_mode(mode);
                    let (used, total) = (
                        usage.used_bytes as f64 / GIB,
                        usage.total_bytes as f64 / GIB,
                    );
                    match (previous.is_evicting(), next.is_evicting()) {
                        (false, true) => tracing::info!(
                            "DELETION_START: timestamp={:.3}, usage={pct:.2}%, used={used:.2}GB, total={total:.2}GB",
                            unix_now()
                        ),
                        (true, false) => tracing::info!(
                            "DELETION_END: timestamp={:.3}, usage={pct:.2}%, used={used:.2}GB, total={total:.2}GB",
                            unix_now()
                        ),
                        _ => {}
                    }
                    if next == Mode::Emergency {
                        tracing::warn!(
                            usage = pct,
                            "usage in emergency band, deleting without pacing"
                        );
                    }
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "statvfs failed"),
        }
        if shutdown.wait(config.usage_poll_interval) {
            break;
        }
    }
    shared.set_mode(Mode::Idle);
}

fn log_atime_behavior(config: &Config) {
    let cache = config.cache_path();
    let dir = if cache.is_dir() {
        cache
    } else {
        config.pvc_mount_path.clone()
    };
    match atime::probe(&dir) {
        Ok(AtimeBehavior::Tracked) => tracing::info!(
            dir = %dir.display(),
            "atime probe: reads move atime, hot protection follows last access"
        ),
        Ok(AtimeBehavior::Untracked) => tracing::warn!(
            dir = %dir.display(),
            "atime probe: reads do not move atime on this mount, so hot protection means recently written and eviction is oldest-written-first"
        ),
        Err(e) => tracing::warn!(
            dir = %dir.display(),
            error = %e,
            "atime probe failed, atime behavior unknown"
        ),
    }
}

/// Picks the usage source and, for `CAPACITY_BYTES`, starts the sampler thread.
fn start_usage_source(
    config: &Config,
    samples: &Arc<Samples>,
    stats: &Arc<Stats>,
    shutdown: &Arc<Shutdown>,
) -> anyhow::Result<(UsageSource, Option<JoinHandle<()>>)> {
    let Some(capacity) = config.capacity_bytes else {
        return Ok((UsageSource::Statvfs(config.pvc_mount_path.clone()), None));
    };
    match disk_usage(&config.pvc_mount_path) {
        Ok(u) if statvfs_overreports(u.total_bytes, capacity) => tracing::warn!(
            statvfs_total_gb = format!("{:.2}", u.total_bytes as f64 / GIB),
            capacity_gb = format!("{:.2}", capacity.get() as f64 / GIB),
            "statvfs reports more than 10x CAPACITY_BYTES: the filesystem does not report this volume's size (VAST before the first write, hostPath, emptyDir); using sampled usage"
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "statvfs failed"),
    }
    tracing::info!(
        capacity_bytes = capacity.get(),
        "CAPACITY_BYTES set: usage is estimated from bucket samples, not statvfs"
    );
    let sampler = Sampler {
        cache: config.cache_path(),
        bucket_len: config.hex_bucket_len,
        samples: Arc::clone(samples),
        stats: Arc::clone(stats),
        shutdown: Arc::clone(shutdown),
    };
    let handle = spawn("sampler".into(), move || sampler.run())?;
    Ok((
        UsageSource::Sampled {
            capacity,
            samples: Arc::clone(samples),
        },
        Some(handle),
    ))
}

fn log_status(shared: &SharedState, budget: &Budget, stats: &Stats) {
    tracing::info!(
        usage = format!("{:.1}%", shared.usage()),
        mode = ?shared.mode(),
        op_rate = format!("{:.0}/s", budget.rate()),
        files_sampled = Stats::get(&stats.files_sampled),
        files_skipped_hot = Stats::get(&stats.files_skipped_hot),
        files_deleted = Stats::get(&stats.files_deleted),
        gb_freed = format!("{:.2}", Stats::get(&stats.bytes_freed) as f64 / GIB),
        dirs_removed = Stats::get(&stats.dirs_removed),
        readdir_ops = Stats::get(&stats.readdir_ops),
        stat_ops = Stats::get(&stats.stat_ops),
        unlink_ops = Stats::get(&stats.unlink_ops),
        rmdir_ops = Stats::get(&stats.rmdir_ops),
        errors = Stats::get(&stats.errors),
        "status"
    );
}

fn spawn<F: FnOnce() + Send + 'static>(name: String, f: F) -> anyhow::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(f)
        .with_context(|| format!("spawning {name}"))
}

type EventsHandle = (Option<mpsc::Sender<Removed>>, Option<JoinHandle<()>>);

type ChainsHandle = (Option<Arc<chains::Chains>>, Option<JoinHandle<()>>);

fn start_chains(config: &Config, shutdown: &Arc<Shutdown>) -> anyhow::Result<ChainsHandle> {
    if config.kv_events_endpoints.is_empty() {
        return Ok((None, None));
    }
    let chains = Arc::new(chains::Chains::new(
        chains::INDEX_CAP,
        config.chain_policy,
        config.chain_max_deferrals,
        config.kv_events_disk_medium.clone(),
    ));
    tracing::info!(
        endpoints = config.kv_events_endpoints.join(","),
        policy = ?config.chain_policy,
        max_deferrals = config.chain_max_deferrals,
        disk_medium = config.kv_events_disk_medium.as_deref().unwrap_or("any"),
        replay_port = ?config.kv_events_replay_port,
        "building prefix-chain index from KV cache events"
    );
    let handle = {
        let (endpoints, replay_port, chains, shutdown) = (
            config.kv_events_endpoints.clone(),
            config.kv_events_replay_port,
            Arc::clone(&chains),
            Arc::clone(shutdown),
        );
        spawn("kv-events".into(), move || {
            if let Err(e) = chains::subscribe(&endpoints, replay_port, &chains, &shutdown) {
                tracing::warn!(error = %e, "KV events subscriber failed, chain index stays empty");
            }
        })?
    };
    Ok((Some(chains), Some(handle)))
}

#[cfg(feature = "events")]
fn start_events(config: &Config) -> anyhow::Result<EventsHandle> {
    let Some(endpoint) = &config.storage_events_endpoint else {
        return Ok((None, None));
    };
    match events::Publisher::bind(endpoint) {
        Ok(publisher) => {
            tracing::info!(endpoint, "storage event publisher bound");
            let (tx, rx) = mpsc::channel();
            let batch = config.event_batch_size;
            let handle = spawn("events".into(), move || events::run(publisher, rx, batch))?;
            Ok((Some(tx), Some(handle)))
        }
        Err(e) => {
            tracing::warn!(endpoint, error = %e, "failed to create storage event publisher");
            Ok((None, None))
        }
    }
}

#[cfg(not(feature = "events"))]
fn start_events(config: &Config) -> anyhow::Result<EventsHandle> {
    if let Some(endpoint) = &config.storage_events_endpoint {
        tracing::warn!(
            endpoint,
            "STORAGE_EVENTS_ENDPOINT is set but this build has no events support; BlockRemoved events are not published"
        );
    }
    Ok((None, None))
}

fn run(config: Config) -> anyhow::Result<()> {
    let config = Arc::new(config);
    let shutdown = Arc::new(Shutdown::default());

    let mut signals = signal_hook::iterator::Signals::new([SIGTERM, SIGINT])
        .context("installing signal handlers")?;
    let signal_handle = signals.handle();
    let signal_thread = {
        let shutdown = Arc::clone(&shutdown);
        spawn("signals".into(), move || {
            if let Some(sig) = signals.forever().next() {
                tracing::info!(signal = sig, "shutting down");
                shutdown.trigger();
            }
        })?
    };

    if !wait_for_mount(&config.pvc_mount_path, &shutdown) {
        signal_handle.close();
        let _ = signal_thread.join();
        anyhow::bail!(
            "PVC mount path {} not available after {MOUNT_WAIT:?}",
            config.pvc_mount_path.display()
        );
    }

    log_atime_behavior(&config);

    let shared = Arc::new(SharedState::default());
    let stats = Arc::new(Stats::default());
    let samples = Arc::new(Samples::default());
    let budget = Arc::new(Budget::new(config.max_files_per_second));

    let (events_tx, events_thread) = start_events(&config)?;
    let (chains, chains_thread) = start_chains(&config, &shutdown)?;
    let (source, sampler) = start_usage_source(&config, &samples, &stats, &shutdown)?;

    let controller = {
        let (config, shared, shutdown) = (
            Arc::clone(&config),
            Arc::clone(&shared),
            Arc::clone(&shutdown),
        );
        spawn("controller".into(), move || {
            controller_loop(config, source, shared, shutdown)
        })?
    };

    let mut workers = Vec::new();
    for (id, shard) in Shard::split(config.workers.get()).into_iter().enumerate() {
        let ctx = Context {
            config: Arc::clone(&config),
            shared: Arc::clone(&shared),
            budget: Arc::clone(&budget),
            stats: Arc::clone(&stats),
            samples: Arc::clone(&samples),
            shutdown: Arc::clone(&shutdown),
            events: events_tx.clone(),
            chains: chains.clone(),
        };
        workers.push(spawn(format!("worker-{id}"), move || {
            Worker::new(id, shard, ctx).run()
        })?);
    }
    drop(events_tx);

    while !shutdown.wait(STATUS_INTERVAL) {
        log_status(&shared, &budget, &stats);
        if let Some(c) = &chains {
            c.log_status();
        }
    }

    for w in workers {
        let _ = w.join();
    }
    let _ = controller.join();
    if let Some(t) = sampler {
        let _ = t.join();
    }
    if let Some(t) = events_thread {
        let _ = t.join();
    }
    if let Some(t) = chains_thread {
        let _ = t.join();
    }
    signal_handle.close();
    let _ = signal_thread.join();
    log_status(&shared, &budget, &stats);
    if let Some(c) = &chains {
        c.log_status();
    }
    tracing::info!("all threads stopped");
    Ok(())
}

fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return ExitCode::FAILURE;
        }
    };
    init_logging(&config);
    for var in Config::ignored_vars_set(|k| std::env::var(k).ok()) {
        tracing::info!(var, "accepted for compatibility, not used");
    }
    tracing::info!(
        mount = %config.pvc_mount_path.display(),
        cache = %config.cache_path().display(),
        cleanup = config.cleanup_threshold.get(),
        target = config.target_threshold.get(),
        workers = config.workers.get(),
        max_files_per_second = config.max_files_per_second,
        hot_threshold_secs = config.hot_threshold.as_secs(),
        dry_run = config.dry_run,
        capacity_bytes = config.capacity_bytes.map(|c| c.get()),
        version = env!("CARGO_PKG_VERSION"),
        "kvreap starting"
    );
    match run(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}
