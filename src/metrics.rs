//! Prometheus metrics on `METRICS_ADDR`, read from kvreap's own counters at
//! scrape time.
//!
//! A plain `std::net::TcpListener` answers `GET /metrics` with the OpenMetrics
//! text encoding over HTTP/1.1 and closes each connection.

use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric, text};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::registry::Registry;

use crate::budget::{Budget, OpKind};
use crate::chains::{Chains, LockStats};
use crate::controller::{Mode, SharedState, UNGRANTED};
use crate::shutdown::Shutdown;
use crate::stats::Stats;

const ACCEPT_POLL: Duration = Duration::from_millis(200);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_REQUEST: usize = 8 * 1024;
const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Everything a scrape reads.
#[derive(Debug)]
pub struct Sources {
    pub stats: Arc<Stats>,
    pub shared: Arc<SharedState>,
    pub budget: Arc<Budget>,
    pub chains: Option<Arc<Chains>>,
}

/// Binds `addr` and serves `/metrics` from `sources` on a thread until shutdown.
pub fn serve(
    addr: SocketAddr,
    sources: Sources,
    shutdown: Arc<Shutdown>,
) -> io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let bound = listener.local_addr()?;
    let mut registry = Registry::default();
    registry.register_collector(Box::new(sources));
    let handle = std::thread::Builder::new()
        .name("metrics".into())
        .spawn(move || accept_loop(&listener, &registry, &shutdown))?;
    Ok((bound, handle))
}

fn accept_loop(listener: &TcpListener, registry: &Registry, shutdown: &Shutdown) {
    while !shutdown.is_set() {
        match listener.accept() {
            Ok((stream, peer)) => {
                if let Err(e) = respond(stream, registry) {
                    tracing::debug!(%peer, error = %e, "metrics request failed");
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                shutdown.wait(ACCEPT_POLL);
            }
            Err(e) => {
                tracing::debug!(error = %e, "metrics accept failed");
                shutdown.wait(ACCEPT_POLL);
            }
        }
    }
}

fn respond(mut stream: TcpStream, registry: &Registry) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let request = read_head(&mut stream)?;
    let (status, content_type, body) = match request_target(&request) {
        Some(("GET", "/metrics")) => ("200 OK", CONTENT_TYPE, render(registry)),
        Some(("GET", _)) => ("404 Not Found", "text/plain", "not found\n".to_string()),
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "GET /metrics\n".to_string(),
        ),
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

/// The request line and headers, up to the blank line or `MAX_REQUEST` bytes.
fn read_head(stream: &mut TcpStream) -> io::Result<String> {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < MAX_REQUEST {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(buf.get(..n).unwrap_or_default());
    }
    Ok(String::from_utf8_lossy(&head).into_owned())
}

/// `(method, path)` of an HTTP request, without any query string.
fn request_target(request: &str) -> Option<(&str, &str)> {
    let mut parts = request.lines().next()?.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    Some((method, target.split('?').next().unwrap_or(target)))
}

fn render(registry: &Registry) -> String {
    let mut body = String::new();
    if let Err(e) = text::encode(&mut body, registry) {
        let _ = writeln!(body, "# encoding failed: {e}");
    }
    body
}

impl Collector for Sources {
    fn encode(&self, mut enc: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let s = &self.stats;
        counter(
            &mut enc,
            "kvreap_files_sampled",
            "Block files statted while sampling buckets.",
            Stats::get(&s.files_sampled),
        )?;
        counter(
            &mut enc,
            "kvreap_files_skipped_hot",
            "Sampled block files left alone as recently written or read.",
            Stats::get(&s.files_skipped_hot),
        )?;
        counter(
            &mut enc,
            "kvreap_files_deleted",
            "Block files unlinked.",
            Stats::get(&s.files_deleted),
        )?;
        counter(
            &mut enc,
            "kvreap_freed_bytes",
            "Bytes of block files unlinked.",
            Stats::get(&s.bytes_freed),
        )?;
        counter(
            &mut enc,
            "kvreap_dirs_removed",
            "Empty directories removed.",
            Stats::get(&s.dirs_removed),
        )?;
        counter(
            &mut enc,
            "kvreap_errors",
            "Filesystem operations that failed with an unexpected error.",
            Stats::get(&s.errors),
        )?;
        labeled_counters(
            &mut enc,
            "kvreap_metadata_ops",
            "Filesystem metadata operations issued.",
            "op",
            &[
                ("readdir", Stats::get(&s.readdir_ops)),
                ("stat", Stats::get(&s.stat_ops)),
                ("unlink", Stats::get(&s.unlink_ops)),
                ("rmdir", Stats::get(&s.rmdir_ops)),
            ],
        )?;
        {
            let mut family = enc.encode_descriptor(
                "kvreap_metadata_op_seconds",
                "Latency of filesystem metadata operations.",
                None,
                MetricType::Histogram,
            )?;
            for (op, kind) in [
                ("readdir", OpKind::Readdir),
                ("stat", OpKind::Stat),
                ("unlink", OpKind::Unlink),
            ] {
                s.timings
                    .op(kind)
                    .encode(family.encode_family(&[("op", op)])?)?;
            }
        }
        s.timings.prune.encode(enc.encode_descriptor(
            "kvreap_prune_seconds",
            "Time from entering the cleanup band to reaching the target.",
            None,
            MetricType::Histogram,
        )?)?;

        gauge(
            &mut enc,
            "kvreap_usage_ratio",
            "Volume usage, 0 to 1.",
            self.shared.usage() / 100.0,
        )?;
        {
            let current = self.shared.mode();
            let mut family = enc.encode_descriptor(
                "kvreap_mode",
                "1 for the controller's current mode.",
                None,
                MetricType::Gauge,
            )?;
            for (name, mode) in [
                ("idle", Mode::Idle),
                ("evicting", Mode::Evicting),
                ("emergency", Mode::Emergency),
            ] {
                ConstGauge::new(u64::from(current == mode))
                    .encode(family.encode_family(&[("mode", name)])?)?;
            }
        }
        let grant = self.shared.to_free();
        if grant != UNGRANTED {
            gauge(
                &mut enc,
                "kvreap_grant_bytes",
                "Bytes the current prune may still delete.",
                grant as f64,
            )?;
        }
        gauge(
            &mut enc,
            "kvreap_op_rate_limit",
            "Metadata operations per second the limiter allows.",
            self.budget.rate(),
        )?;

        if let Some(chains) = &self.chains {
            encode_chains(&mut enc, chains)?;
        }
        Ok(())
    }
}

fn encode_chains(enc: &mut DescriptorEncoder, chains: &Chains) -> Result<(), std::fmt::Error> {
    let c = &chains.stats;
    gauge(
        enc,
        "kvreap_index_blocks",
        "Blocks in the prefix-chain index.",
        chains.index_blocks() as f64,
    )?;
    counter(
        enc,
        "kvreap_kv_event_batches",
        "KV event batches applied.",
        Stats::get(&c.batches),
    )?;
    counter(
        enc,
        "kvreap_kv_event_decode_errors",
        "KV event payloads that did not decode.",
        Stats::get(&c.decode_errors),
    )?;
    counter(
        enc,
        "kvreap_kv_event_gaps",
        "Live batches whose sequence number skipped ahead.",
        Stats::get(&c.gaps),
    )?;
    counter(
        enc,
        "kvreap_kv_event_resets",
        "Publishers whose sequence numbers went backwards.",
        Stats::get(&c.resets),
    )?;
    counter(
        enc,
        "kvreap_kv_event_replays",
        "Replays completed from a publisher's replay socket.",
        Stats::get(&c.replays),
    )?;
    counter(
        enc,
        "kvreap_kv_event_replay_failures",
        "Replays that failed.",
        Stats::get(&c.replay_failures),
    )?;
    counter(
        enc,
        "kvreap_kv_event_replayed_batches",
        "Batches applied from replays.",
        Stats::get(&c.replayed_batches),
    )?;
    counter(
        enc,
        "kvreap_kv_events_lost",
        "Batches never applied: missed live and gone from the replay buffer.",
        Stats::get(&c.events_lost),
    )?;
    counter(
        enc,
        "kvreap_blocks_stored",
        "Blocks marked on disk by store events.",
        Stats::get(&c.blocks_stored),
    )?;
    counter(
        enc,
        "kvreap_blocks_undigested",
        "Stored blocks announced without a full hash.",
        Stats::get(&c.undigested),
    )?;
    labeled_counters(
        enc,
        "kvreap_chain_deletions",
        "Blocks deleted, by their position in their prefix chain.",
        "position",
        &[
            ("root", Stats::get(&c.deleted_root)),
            ("orphan", Stats::get(&c.deleted_orphan)),
            ("internal", Stats::get(&c.deleted_internal)),
            ("leaf", Stats::get(&c.deleted_leaf)),
            ("untracked", Stats::get(&c.deleted_untracked)),
        ],
    )?;
    counter(
        enc,
        "kvreap_chain_cascaded",
        "Blocks deleted below the block an eviction picked.",
        Stats::get(&c.cascaded),
    )?;
    counter(
        enc,
        "kvreap_chain_young_edges",
        "Leaf edges held back as recently written.",
        Stats::get(&c.young_edges),
    )?;
    counter(
        enc,
        "kvreap_chain_young_fallbacks",
        "Recently written edges deleted because nothing older was left.",
        Stats::get(&c.young_fallbacks),
    )?;
    counter(
        enc,
        "kvreap_chain_deferrals",
        "Chain interiors deferred by tail-first eviction.",
        Stats::get(&c.deferrals),
    )?;
    counter(
        enc,
        "kvreap_chain_vanished",
        "Indexed blocks whose files were already gone.",
        Stats::get(&c.vanished),
    )?;
    counter(
        enc,
        "kvreap_chain_claim_conflicts",
        "Edges skipped because another worker was deleting them.",
        Stats::get(&c.claim_conflicts),
    )?;
    lock_seconds(
        enc,
        "kvreap_index_lock_wait_seconds",
        "Time spent waiting for the chain index lock.",
        |l| &l.wait_us,
        chains,
    )?;
    lock_seconds(
        enc,
        "kvreap_index_lock_hold_seconds",
        "Time the chain index lock was held.",
        |l| &l.hold_us,
        chains,
    )
}

fn counter(
    enc: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    value: u64,
) -> Result<(), std::fmt::Error> {
    ConstCounter::new(value).encode(enc.encode_descriptor(name, help, None, MetricType::Counter)?)
}

fn gauge(
    enc: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    value: f64,
) -> Result<(), std::fmt::Error> {
    ConstGauge::new(value).encode(enc.encode_descriptor(name, help, None, MetricType::Gauge)?)
}

fn labeled_counters(
    enc: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    label: &str,
    values: &[(&str, u64)],
) -> Result<(), std::fmt::Error> {
    let mut family = enc.encode_descriptor(name, help, None, MetricType::Counter)?;
    for &(value, n) in values {
        ConstCounter::new(n).encode(family.encode_family(&[(label, value)])?)?;
    }
    Ok(())
}

fn lock_seconds(
    enc: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    field: impl Fn(&LockStats) -> &std::sync::atomic::AtomicU64,
    chains: &Chains,
) -> Result<(), std::fmt::Error> {
    let mut family = enc.encode_descriptor(name, help, None, MetricType::Counter)?;
    for (side, stats) in [
        ("events", &chains.stats.events_lock),
        ("workers", &chains.stats.worker_lock),
    ] {
        let seconds = Stats::get(field(stats)) as f64 / 1e6;
        ConstCounter::new(seconds).encode(family.encode_family(&[("side", side)])?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    use prometheus_client::registry::Registry;

    use crate::budget::{Budget, OpKind};
    use crate::chains::Chains;
    use crate::chains::testing::{h, stored};
    use crate::config::ChainPolicy;
    use crate::controller::{Mode, SharedState};
    use crate::metrics::{Sources, render, request_target, serve};
    use crate::shutdown::Shutdown;
    use crate::stats::Stats;

    fn sources(chains: Option<Arc<Chains>>) -> Sources {
        Sources {
            stats: Arc::new(Stats::default()),
            shared: Arc::new(SharedState::default()),
            budget: Arc::new(Budget::new(0.0)),
            chains,
        }
    }

    fn scrape(s: Sources) -> String {
        let mut registry = Registry::default();
        registry.register_collector(Box::new(s));
        render(&registry)
    }

    fn get(addr: std::net::SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout");
        stream.write_all(request.as_bytes()).expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }

    #[test]
    fn scrape_reports_the_counters_kvreap_already_keeps() {
        let s = sources(None);
        Stats::add(&s.stats.files_deleted, 3);
        Stats::add(&s.stats.bytes_freed, 4096);
        Stats::add(&s.stats.unlink_ops, 5);
        s.stats.timings.op(OpKind::Unlink).observe(0.002);
        s.stats.timings.prune.observe(12.0);
        s.shared.set_mode(Mode::Evicting);
        s.shared.set_usage(87.5);
        let body = scrape(s);
        for line in [
            "kvreap_files_deleted_total 3",
            "kvreap_freed_bytes_total 4096",
            "kvreap_metadata_ops_total{op=\"unlink\"} 5",
            "kvreap_metadata_op_seconds_count{op=\"unlink\"} 1",
            "kvreap_metadata_op_seconds_count{op=\"stat\"} 0",
            "kvreap_prune_seconds_count 1",
            "kvreap_prune_seconds_sum 12.0",
            "kvreap_usage_ratio 0.875",
            "kvreap_mode{mode=\"evicting\"} 1",
            "kvreap_mode{mode=\"idle\"} 0",
            "# EOF",
        ] {
            assert!(body.contains(line), "missing {line:?} in\n{body}");
        }
        assert!(
            !body.contains("kvreap_grant_bytes"),
            "no grant before the first reading"
        );
        assert!(
            !body.contains("kvreap_kv_event"),
            "no chain metrics without an index"
        );
    }

    #[test]
    fn scrape_reports_chain_metrics_when_events_are_followed() {
        let chains = Arc::new(Chains::new(100, ChainPolicy::Radix, 2, None));
        chains.apply(&[stored(None, &[1, 2, 3], "STORAGE")]);
        chains.deleted(h(3));
        chains.deleted(h(1));
        let s = sources(Some(Arc::clone(&chains)));
        s.shared.set_to_free(1000);
        let body = scrape(s);
        for line in [
            "kvreap_index_blocks 2",
            "kvreap_blocks_stored_total 3",
            "kvreap_chain_deletions_total{position=\"leaf\"} 1",
            "kvreap_chain_deletions_total{position=\"root\"} 1",
            "kvreap_chain_deletions_total{position=\"internal\"} 0",
            "kvreap_index_lock_wait_seconds_total{side=\"workers\"}",
            "kvreap_grant_bytes 1000",
        ] {
            assert!(body.contains(line), "missing {line:?} in\n{body}");
        }
    }

    #[test]
    fn request_target_strips_queries_and_rejects_garbage() {
        assert_eq!(
            request_target("GET /metrics?x=1 HTTP/1.1\r\nHost: a\r\n\r\n"),
            Some(("GET", "/metrics"))
        );
        assert_eq!(request_target("POST / HTTP/1.1\r\n"), Some(("POST", "/")));
        assert_eq!(request_target(""), None);
        assert_eq!(request_target("GET\r\n"), None);
    }

    #[test]
    fn server_answers_metrics_404_and_405_then_stops_on_shutdown() {
        let s = sources(None);
        Stats::add(&s.stats.files_deleted, 7);
        let shutdown = Arc::new(Shutdown::default());
        let (addr, handle) = serve(
            "127.0.0.1:0".parse().expect("addr"),
            s,
            Arc::clone(&shutdown),
        )
        .expect("serve");

        let ok = get(addr, "GET /metrics HTTP/1.1\r\nHost: kvreap\r\n\r\n");
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(
            ok.contains("Content-Type: application/openmetrics-text"),
            "{ok}"
        );
        assert!(ok.contains("kvreap_files_deleted_total 7"), "{ok}");
        let length: usize = ok
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .expect("length")
            .parse()
            .expect("number");
        assert_eq!(ok.split_once("\r\n\r\n").expect("body").1.len(), length);

        assert!(get(addr, "GET / HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 404"));
        assert!(get(addr, "POST /metrics HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 405"));

        shutdown.trigger();
        handle.join().expect("join");
    }
}
