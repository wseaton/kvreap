//! Follows each vLLM pod's KV events PUB socket and replays what it missed.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::chains::Chains;
use crate::chains::decode::decode_batch;
use crate::shutdown::Shutdown;
use crate::stats::Stats;

const RECV_TIMEOUT: Duration = Duration::from_millis(500);
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const RESOLVE_INTERVAL: Duration = Duration::from_secs(15);
const RESOLVE_RETRY: Duration = Duration::from_secs(1);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(120);
const REPLAY_IDLE: Duration = Duration::from_secs(2);
const REPLAY_COOLDOWN: Duration = Duration::from_secs(30);
const REPLAY_ATTEMPTS: u32 = 3;
const MAX_CONCURRENT_REPLAYS: usize = 8;
/// Subscribes to every address the endpoints resolve to and feeds decoded
/// batches into `chains` until shutdown.
///
/// An endpoint may name a headless Service: it is resolved every
/// `RESOLVE_INTERVAL` (every `RESOLVE_RETRY` while nothing resolves), each
/// address gets its own SUB socket (so one vLLM that is down or slow does not
/// hold up the others), and sockets for addresses that disappear are dropped.
/// A failed lookup keeps the last addresses.
///
/// With `replay_port`, missed batches are fetched from each publisher's
/// `replay_endpoint` on that port, the way llm-d's router does: on connect,
/// on a sequence gap, on joining mid-stream, and after the publisher restarts.
pub fn subscribe(
    endpoints: &[String],
    replay_port: Option<u16>,
    chains: &Chains,
    shutdown: &Shutdown,
) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Feed>(1024);
        let replays = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REPLAYS));
        let mut resolved: HashMap<&str, HashSet<SocketAddr>> = HashMap::new();
        let mut followers: HashMap<SocketAddr, tokio::task::JoinHandle<()>> = HashMap::new();
        let mut next_resolve = Instant::now();
        while !shutdown.is_set() {
            if Instant::now() >= next_resolve {
                for endpoint in endpoints {
                    match resolve(endpoint).await {
                        Ok(addrs) => {
                            resolved.insert(endpoint.as_str(), addrs);
                        }
                        Err(e) => {
                            tracing::warn!(endpoint, error = %e, "KV events endpoint lookup failed");
                        }
                    }
                }
                let desired: HashSet<SocketAddr> = resolved.values().flatten().copied().collect();
                let current: HashSet<SocketAddr> = followers.keys().copied().collect();
                let (add, remove) = plan_peers(&current, &desired);
                for addr in remove {
                    if let Some(handle) = followers.remove(&addr) {
                        handle.abort();
                        tracing::info!(%addr, "KV events peer gone");
                    }
                }
                for addr in add {
                    let peer = Peer {
                        addr,
                        replay: replay_port.map(|port| SocketAddr::new(addr.ip(), port)),
                        replays: Arc::clone(&replays),
                        tx: tx.clone(),
                    };
                    followers.insert(addr, tokio::spawn(follow(peer)));
                }
                next_resolve = Instant::now()
                    + if followers.is_empty() {
                        RESOLVE_RETRY
                    } else {
                        RESOLVE_INTERVAL
                    };
            }
            if let Ok(Some(feed)) = tokio::time::timeout(RECV_TIMEOUT, rx.recv()).await {
                let depth = u64::try_from(rx.len()).unwrap_or(u64::MAX);
                chains.stats.queue_depth_max.fetch_max(depth, Ordering::Relaxed);
                record(chains, feed);
            }
        }
        for handle in followers.into_values() {
            handle.abort();
        }
    });
    Ok(())
}

/// Addresses of a `tcp://host:port` endpoint.
async fn resolve(endpoint: &str) -> std::io::Result<HashSet<SocketAddr>> {
    let host_port = endpoint.strip_prefix("tcp://").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{endpoint} is not a tcp:// endpoint"),
        )
    })?;
    Ok(tokio::net::lookup_host(host_port).await?.collect())
}

/// Addresses to start following and to drop.
fn plan_peers(
    current: &HashSet<SocketAddr>,
    desired: &HashSet<SocketAddr>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let mut add: Vec<SocketAddr> = desired.difference(current).copied().collect();
    let mut remove: Vec<SocketAddr> = current.difference(desired).copied().collect();
    add.sort();
    remove.sort();
    (add, remove)
}

/// What a peer task reports to the subscriber loop.
#[derive(Debug)]
enum Feed {
    Batch(Vec<u8>),
    Gap,
    Reset,
    /// Sequence numbers that were never applied and can no longer be replayed.
    Lost(u64),
    Replayed(u64),
    ReplayFailed,
}

fn record(chains: &Chains, feed: Feed) {
    let s = &chains.stats;
    match feed {
        Feed::Batch(payload) => apply_payload(chains, &payload),
        Feed::Gap => Stats::add(&s.gaps, 1),
        Feed::Reset => Stats::add(&s.resets, 1),
        Feed::Lost(n) => Stats::add(&s.events_lost, n),
        Feed::Replayed(n) => {
            Stats::add(&s.replays, 1);
            Stats::add(&s.replayed_batches, n);
        }
        Feed::ReplayFailed => Stats::add(&s.replay_failures, 1),
    }
}

fn apply_payload(chains: &Chains, payload: &[u8]) {
    match decode_batch(payload) {
        Ok(events) => {
            Stats::add(&chains.stats.batches, 1);
            chains.apply(&events);
        }
        Err(e) => {
            Stats::add(&chains.stats.decode_errors, 1);
            tracing::debug!(error = %e, "undecodable KV events batch");
        }
    }
}

/// Sequence numbers seen from one publisher.
#[derive(Debug, Default)]
struct Sequence {
    applied: Option<u64>,
    live: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Live {
    Apply,
    Skip,
    /// Behind the publisher: replay from `next()` first.
    Replay,
    /// The publisher started over (sequence went backwards): replay from 0.
    Restart,
}

impl Sequence {
    /// What to do with a live message before applying it.
    fn live(&mut self, seq: u64) -> Live {
        match self.live.replace(seq) {
            Some(prev) if seq == prev => return Live::Skip,
            Some(prev) if seq < prev => {
                self.applied = None;
                return Live::Restart;
            }
            _ => {}
        }
        match seq.cmp(&self.next()) {
            std::cmp::Ordering::Less => Live::Skip,
            std::cmp::Ordering::Equal => Live::Apply,
            std::cmp::Ordering::Greater => Live::Replay,
        }
    }

    /// The first sequence number not yet applied.
    fn next(&self) -> u64 {
        self.applied.map_or(0, |last| last.saturating_add(1))
    }

    /// Marks `seq` applied. `None` if it already was, else how many sequence
    /// numbers before it were skipped.
    fn advance(&mut self, seq: u64) -> Option<u64> {
        let lost = seq.checked_sub(self.next())?;
        self.applied = Some(seq);
        Some(lost)
    }
}

/// `[topic, seq, payload]`, as vLLM's PUB and replay sockets send it.
fn sequenced<F: AsRef<[u8]>>(frames: Vec<F>) -> Option<(u64, F)> {
    let [_topic, seq, payload] = <[F; 3]>::try_from(frames).ok()?;
    let seq = u64::from_be_bytes(<[u8; 8]>::try_from(seq.as_ref()).ok()?);
    Some((seq, payload))
}

#[derive(Debug, thiserror::Error)]
enum ReplayError {
    #[error(transparent)]
    Zmq(#[from] zeromq::ZmqError),
    #[error("replay endpoint did not accept a connection within {REPLAY_IDLE:?}")]
    Unreachable,
    #[error("replay reply is not [topic, seq, payload]")]
    Malformed,
    #[error("no progress after {REPLAY_ATTEMPTS} attempts")]
    Stalled,
    #[error("replay did not finish within {REPLAY_TIMEOUT:?}")]
    Timeout,
}

struct Peer {
    addr: SocketAddr,
    replay: Option<SocketAddr>,
    replays: Arc<tokio::sync::Semaphore>,
    tx: tokio::sync::mpsc::Sender<Feed>,
}

/// Forwards the batches from one publisher to the subscriber loop in
/// sequence order, replaying what the SUB socket missed.
async fn follow(peer: Peer) {
    use zeromq::{Socket, SocketRecv, SubSocket};

    let endpoint = format!("tcp://{}", peer.addr);
    let mut sequence = Sequence::default();
    let mut replay_after = Instant::now();
    let mut wait = RECONNECT_MIN;
    loop {
        let mut sub = SubSocket::new();
        let connected = match sub.subscribe("").await {
            Ok(()) => sub.connect(&endpoint).await,
            Err(e) => Err(e),
        };
        if let Err(e) = connected {
            tracing::warn!(endpoint, error = %e, retry_in = ?wait, "KV events connect failed");
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RECONNECT_MAX);
            continue;
        }
        tracing::info!(endpoint, "subscribed to KV cache events");
        catch_up(&peer, &mut sequence, &mut replay_after).await;
        loop {
            let message = match sub.recv().await {
                Ok(message) => message,
                Err(e) => {
                    tracing::debug!(endpoint, error = %e, "KV events receive failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let Some((seq, payload)) = sequenced(message.into_vec()) else {
                continue;
            };
            match sequence.live(seq) {
                Live::Skip => continue,
                Live::Apply => {}
                live @ (Live::Replay | Live::Restart) => {
                    let note = match live {
                        Live::Restart => Some(Feed::Reset),
                        _ if sequence.applied.is_some() => Some(Feed::Gap),
                        _ => None,
                    };
                    if let Some(note) = note {
                        tracing::info!(
                            endpoint,
                            seq,
                            next = sequence.next(),
                            ?note,
                            "KV events out of sequence"
                        );
                        let _ = peer.tx.send(note).await;
                    }
                    catch_up(&peer, &mut sequence, &mut replay_after).await;
                }
            }
            if let Some(lost) = sequence.advance(seq) {
                if lost > 0 {
                    let _ = peer.tx.send(Feed::Lost(lost)).await;
                }
                if peer.tx.send(Feed::Batch(payload.to_vec())).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Replays from `sequence.next()` unless the last replay failed less than
/// `REPLAY_COOLDOWN` ago.
async fn catch_up(peer: &Peer, sequence: &mut Sequence, replay_after: &mut Instant) {
    let Some(endpoint) = peer.replay else {
        return;
    };
    if Instant::now() < *replay_after {
        return;
    }
    let Ok(_permit) = peer.replays.acquire().await else {
        return;
    };
    let from = sequence.next();
    let feed = match replay(endpoint, sequence, &peer.tx).await {
        Ok(batches) => {
            tracing::debug!(%endpoint, from, batches, "KV events replayed");
            Feed::Replayed(batches)
        }
        Err(e) => {
            tracing::warn!(%endpoint, from, error = %e, retry_after = ?REPLAY_COOLDOWN, "KV events replay failed");
            *replay_after = Instant::now() + REPLAY_COOLDOWN;
            Feed::ReplayFailed
        }
    };
    let _ = peer.tx.send(feed).await;
}

/// Asks the publisher's replay socket for everything from `sequence.next()`
/// on, retrying from where it stopped until it sends the end marker.
async fn replay(
    endpoint: SocketAddr,
    sequence: &mut Sequence,
    tx: &tokio::sync::mpsc::Sender<Feed>,
) -> Result<u64, ReplayError> {
    let deadline = Instant::now() + REPLAY_TIMEOUT;
    let mut batches = 0;
    let mut stalled = 0;
    loop {
        let before = batches;
        let result = replay_attempt(endpoint, sequence, tx, deadline, &mut batches).await;
        stalled = if batches > before { 0 } else { stalled + 1 };
        match result {
            Ok(true) => return Ok(batches),
            Ok(false) => {}
            Err(e) if stalled >= REPLAY_ATTEMPTS => return Err(e),
            Err(e) => tracing::debug!(%endpoint, error = %e, "KV events replay attempt failed"),
        }
        if stalled >= REPLAY_ATTEMPTS {
            return Err(ReplayError::Stalled);
        }
        if Instant::now() >= deadline {
            return Err(ReplayError::Timeout);
        }
    }
}

/// One request on a fresh DEALER. `Ok(true)` once the end marker (an empty
/// payload) arrives, `Ok(false)` when the socket goes quiet first.
async fn replay_attempt(
    endpoint: SocketAddr,
    sequence: &mut Sequence,
    tx: &tokio::sync::mpsc::Sender<Feed>,
    deadline: Instant,
    batches: &mut u64,
) -> Result<bool, ReplayError> {
    use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

    let mut dealer = DealerSocket::new();
    tokio::time::timeout(REPLAY_IDLE, dealer.connect(&format!("tcp://{endpoint}")))
        .await
        .map_err(|_| ReplayError::Unreachable)??;
    let mut request = ZmqMessage::from(sequence.next().to_be_bytes().to_vec());
    request.prepend(&ZmqMessage::from(Vec::new()));
    dealer.send(request).await?;
    loop {
        let idle = REPLAY_IDLE.min(deadline.saturating_duration_since(Instant::now()));
        let Ok(message) = tokio::time::timeout(idle, dealer.recv()).await else {
            return Ok(false);
        };
        let mut frames = message?.into_vec();
        if frames.first().is_some_and(|f| f.as_ref().is_empty()) {
            frames.remove(0);
        }
        let (seq, payload) = sequenced(frames).ok_or(ReplayError::Malformed)?;
        if payload.as_ref().is_empty() {
            return Ok(true);
        }
        if let Some(lost) = sequence.advance(seq) {
            if lost > 0 {
                let _ = tx.send(Feed::Lost(lost)).await;
            }
            let _ = tx.send(Feed::Batch(payload.to_vec())).await;
            *batches += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::chains::Chains;
    use crate::chains::index::{Position, Rank};
    use crate::chains::subscriber::subscribe;
    use crate::chains::testing::{GOLDEN_LEGACY, GOLDEN_V031, h, stored_batch, unhex};
    use crate::config::ChainPolicy;
    use crate::shutdown::Shutdown;
    use crate::stats::Stats;

    /// Binds a real ZMQ PUB on localhost and publishes `[topic, seq, payload]`
    /// frames until `done` says to stop.
    fn publish_until(payloads: Vec<Vec<u8>>, done: impl Fn() -> bool + Send + 'static) -> String {
        use zeromq::{PubSocket, Socket, SocketSend, ZmqMessage};

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut publisher = PubSocket::new();
                let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
                tx.send(endpoint.to_string()).expect("endpoint");
                let mut seq = 0u64;
                while !done() {
                    for payload in &payloads {
                        seq += 1;
                        let frames = vec![
                            bytes::Bytes::new(),
                            bytes::Bytes::copy_from_slice(&seq.to_be_bytes()),
                            bytes::Bytes::copy_from_slice(payload),
                        ];
                        let message = ZmqMessage::try_from(frames).expect("frames");
                        publisher.send(message).await.expect("send");
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            });
        });
        rx.recv().expect("bound")
    }

    #[test]
    fn subscriber_applies_batches_from_a_real_pub_socket() {
        let chains = Arc::new(Chains::new(100, ChainPolicy::TailFirst, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let endpoint = {
            let chains = Arc::clone(&chains);
            publish_until(vec![unhex(GOLDEN_V031), b"\xc1".to_vec()], move || {
                Stats::get(&chains.stats.batches) > 0 && Stats::get(&chains.stats.decode_errors) > 0
            })
        };
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            std::thread::spawn(move || subscribe(&[endpoint], None, &chains, &shutdown))
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while (Stats::get(&chains.stats.batches) == 0
            || Stats::get(&chains.stats.decode_errors) == 0)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        assert!(Stats::get(&chains.stats.batches) >= 1);
        assert!(Stats::get(&chains.stats.decode_errors) >= 1);
        assert_eq!(
            chains.rank(h(0x12), 0),
            Rank::Childless,
            "0x13 removed from STORAGE"
        );
        assert_eq!(chains.deleted(h(0x11)), Position::Root);
    }

    #[test]
    fn peer_plan_adds_new_addresses_and_drops_gone_ones() {
        use std::collections::HashSet;
        use std::net::SocketAddr;

        use crate::chains::subscriber::plan_peers;

        let a: SocketAddr = "10.0.0.1:5557".parse().expect("addr");
        let b: SocketAddr = "10.0.0.2:5557".parse().expect("addr");
        let c: SocketAddr = "10.0.0.3:5557".parse().expect("addr");
        let current: HashSet<_> = [a, b].into();
        let desired: HashSet<_> = [b, c].into();
        assert_eq!(plan_peers(&current, &desired), (vec![c], vec![a]));
        assert_eq!(plan_peers(&desired, &desired), (vec![], vec![]));
    }

    #[test]
    fn subscriber_resolves_a_hostname_to_its_addresses() {
        let chains = Arc::new(Chains::new(100, ChainPolicy::TailFirst, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let endpoint = {
            let chains = Arc::clone(&chains);
            publish_until(vec![unhex(GOLDEN_LEGACY)], move || {
                Stats::get(&chains.stats.batches) > 0
            })
        };
        let port = endpoint.rsplit(':').next().expect("port").to_string();
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            let named = format!("tcp://localhost:{port}");
            std::thread::spawn(move || subscribe(&[named], None, &chains, &shutdown))
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while Stats::get(&chains.stats.batches) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        assert!(Stats::get(&chains.stats.batches) >= 1);
    }

    #[test]
    fn subscriber_waits_for_an_endpoint_that_comes_up_late() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let chains = Arc::new(Chains::new(100, ChainPolicy::TailFirst, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            let endpoint = format!("tcp://127.0.0.1:{port}");
            std::thread::spawn(move || subscribe(&[endpoint], None, &chains, &shutdown))
        };
        std::thread::sleep(Duration::from_millis(1500));
        assert_eq!(Stats::get(&chains.stats.batches), 0);

        let late = {
            let chains = Arc::clone(&chains);
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                use zeromq::{PubSocket, Socket, SocketSend, ZmqMessage};
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async move {
                    let mut publisher = PubSocket::new();
                    publisher
                        .bind(&format!("tcp://127.0.0.1:{port}"))
                        .await
                        .expect("bind");
                    tx.send(()).expect("bound");
                    while Stats::get(&chains.stats.batches) == 0 {
                        let frames = vec![
                            bytes::Bytes::new(),
                            bytes::Bytes::copy_from_slice(&1u64.to_be_bytes()),
                            bytes::Bytes::from(unhex(GOLDEN_LEGACY)),
                        ];
                        publisher
                            .send(ZmqMessage::try_from(frames).expect("frames"))
                            .await
                            .expect("send");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                });
            });
            rx
        };
        late.recv().expect("bound");
        let deadline = Instant::now() + Duration::from_secs(30);
        while Stats::get(&chains.stats.batches) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        assert!(
            Stats::get(&chains.stats.batches) >= 1,
            "never received after the PUB came up"
        );
    }

    #[test]
    fn sequence_applies_in_order_and_skips_duplicates() {
        use crate::chains::subscriber::{Live, Sequence};

        let mut seq = Sequence::default();
        assert_eq!(seq.live(0), Live::Apply);
        assert_eq!(seq.advance(0), Some(0));
        assert_eq!(seq.live(1), Live::Apply);
        assert_eq!(seq.advance(1), Some(0));
        assert_eq!(seq.live(1), Live::Skip, "duplicate live message");
        assert_eq!(seq.advance(1), None, "already applied");
        assert_eq!(seq.next(), 2);
    }

    #[test]
    fn sequence_replays_gaps_and_mid_stream_joins() {
        use crate::chains::subscriber::{Live, Sequence};

        let mut join = Sequence::default();
        assert_eq!(join.live(41), Live::Replay, "joined mid-stream");
        assert_eq!(join.next(), 0);
        assert_eq!(join.advance(41), Some(41), "nothing replayed: 0..41 lost");

        let mut gap = Sequence::default();
        gap.advance(4);
        assert_eq!(gap.live(7), Live::Replay);
        assert_eq!(gap.next(), 5);
        assert_eq!(gap.advance(5), Some(0), "replayed");
        assert_eq!(gap.advance(7), Some(1), "6 fell out of the replay buffer");
    }

    #[test]
    fn sequence_starts_over_when_the_publisher_restarts() {
        use crate::chains::subscriber::{Live, Sequence};

        let mut seq = Sequence::default();
        for n in 0..10 {
            seq.live(n);
            seq.advance(n);
        }
        assert_eq!(seq.live(0), Live::Restart);
        assert_eq!(seq.next(), 0);
        assert_eq!(seq.advance(0), Some(0));
        assert_eq!(seq.live(1), Live::Apply);
    }

    #[test]
    fn sequenced_frames_need_three_parts_and_an_eight_byte_seq() {
        use crate::chains::subscriber::sequenced;

        let frames = |seq: &[u8]| vec![b"topic".to_vec(), seq.to_vec(), b"payload".to_vec()];
        assert_eq!(
            sequenced(frames(&7u64.to_be_bytes())),
            Some((7, b"payload".to_vec()))
        );
        assert_eq!(sequenced(frames(&[7])), None);
        assert_eq!(sequenced(vec![b"payload".to_vec()]), None);
    }

    type Sequenced = (u64, Vec<u8>);

    /// A vLLM-like publisher: a PUB socket plus a ROUTER replay socket that
    /// serves every batch ever published, the way vLLM's `replay_endpoint`
    /// does. Seqs 0 and 1 are published before anyone can subscribe, and seq
    /// `hidden` only goes to the replay buffer. Returns the PUB endpoint and
    /// the replay port.
    fn publish_with_replay(hidden: u64, done: impl Fn() -> bool + Send + 'static) -> (String, u16) {
        use std::sync::Mutex;

        use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let buffer: Arc<Mutex<Vec<Sequenced>>> = Arc::default();
                let mut publisher = PubSocket::new();
                let endpoint = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");
                let mut router = RouterSocket::new();
                let replay = router.bind("tcp://127.0.0.1:0").await.expect("bind");
                let zeromq::Endpoint::Tcp(_, replay_port) = replay else {
                    panic!("tcp endpoint");
                };
                let frame = |b: &[u8]| bytes::Bytes::copy_from_slice(b);
                let serve = {
                    let buffer = Arc::clone(&buffer);
                    tokio::spawn(async move {
                        while let Ok(request) = router.recv().await {
                            let frames = request.into_vec();
                            let [id, _, start] =
                                <[bytes::Bytes; 3]>::try_from(frames).expect("request");
                            let start = u64::from_be_bytes(start.as_ref().try_into().expect("seq"));
                            let replies: Vec<Sequenced> = buffer
                                .lock()
                                .expect("buffer")
                                .iter()
                                .filter(|(seq, _)| *seq >= start)
                                .cloned()
                                .collect();
                            for (seq, payload) in replies {
                                let reply = vec![
                                    id.clone(),
                                    frame(b""),
                                    frame(b"kv"),
                                    frame(&seq.to_be_bytes()),
                                    frame(&payload),
                                ];
                                router
                                    .send(ZmqMessage::try_from(reply).expect("frames"))
                                    .await
                                    .expect("reply");
                            }
                            let end = vec![
                                id,
                                frame(b""),
                                frame(b""),
                                frame(&(-1i64).to_be_bytes()),
                                frame(b""),
                            ];
                            router
                                .send(ZmqMessage::try_from(end).expect("frames"))
                                .await
                                .expect("end");
                        }
                    })
                };
                let mut publish = async |seq: u64, batch: Vec<u8>, live: bool| {
                    buffer.lock().expect("buffer").push((seq, batch.clone()));
                    if live {
                        let frames = vec![
                            frame(b"kv"),
                            frame(&seq.to_be_bytes()),
                            bytes::Bytes::from(batch),
                        ];
                        publisher
                            .send(ZmqMessage::try_from(frames).expect("frames"))
                            .await
                            .expect("send");
                    }
                };
                publish(0, stored_batch(None, &[0x100]), true).await;
                publish(1, stored_batch(Some(0x100), &[0x101]), true).await;
                tx.send((endpoint.to_string(), replay_port))
                    .expect("endpoint");
                let mut seq = 2u64;
                while !done() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if seq == hidden {
                        publish(seq, stored_batch(Some(0x101), &[0x300]), false).await;
                    } else {
                        publish(seq, stored_batch(Some(0x101), &[0x200 + seq]), true).await;
                    }
                    seq += 1;
                }
                serve.abort();
            });
        });
        rx.recv().expect("bound")
    }

    #[test]
    fn subscriber_replays_what_the_sub_socket_missed() {
        let chains = Arc::new(Chains::new(1000, ChainPolicy::Radix, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let recovered = {
            let chains = Arc::clone(&chains);
            move || {
                let index = chains.index();
                index.on_disk(h(0x100)) && index.on_disk(h(0x101)) && index.on_disk(h(0x300))
            }
        };
        let (endpoint, replay_port) = {
            let recovered = recovered.clone();
            publish_with_replay(40, recovered)
        };
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            std::thread::spawn(move || {
                subscribe(&[endpoint], Some(replay_port), &chains, &shutdown)
            })
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while !recovered() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        let s = &chains.stats;
        assert!(
            recovered(),
            "seqs 0, 1 (before subscribing) and 40 (never live) replayed"
        );
        assert!(Stats::get(&s.replays) >= 2, "on connect and on the gap");
        assert!(Stats::get(&s.gaps) >= 1);
        assert_eq!(Stats::get(&s.events_lost), 0);
        assert_eq!(Stats::get(&s.replay_failures), 0);
        assert_eq!(Stats::get(&s.decode_errors), 0);
    }

    #[test]
    fn subscriber_counts_losses_when_replay_is_down() {
        let chains = Arc::new(Chains::new(1000, ChainPolicy::Radix, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let dead_port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let (endpoint, _) = {
            let chains = Arc::clone(&chains);
            publish_with_replay(u64::MAX, move || Stats::get(&chains.stats.batches) >= 5)
        };
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            std::thread::spawn(move || subscribe(&[endpoint], Some(dead_port), &chains, &shutdown))
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while Stats::get(&chains.stats.batches) < 5 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        let s = &chains.stats;
        assert!(Stats::get(&s.batches) >= 5, "live batches still apply");
        assert_eq!(Stats::get(&s.replay_failures), 1, "then cooldown");
        assert!(Stats::get(&s.events_lost) >= 2, "seqs 0 and 1");
        assert!(!chains.index().on_disk(h(0x100)));
    }
}
