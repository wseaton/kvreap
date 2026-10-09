//! `BlockRemoved` events, byte-compatible with
//! `llmd_fs_backend.event_publisher.StorageEventPublisher`.
//!
//! Frames: `[b"kv@SHARED_STORAGE@<model>", seq as u64 BE, payload]`, where
//! payload = msgpack `[unix_ts_f64, [bin(event), ...]]` and
//! event   = msgpack `["BlockRemoved", [hash_u64, ...], "SHARED_STORAGE"]`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::layout::BlockHash;

const MEDIUM: &str = "SHARED_STORAGE";
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const SNDHWM: i32 = 100_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    pub model_base: PathBuf,
    pub hash: BlockHash,
}

pub fn encode_block_removed(hashes: &[BlockHash]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + hashes.len() * 9);
    let _ = rmp::encode::write_array_len(&mut buf, 3);
    let _ = rmp::encode::write_str(&mut buf, "BlockRemoved");
    let _ = rmp::encode::write_array_len(&mut buf, u32::try_from(hashes.len()).unwrap_or(u32::MAX));
    for h in hashes {
        let _ = rmp::encode::write_uint(&mut buf, h.0);
    }
    let _ = rmp::encode::write_str(&mut buf, MEDIUM);
    buf
}

pub fn encode_payload(timestamp: f64, events: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = rmp::encode::write_array_len(&mut buf, 2);
    let _ = rmp::encode::write_f64(&mut buf, timestamp);
    let _ = rmp::encode::write_array_len(&mut buf, u32::try_from(events.len()).unwrap_or(u32::MAX));
    for e in events {
        let _ = rmp::encode::write_bin(&mut buf, e);
    }
    buf
}

pub fn topic(model_name: &str) -> String {
    format!("kv@{MEDIUM}@{model_name}")
}

fn read_model_name(model_base: &Path) -> Option<String> {
    let raw = std::fs::read(model_base.join("config.json")).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    value.get("model_name")?.as_str().map(str::to_owned)
}

/// Groups removals per model and flushes a model's batch when it reaches
/// `batch_size` or every `FLUSH_INTERVAL`.
#[derive(Debug)]
pub struct Batcher {
    batch_size: usize,
    names: HashMap<PathBuf, Option<String>>,
    pending: HashMap<String, Vec<BlockHash>>,
}

impl Batcher {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            names: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    fn model_name(&mut self, base: &Path) -> Option<String> {
        self.names
            .entry(base.to_path_buf())
            .or_insert_with(|| read_model_name(base))
            .clone()
    }

    /// Adds a removal; returns a full batch to publish, if any.
    pub fn push(&mut self, removed: Removed) -> Option<(String, Vec<BlockHash>)> {
        let model = self.model_name(&removed.model_base)?;
        let batch = self.pending.entry(model.clone()).or_default();
        batch.push(removed.hash);
        if batch.len() >= self.batch_size {
            let full = std::mem::take(batch);
            return Some((model, full));
        }
        None
    }

    pub fn drain(&mut self) -> Vec<(String, Vec<BlockHash>)> {
        self.pending
            .drain()
            .filter(|(_, hashes)| !hashes.is_empty())
            .collect()
    }
}

pub struct Publisher {
    _ctx: zmq::Context,
    socket: zmq::Socket,
    seq: u64,
}

impl Publisher {
    pub fn bind(endpoint: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB)?;
        socket.set_linger(0)?;
        socket.set_sndhwm(SNDHWM)?;
        socket.bind(endpoint)?;
        Ok(Self {
            _ctx: ctx,
            socket,
            seq: 0,
        })
    }

    pub fn publish(&mut self, model: &str, hashes: &[BlockHash]) -> Result<(), zmq::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let payload = encode_payload(now, &[encode_block_removed(hashes)]);
        self.seq += 1;
        let topic = topic(model);
        let seq = self.seq.to_be_bytes();
        let frames: [&[u8]; 3] = [topic.as_bytes(), &seq, &payload];
        self.socket.send_multipart(frames, 0)
    }
}

/// Runs until every sender is dropped, then flushes what is left.
pub fn run(mut publisher: Publisher, rx: Receiver<Removed>, batch_size: usize) {
    let mut batcher = Batcher::new(batch_size);
    let mut last_flush = Instant::now();
    let send = |publisher: &mut Publisher, model: &str, hashes: &[BlockHash]| {
        if let Err(e) = publisher.publish(model, hashes) {
            tracing::warn!(error = %e, model, "failed to publish BlockRemoved events");
        }
    };
    loop {
        match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(removed) => {
                if let Some((model, hashes)) = batcher.push(removed) {
                    send(&mut publisher, &model, &hashes);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if last_flush.elapsed() >= FLUSH_INTERVAL {
            for (model, hashes) in batcher.drain() {
                send(&mut publisher, &model, &hashes);
            }
            last_flush = Instant::now();
        }
    }
    for (model, hashes) in batcher.drain() {
        send(&mut publisher, &model, &hashes);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::events::{Batcher, Publisher, Removed, encode_block_removed, encode_payload, topic};
    use crate::layout::BlockHash;

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
            .collect()
    }

    const HASHES: [BlockHash; 3] = [BlockHash(0xABCDEF0123456789), BlockHash(1), BlockHash(300)];
    // Generated with python msgpack.packb(..., use_bin_type=True), as event_publisher.py does.
    const GOLDEN_EVENT: &str =
        "93ac426c6f636b52656d6f76656493cfabcdef012345678901cd012cae5348415245445f53544f52414745";
    const GOLDEN_PAYLOAD: &str = "92cb41da39de0020000091c42b93ac426c6f636b52656d6f76656493cfabcdef012345678901cd012cae5348415245445f53544f52414745";

    #[test]
    fn event_bytes_match_python_publisher() {
        assert_eq!(encode_block_removed(&HASHES), unhex(GOLDEN_EVENT));
    }

    #[test]
    fn payload_bytes_match_python_publisher() {
        let event = encode_block_removed(&HASHES);
        assert_eq!(
            encode_payload(1760000000.5, &[event]),
            unhex(GOLDEN_PAYLOAD)
        );
    }

    #[test]
    fn topic_format() {
        assert_eq!(
            topic("meta-llama/Llama-3.1-8B"),
            "kv@SHARED_STORAGE@meta-llama/Llama-3.1-8B"
        );
    }

    fn model_dir(root: &std::path::Path, base: &str, name: &str) -> std::path::PathBuf {
        let dir = root.join(base);
        fs::create_dir_all(&dir).expect("mkdir");
        fs::write(
            dir.join("config.json"),
            format!(r#"{{"model_name": "{name}"}}"#),
        )
        .expect("write");
        dir
    }

    #[test]
    fn batcher_groups_by_model_and_flushes_at_batch_size() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let a = model_dir(tmp.path(), "a_111111111111", "org/a");
        let b = model_dir(tmp.path(), "b_222222222222", "org/b");
        let mut batcher = Batcher::new(2);

        assert_eq!(
            batcher.push(Removed {
                model_base: a.clone(),
                hash: BlockHash(1)
            }),
            None
        );
        assert_eq!(
            batcher.push(Removed {
                model_base: b.clone(),
                hash: BlockHash(9)
            }),
            None
        );
        assert_eq!(
            batcher.push(Removed {
                model_base: a,
                hash: BlockHash(2)
            }),
            Some(("org/a".to_string(), vec![BlockHash(1), BlockHash(2)]))
        );
        assert_eq!(
            batcher.drain(),
            vec![("org/b".to_string(), vec![BlockHash(9)])]
        );
        assert!(batcher.drain().is_empty());
    }

    #[test]
    fn batcher_drops_removals_without_model_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut batcher = Batcher::new(1);
        let missing = tmp.path().join("nope_000000000000");
        assert_eq!(
            batcher.push(Removed {
                model_base: missing,
                hash: BlockHash(1)
            }),
            None
        );
        assert!(batcher.drain().is_empty());
    }

    #[test]
    fn batcher_caches_model_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let a = model_dir(tmp.path(), "a_111111111111", "org/a");
        let mut batcher = Batcher::new(1);
        assert!(
            batcher
                .push(Removed {
                    model_base: a.clone(),
                    hash: BlockHash(1)
                })
                .is_some()
        );
        fs::remove_file(a.join("config.json")).expect("rm");
        assert!(
            batcher
                .push(Removed {
                    model_base: a,
                    hash: BlockHash(2)
                })
                .is_some()
        );
    }

    #[test]
    fn publisher_frames_round_trip_over_zmq() {
        let ctx = zmq::Context::new();
        let mut publisher = Publisher::bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = publisher
            .socket
            .get_last_endpoint()
            .expect("endpoint")
            .expect("utf8");
        let sub = ctx.socket(zmq::SUB).expect("sub");
        sub.connect(&endpoint).expect("connect");
        sub.set_subscribe(b"kv@").expect("subscribe");
        sub.set_rcvtimeo(200).expect("rcvtimeo");

        // PUB drops messages until the subscription propagates, so retry.
        let frames = (0..50)
            .find_map(|_| {
                publisher.publish("org/m", &HASHES).expect("publish");
                sub.recv_multipart(0).ok()
            })
            .expect("received a message");

        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0], b"kv@SHARED_STORAGE@org/m");
        let seq = u64::from_be_bytes(frames[1].as_slice().try_into().expect("8 bytes"));
        assert!(seq >= 1);
        let event = unhex(GOLDEN_EVENT);
        assert!(frames[2].ends_with(&event));
        assert_eq!(frames[2][0], 0x92);
    }
}
