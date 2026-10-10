//! Prefix-chain index built from vLLM's KV cache events.
//!
//! A prefix-cache lookup stops at the first missing block, so deleting block k
//! of a chain makes every later block useless. vLLM publishes `BlockStored`
//! events with each block's parent; this module keeps `block -> parent` and a
//! per-block count of children still on disk, so the worker can delete dead and
//! childless blocks before the heads that the rest of a chain depends on.
//!
//! ```text
//!  root ──► b1 ──► b2 ──► b3        evict b3 (leaf) first: b2 becomes a leaf
//!                    └──► b2'       evict b1 first: b2, b3, b2' are dead weight
//! ```
//!
//! Wire format (vLLM `ZmqEventPublisher`): frames `[topic, seq u64 BE, payload]`,
//! payload = msgpack `EventBatch` `[ts, [event, ...], dp_rank?]`. Events are
//! msgspec tagged structs: maps keyed by field name with `"type"` as the tag
//! (vLLM >= 0.12), or arrays with the tag first (older releases).

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::config::ChainPolicy;
use crate::layout::BlockHash;
use crate::shutdown::Shutdown;
use crate::stats::Stats;

pub const INDEX_CAP: usize = 4 << 20;
const SUBTREE_BUDGET: usize = 4096;
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
const MAX_DEPTH: usize = 16;
const MEDIUM_STORAGE: &str = "STORAGE";

/// Where a block sat in its chain when it was deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// No events seen for this block.
    Untracked,
    /// First block of a prompt (no parent).
    Root,
    /// Parent known but no longer on disk: unreachable by a prefix lookup.
    Orphan,
    /// Parent on disk and at least one child on disk.
    Internal,
    /// Parent on disk and no children on disk.
    Leaf,
}

/// Eviction preference within the oldest pool candidates, best first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rank {
    Dead,
    Childless,
    Overdue,
    Interior,
}

#[derive(Debug, Clone)]
struct Node {
    parent: Option<BlockHash>,
    /// Every child ever linked; filter by `on_disk` when walking.
    children: Vec<BlockHash>,
    children_on_disk: u32,
    on_disk: bool,
    /// Seen on disk and then removed. A block whose parent was never seen on
    /// disk (vLLM wrote it before kvreap started) is not dead; one whose
    /// parent is gone is.
    gone: bool,
    generation: u64,
    /// Full block hash, which names the block's file.
    digest: Option<Box<[u8]>>,
    /// When the block was last marked on disk.
    stored_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubtreeKey {
    live: bool,
    per_leaf: Reverse<usize>,
}

/// A radix-tree leaf edge: the on-disk blocks from just below the last fork
/// (or a chain root, or a missing parent) down to a leaf, root side first.
/// Only a leaf edge serves a single cached prefix, so it is the unit of
/// connectivity-based eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEdge {
    pub blocks: Vec<Descendant>,
    /// Most recent on-disk mark of any block in the edge.
    pub newest_store: Option<Instant>,
}

/// A block below a subtree root, with the file name of its digest if known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descendant {
    pub hash: BlockHash,
    pub file_name: Option<String>,
}

/// Bounded `block -> parent` map with on-disk child counts. When over `cap`,
/// the oldest-inserted blocks are forgotten first.
#[derive(Debug)]
pub struct ChainIndex {
    cap: usize,
    nodes: HashMap<BlockHash, Node>,
    order: VecDeque<(BlockHash, u64)>,
    next_generation: u64,
}

impl ChainIndex {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            nodes: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    fn node_mut(&mut self, hash: BlockHash) -> &mut Node {
        let generation = self.next_generation;
        let created = !self.nodes.contains_key(&hash);
        if created {
            self.next_generation += 1;
            self.order.push_back((hash, generation));
        }
        self.nodes.entry(hash).or_insert(Node {
            parent: None,
            children: Vec::new(),
            children_on_disk: 0,
            on_disk: false,
            gone: false,
            generation,
            digest: None,
            stored_at: None,
        })
    }

    fn adjust_children(&mut self, parent: Option<BlockHash>, up: bool) {
        let Some(p) = parent else {
            return;
        };
        if up {
            let n = self.node_mut(p);
            n.children_on_disk = n.children_on_disk.saturating_add(1);
        } else if let Some(n) = self.nodes.get_mut(&p) {
            n.children_on_disk = n.children_on_disk.saturating_sub(1);
            if !n.on_disk && n.children_on_disk == 0 && n.parent.is_none() {
                self.nodes.remove(&p);
            }
        }
    }

    /// Records `hashes` as a chain hanging off `parent`; `on_disk` also marks
    /// them written. Without it only the parent links are learned.
    pub fn store(&mut self, parent: Option<BlockHash>, hashes: &[BlockHash], on_disk: bool) {
        let mut parent = parent;
        for &hash in hashes {
            self.store_one(hash, parent, on_disk);
            parent = Some(hash);
        }
        self.enforce_cap();
    }

    fn store_one(&mut self, hash: BlockHash, parent: Option<BlockHash>, on_disk: bool) {
        let n = self.node_mut(hash);
        let (old_parent, was_on_disk) = (n.parent, n.on_disk);
        let parent = old_parent.or(parent);
        n.parent = parent;
        if on_disk && !n.on_disk {
            n.on_disk = true;
            n.gone = false;
            n.stored_at = Some(Instant::now());
        }
        let now_on_disk = n.on_disk;
        let gained_parent = old_parent.is_none() && parent.is_some();
        if let (true, Some(p)) = (gained_parent, parent) {
            let siblings = &mut self.node_mut(p).children;
            if !siblings.contains(&hash) {
                siblings.push(hash);
            }
        }
        let counted = was_on_disk && !gained_parent;
        if now_on_disk && !counted {
            self.adjust_children(parent, true);
        }
    }

    /// Records the full hash that names `hash`'s file.
    pub fn set_digest(&mut self, hash: BlockHash, digest: &[u8]) {
        if let Some(n) = self.nodes.get_mut(&hash)
            && n.digest.is_none()
        {
            n.digest = Some(digest.into());
        }
    }

    fn on_disk_children(&self, hash: BlockHash) -> impl Iterator<Item = BlockHash> + '_ {
        self.nodes
            .get(&hash)
            .map(|n| n.children.as_slice())
            .unwrap_or_default()
            .iter()
            .copied()
            .filter(move |c| {
                self.on_disk(*c) && self.nodes.get(c).is_some_and(|n| n.parent == Some(hash))
            })
    }

    /// `(blocks, leaves)` of the on-disk subtree rooted at `hash`, walking at
    /// most `budget` blocks. A childless block is one block and one leaf.
    pub fn shape(&self, hash: BlockHash, budget: usize) -> (usize, usize) {
        let mut stack = vec![hash];
        let (mut blocks, mut leaves) = (0, 0);
        while let Some(h) = stack.pop() {
            if blocks >= budget {
                break;
            }
            blocks += 1;
            let before = stack.len();
            stack.extend(self.on_disk_children(h));
            if stack.len() == before {
                leaves += 1;
            }
        }
        (blocks, leaves.max(1))
    }

    fn descendant(&self, hash: BlockHash) -> Descendant {
        let file_name = self
            .nodes
            .get(&hash)
            .and_then(|n| n.digest.as_deref())
            .map(|d| d.iter().map(|b| format!("{b:02x}")).collect::<String>() + ".bin");
        Descendant { hash, file_name }
    }

    /// The leaf edge `hash` maps to: its own edge when nothing forks below
    /// it, else the edge of the oldest-stored child at each fork on the way
    /// down. `None` when `hash` is not on disk or the walk exceeds `budget`.
    ///
    /// ```text
    ///   s0 ── s1 ──┬── a1 ── a2      hash = a1  → edge [a1, a2]
    ///              └── b1            hash = s0  → edge [b1] (if b1 is older)
    /// ```
    pub fn leaf_edge(&self, hash: BlockHash, budget: usize) -> Option<LeafEdge> {
        if !self.on_disk(hash) {
            return None;
        }
        let mut leaf = hash;
        let mut steps = 0;
        loop {
            steps += 1;
            if steps > budget {
                return None;
            }
            let oldest_child = self
                .on_disk_children(leaf)
                .min_by_key(|c| self.nodes.get(c).and_then(|n| n.stored_at));
            match oldest_child {
                Some(c) => leaf = c,
                None => break,
            }
        }
        let mut blocks = vec![self.descendant(leaf)];
        let mut top = leaf;
        while let Some(p) = self.nodes.get(&top).and_then(|n| n.parent) {
            let single = self.on_disk(p) && self.on_disk_children(p).take(2).count() == 1;
            if !single || blocks.len() >= budget {
                break;
            }
            blocks.push(self.descendant(p));
            top = p;
        }
        blocks.reverse();
        let newest_store = blocks
            .iter()
            .filter_map(|b| self.nodes.get(&b.hash).and_then(|n| n.stored_at))
            .max();
        Some(LeafEdge {
            blocks,
            newest_store,
        })
    }

    /// On-disk blocks strictly below `hash`, at most `budget` of them.
    pub fn descendants(&self, hash: BlockHash, budget: usize) -> Vec<Descendant> {
        let mut out = Vec::new();
        let mut stack: Vec<BlockHash> = self.on_disk_children(hash).collect();
        while let Some(h) = stack.pop() {
            if out.len() >= budget {
                break;
            }
            stack.extend(self.on_disk_children(h));
            out.push(self.descendant(h));
        }
        out
    }

    /// Marks `hash` gone from disk and returns where it sat in its chain.
    pub fn remove(&mut self, hash: BlockHash) -> Position {
        let position = self.position(hash);
        let Some(node) = self.nodes.get_mut(&hash) else {
            return position;
        };
        if !node.on_disk {
            return position;
        }
        node.on_disk = false;
        node.gone = true;
        let (parent, children) = (node.parent, node.children_on_disk);
        if children == 0 {
            self.nodes.remove(&hash);
        }
        self.adjust_children(parent, false);
        position
    }

    fn on_disk(&self, hash: BlockHash) -> bool {
        self.nodes.get(&hash).is_some_and(|n| n.on_disk)
    }

    fn gone(&self, hash: BlockHash) -> bool {
        self.nodes.get(&hash).is_some_and(|n| n.gone && !n.on_disk)
    }

    pub fn position(&self, hash: BlockHash) -> Position {
        let Some(node) = self.nodes.get(&hash).filter(|n| n.on_disk) else {
            return Position::Untracked;
        };
        match node.parent {
            Some(p) if self.gone(p) => Position::Orphan,
            Some(p) if self.on_disk(p) && node.children_on_disk > 0 => Position::Internal,
            Some(p) if self.on_disk(p) => Position::Leaf,
            _ => Position::Root,
        }
    }

    pub fn rank(&self, hash: BlockHash, deferrals: u32, max_deferrals: u32) -> Rank {
        let Some(node) = self.nodes.get(&hash).filter(|n| n.on_disk) else {
            return Rank::Childless;
        };
        if node.parent.is_some_and(|p| self.gone(p)) {
            Rank::Dead
        } else if node.children_on_disk == 0 {
            Rank::Childless
        } else if deferrals >= max_deferrals {
            Rank::Overdue
        } else {
            Rank::Interior
        }
    }

    fn enforce_cap(&mut self) {
        while self.nodes.len() > self.cap {
            let Some((hash, generation)) = self.order.pop_front() else {
                break;
            };
            let current = self
                .nodes
                .get(&hash)
                .is_some_and(|n| n.generation == generation);
            if let Some(node) = current.then(|| self.nodes.remove(&hash)).flatten()
                && node.on_disk
            {
                self.adjust_children(node.parent, false);
            }
        }
        if self.order.len() > self.cap.saturating_mul(2) {
            let nodes = &self.nodes;
            self.order
                .retain(|(h, g)| nodes.get(h).is_some_and(|n| n.generation == *g));
        }
    }
}

#[derive(Debug, Default)]
pub struct ChainStats {
    pub batches: AtomicU64,
    pub decode_errors: AtomicU64,
    pub blocks_stored: AtomicU64,
    pub deleted_untracked: AtomicU64,
    pub deleted_root: AtomicU64,
    pub deleted_orphan: AtomicU64,
    pub deleted_internal: AtomicU64,
    pub deleted_leaf: AtomicU64,
    pub deferrals: AtomicU64,
    pub cascaded: AtomicU64,
    /// Stored blocks announced as int hashes, whose files cannot be named.
    pub undigested: AtomicU64,
    /// Leaf edges left alone because a block in them is younger than the hot threshold.
    pub young_edges: AtomicU64,
    /// Young edges deleted because nothing older was left to free.
    pub young_fallbacks: AtomicU64,
    /// Live batches whose sequence number skipped ahead of the last applied.
    pub gaps: AtomicU64,
    /// Publishers whose sequence numbers went backwards (restarted).
    pub resets: AtomicU64,
    pub replays: AtomicU64,
    pub replayed_batches: AtomicU64,
    pub replay_failures: AtomicU64,
    /// Batches never applied: skipped by the SUB socket and gone from the replay buffer.
    pub events_lost: AtomicU64,
}

/// The index shared by the subscriber and every worker.
#[derive(Debug)]
pub struct Chains {
    index: Mutex<ChainIndex>,
    pub policy: ChainPolicy,
    pub max_deferrals: u32,
    /// When set, only `BlockStored` events of this medium mark blocks on
    /// disk; the rest only teach parent links.
    pub disk_medium: Option<String>,
    pub stats: ChainStats,
}

impl Chains {
    pub fn new(
        cap: usize,
        policy: ChainPolicy,
        max_deferrals: u32,
        disk_medium: Option<String>,
    ) -> Self {
        Self {
            index: Mutex::new(ChainIndex::new(cap)),
            policy,
            max_deferrals,
            disk_medium,
            stats: ChainStats::default(),
        }
    }

    fn index(&self) -> MutexGuard<'_, ChainIndex> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn rank(&self, hash: BlockHash, deferrals: u32) -> Rank {
        self.index().rank(hash, deferrals, self.max_deferrals)
    }

    /// Subtree eviction order, smallest first: dead blocks, then the most
    /// blocks freed per continuation lost (subtree blocks / leaves), so the
    /// heads of unshared chains go before prefixes many requests extend.
    pub fn subtree_key(&self, hash: BlockHash) -> SubtreeKey {
        let index = self.index();
        if index.rank(hash, 0, u32::MAX) == Rank::Dead {
            return SubtreeKey {
                live: false,
                per_leaf: Reverse(0),
            };
        }
        let (blocks, leaves) = index.shape(hash, SUBTREE_BUDGET);
        SubtreeKey {
            live: true,
            per_leaf: Reverse(blocks.saturating_mul(1000) / leaves),
        }
    }

    pub fn descendants(&self, hash: BlockHash) -> Vec<Descendant> {
        self.index().descendants(hash, SUBTREE_BUDGET)
    }

    pub fn leaf_edge(&self, hash: BlockHash) -> Option<LeafEdge> {
        self.index().leaf_edge(hash, SUBTREE_BUDGET)
    }

    pub fn on_disk(&self, hash: BlockHash) -> bool {
        self.index().on_disk(hash)
    }

    /// Records a deletion by kvreap and counts its chain position.
    pub fn deleted(&self, hash: BlockHash) -> Position {
        let position = self.index().remove(hash);
        let counter = match position {
            Position::Untracked => &self.stats.deleted_untracked,
            Position::Root => &self.stats.deleted_root,
            Position::Orphan => &self.stats.deleted_orphan,
            Position::Internal => &self.stats.deleted_internal,
            Position::Leaf => &self.stats.deleted_leaf,
        };
        Stats::add(counter, 1);
        position
    }

    pub fn apply(&self, events: &[KvEvent]) {
        let mut index = self.index();
        for event in events {
            match event {
                KvEvent::Stored {
                    parent,
                    hashes,
                    digests,
                    medium,
                } => {
                    let on_disk = self
                        .disk_medium
                        .as_deref()
                        .is_none_or(|m| medium.as_deref() == Some(m));
                    index.store(*parent, hashes, on_disk);
                    if digests.is_empty() {
                        Stats::add(&self.stats.undigested, hashes.len() as u64);
                    }
                    for (h, d) in hashes.iter().zip(digests) {
                        index.set_digest(*h, d);
                    }
                    if on_disk {
                        Stats::add(&self.stats.blocks_stored, hashes.len() as u64);
                    }
                }
                KvEvent::Removed { hashes, medium }
                    if medium.as_deref()
                        == Some(self.disk_medium.as_deref().unwrap_or(MEDIUM_STORAGE)) =>
                {
                    for &h in hashes {
                        index.remove(h);
                    }
                }
                KvEvent::Removed { .. } | KvEvent::Other => {}
            }
        }
    }

    pub fn log_status(&self) {
        let s = &self.stats;
        let (root, orphan) = (Stats::get(&s.deleted_root), Stats::get(&s.deleted_orphan));
        tracing::info!(
            policy = ?self.policy,
            disk_medium = self.disk_medium.as_deref().unwrap_or("any"),
            index_blocks = self.index().len(),
            event_batches = Stats::get(&s.batches),
            decode_errors = Stats::get(&s.decode_errors),
            blocks_stored = Stats::get(&s.blocks_stored),
            deleted_heads = root + orphan,
            deleted_root = root,
            deleted_orphan = orphan,
            deleted_internal = Stats::get(&s.deleted_internal),
            deleted_leaf = Stats::get(&s.deleted_leaf),
            deleted_untracked = Stats::get(&s.deleted_untracked),
            deferrals = Stats::get(&s.deferrals),
            cascaded = Stats::get(&s.cascaded),
            undigested = Stats::get(&s.undigested),
            young_edges = Stats::get(&s.young_edges),
            young_fallbacks = Stats::get(&s.young_fallbacks),
            gaps = Stats::get(&s.gaps),
            resets = Stats::get(&s.resets),
            replays = Stats::get(&s.replays),
            replayed_batches = Stats::get(&s.replayed_batches),
            replay_failures = Stats::get(&s.replay_failures),
            events_lost = Stats::get(&s.events_lost),
            "chains"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvEvent {
    Stored {
        parent: Option<BlockHash>,
        hashes: Vec<BlockHash>,
        /// Full hashes, parallel to `hashes`; empty when vLLM sends int hashes.
        digests: Vec<Vec<u8>>,
        medium: Option<String>,
    },
    Removed {
        hashes: Vec<BlockHash>,
        medium: Option<String>,
    },
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("truncated msgpack")]
    Truncated,
    #[error("unsupported msgpack marker 0x{0:02x}")]
    Marker(u8),
    #[error("msgpack nested too deep")]
    TooDeep,
    #[error("payload is not an EventBatch")]
    Shape,
}

/// The subset of msgpack values the event decoder needs.
#[derive(Debug, Clone, PartialEq)]
enum Value<'a> {
    UInt(u64),
    Int(i64),
    Str(&'a [u8]),
    Bin(&'a [u8]),
    Array(Vec<Value<'a>>),
    Map(Vec<(Value<'a>, Value<'a>)>),
    /// nil, booleans, floats and extension types: skipped, never inspected.
    Other,
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        let out = self.buf.get(self.pos..end).ok_or(DecodeError::Truncated)?;
        self.pos = end;
        Ok(out)
    }

    fn be<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.take(N)?.try_into().map_err(|_| DecodeError::Truncated)
    }

    fn len(&mut self, bytes: usize) -> Result<usize, DecodeError> {
        let n = match bytes {
            1 => u64::from(u8::from_be_bytes(self.be()?)),
            2 => u64::from(u16::from_be_bytes(self.be()?)),
            _ => u64::from(u32::from_be_bytes(self.be()?)),
        };
        let n = usize::try_from(n).map_err(|_| DecodeError::Truncated)?;
        if n > self.buf.len() {
            return Err(DecodeError::Truncated);
        }
        Ok(n)
    }

    fn array(&mut self, n: usize, depth: usize) -> Result<Value<'a>, DecodeError> {
        (0..n)
            .map(|_| self.value(depth + 1))
            .collect::<Result<_, _>>()
            .map(Value::Array)
    }

    fn map(&mut self, n: usize, depth: usize) -> Result<Value<'a>, DecodeError> {
        (0..n)
            .map(|_| Ok((self.value(depth + 1)?, self.value(depth + 1)?)))
            .collect::<Result<_, _>>()
            .map(Value::Map)
    }

    fn ext(&mut self, n: usize) -> Result<Value<'a>, DecodeError> {
        self.take(n + 1).map(|_| Value::Other)
    }

    fn value(&mut self, depth: usize) -> Result<Value<'a>, DecodeError> {
        if depth > MAX_DEPTH {
            return Err(DecodeError::TooDeep);
        }
        let [m] = self.be::<1>()?;
        Ok(match m {
            0x00..=0x7f => Value::UInt(u64::from(m)),
            0x80..=0x8f => self.map(usize::from(m & 0x0f), depth)?,
            0x90..=0x9f => self.array(usize::from(m & 0x0f), depth)?,
            0xa0..=0xbf => Value::Str(self.take(usize::from(m & 0x1f))?),
            0xc0 | 0xc2 | 0xc3 => Value::Other,
            0xc4..=0xc6 => {
                let n = self.len(1 << (m - 0xc4))?;
                Value::Bin(self.take(n)?)
            }
            0xc7..=0xc9 => {
                let n = self.len(1 << (m - 0xc7))?;
                self.ext(n)?
            }
            0xca => self.take(4).map(|_| Value::Other)?,
            0xcb => self.take(8).map(|_| Value::Other)?,
            0xcc => Value::UInt(u64::from(u8::from_be_bytes(self.be()?))),
            0xcd => Value::UInt(u64::from(u16::from_be_bytes(self.be()?))),
            0xce => Value::UInt(u64::from(u32::from_be_bytes(self.be()?))),
            0xcf => Value::UInt(u64::from_be_bytes(self.be()?)),
            0xd0 => Value::Int(i64::from(i8::from_be_bytes(self.be()?))),
            0xd1 => Value::Int(i64::from(i16::from_be_bytes(self.be()?))),
            0xd2 => Value::Int(i64::from(i32::from_be_bytes(self.be()?))),
            0xd3 => Value::Int(i64::from_be_bytes(self.be()?)),
            0xd4..=0xd8 => self.ext(1 << (m - 0xd4))?,
            0xd9..=0xdb => {
                let n = self.len(1 << (m - 0xd9))?;
                Value::Str(self.take(n)?)
            }
            0xdc | 0xdd => {
                let n = self.len(2 << (m - 0xdc))?;
                self.array(n, depth)?
            }
            0xde | 0xdf => {
                let n = self.len(2 << (m - 0xde))?;
                self.map(n, depth)?
            }
            0xe0..=0xff => Value::Int(i64::from(i8::from_be_bytes([m]))),
            other => return Err(DecodeError::Marker(other)),
        })
    }
}

fn parse(buf: &[u8]) -> Result<Value<'_>, DecodeError> {
    Reader { buf, pos: 0 }.value(0)
}

/// Low 64 bits of a block hash, matching `BlockHash::from_file_name`.
fn block_hash(v: &Value<'_>) -> Option<BlockHash> {
    match v {
        Value::UInt(n) => Some(BlockHash(*n)),
        Value::Int(n) => Some(BlockHash(u64::from_ne_bytes(n.to_ne_bytes()))),
        Value::Bin(b) => {
            let tail = &b[b.len().saturating_sub(8)..];
            Some(BlockHash(
                tail.iter().fold(0u64, |acc, &x| (acc << 8) | u64::from(x)),
            ))
        }
        _ => None,
    }
}

fn digests(v: Option<&Value<'_>>) -> Vec<Vec<u8>> {
    match v {
        Some(Value::Array(items)) => items
            .iter()
            .map(|i| match i {
                Value::Bin(b) => Some(b.to_vec()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn hashes(v: Option<&Value<'_>>) -> Vec<BlockHash> {
    match v {
        Some(Value::Array(items)) => items.iter().filter_map(block_hash).collect(),
        _ => Vec::new(),
    }
}

fn string(v: Option<&Value<'_>>) -> Option<String> {
    match v {
        Some(Value::Str(s)) => std::str::from_utf8(s).ok().map(str::to_owned),
        _ => None,
    }
}

/// A tagged struct's fields: by name when map-encoded, by position when array-encoded.
enum Fields<'v, 'a> {
    Map(&'v [(Value<'a>, Value<'a>)]),
    Array(&'v [Value<'a>]),
}

impl<'v, 'a> Fields<'v, 'a> {
    fn get(&self, name: &str, index: usize) -> Option<&'v Value<'a>> {
        match self {
            Self::Map(entries) => entries
                .iter()
                .find(|(k, _)| matches!(k, Value::Str(s) if *s == name.as_bytes()))
                .map(|(_, v)| v),
            Self::Array(items) => items.get(index),
        }
    }
}

fn event(v: &Value<'_>) -> KvEvent {
    let fields = match v {
        Value::Map(entries) => Fields::Map(entries),
        Value::Array(items) => Fields::Array(items),
        _ => return KvEvent::Other,
    };
    match string(fields.get("type", 0)).as_deref() {
        Some("BlockStored") => KvEvent::Stored {
            hashes: hashes(fields.get("block_hashes", 1)),
            digests: digests(fields.get("block_hashes", 1)),
            parent: fields.get("parent_block_hash", 2).and_then(block_hash),
            medium: string(fields.get("medium", 6)),
        },
        Some("BlockRemoved") => KvEvent::Removed {
            hashes: hashes(fields.get("block_hashes", 1)),
            medium: string(fields.get("medium", 2)),
        },
        _ => KvEvent::Other,
    }
}

/// Decodes an `EventBatch` payload. Events wrapped as msgpack bin (the
/// llm-d storage publisher's format) are unwrapped.
pub fn decode_batch(payload: &[u8]) -> Result<Vec<KvEvent>, DecodeError> {
    let Value::Array(batch) = parse(payload)? else {
        return Err(DecodeError::Shape);
    };
    let Some(Value::Array(events)) = batch.get(1) else {
        return Err(DecodeError::Shape);
    };
    events
        .iter()
        .map(|e| match e {
            Value::Bin(inner) => parse(inner).map(|v| event(&v)),
            other => Ok(event(other)),
        })
        .collect()
}

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
    /// Replay from `next()` first; `reset` when the publisher started over.
    Replay {
        reset: bool,
    },
}

impl Sequence {
    /// What to do with a live message before applying it.
    fn live(&mut self, seq: u64) -> Live {
        match self.live.replace(seq) {
            Some(prev) if seq == prev => return Live::Skip,
            Some(prev) if seq < prev => {
                self.applied = None;
                return Live::Replay { reset: true };
            }
            _ => {}
        }
        match self.applied {
            Some(last) if seq <= last => Live::Skip,
            Some(last) if seq - last > 1 => Live::Replay { reset: false },
            Some(_) => Live::Apply,
            None if seq > 0 => Live::Replay { reset: false },
            None => Live::Apply,
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
                Live::Replay { reset } => {
                    let note = if reset {
                        Some(Feed::Reset)
                    } else if sequence.applied.is_some() {
                        Some(Feed::Gap)
                    } else {
                        None
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

    use crate::chains::{
        ChainIndex, Chains, DecodeError, KvEvent, LeafEdge, Position, Rank, decode_batch, subscribe,
    };
    use crate::config::ChainPolicy;
    use crate::layout::BlockHash;
    use crate::shutdown::Shutdown;
    use crate::stats::Stats;

    fn h(n: u64) -> BlockHash {
        BlockHash(n)
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
            .collect()
    }

    // Generated with msgspec from vLLM v0.31.0's kv_events.py structs: one
    // BlockStored of [0x11, 0x12] (root), one of [0x13] with parent 0x12, a GPU
    // BlockRemoved of 0x12, a STORAGE BlockRemoved of 0x13, AllBlocksCleared.
    // Hashes are 32 bytes (0xaa * 24 + the u64).
    const GOLDEN_V031: &str = "93cb41da39de00200000958aa474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657392c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000011c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f696473dc0020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1faa626c6f636b5f73697a6510a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c0a967726f75705f69647800b26b765f63616368655f737065635f6b696e64ae66756c6c5f617474656e74696f6e88a474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000013b1706172656e745f626c6f636b5f68617368c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012a9746f6b656e5f696473dc0010000102030405060708090a0b0c0d0e0faa626c6f636b5f73697a6510a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c083a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012a66d656469756da347505584a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000013a66d656469756da753544f52414745a967726f75705f6964780081a474797065b0416c6c426c6f636b73436c656172656400";
    // Same, with array_like tagged events (vLLM <= 0.11) and int hashes.
    const GOLDEN_LEGACY: &str = "93cb41da39de002000009297ab426c6f636b53746f7265649221222092010210c0a347505593ac426c6f636b52656d6f7665649122a753544f52414745c0";

    fn stored(parent: Option<u64>, hashes: &[u64], medium: &str) -> KvEvent {
        KvEvent::Stored {
            parent: parent.map(BlockHash),
            hashes: hashes.iter().copied().map(BlockHash).collect(),
            digests: Vec::new(),
            medium: Some(medium.to_string()),
        }
    }

    fn removed(hashes: &[u64], medium: &str) -> KvEvent {
        KvEvent::Removed {
            hashes: hashes.iter().copied().map(BlockHash).collect(),
            medium: Some(medium.to_string()),
        }
    }

    fn digest(n: u64) -> Vec<u8> {
        let mut d = vec![0xaa; 24];
        d.extend(n.to_be_bytes());
        d
    }

    fn with_digests(mut e: KvEvent) -> KvEvent {
        if let KvEvent::Stored {
            hashes, digests, ..
        } = &mut e
        {
            *digests = hashes.iter().map(|h| digest(h.0)).collect();
        }
        e
    }

    #[test]
    fn decodes_vllm_031_map_encoded_batch() {
        let events = decode_batch(&unhex(GOLDEN_V031)).expect("decode");
        assert_eq!(
            events,
            vec![
                with_digests(stored(None, &[0x11, 0x12], "GPU")),
                with_digests(stored(Some(0x12), &[0x13], "GPU")),
                removed(&[0x12], "GPU"),
                removed(&[0x13], "STORAGE"),
                KvEvent::Other,
            ]
        );
    }

    #[test]
    fn decodes_legacy_array_encoded_batch() {
        let events = decode_batch(&unhex(GOLDEN_LEGACY)).expect("decode");
        assert_eq!(
            events,
            vec![
                stored(Some(0x20), &[0x21, 0x22], "GPU"),
                removed(&[0x22], "STORAGE"),
            ]
        );
    }

    #[test]
    fn bin_wrapped_events_are_unwrapped() {
        // [ts, [bin(["BlockRemoved", [7], "STORAGE"])]], the llm-d storage publisher format.
        let inner = unhex("93ac426c6f636b52656d6f7665649107a753544f52414745");
        let mut payload = unhex("92cb41da39de0020000091c4");
        payload.push(u8::try_from(inner.len()).expect("small"));
        payload.extend(&inner);
        assert_eq!(
            decode_batch(&payload).expect("decode"),
            vec![removed(&[7], "STORAGE")]
        );
    }

    #[test]
    fn truncated_and_malformed_payloads_are_errors() {
        let full = unhex(GOLDEN_V031);
        for cut in [1, 10, full.len() / 2, full.len() - 1] {
            assert!(decode_batch(&full[..cut]).is_err(), "cut at {cut}");
        }
        assert_eq!(decode_batch(&unhex("c0")), Err(DecodeError::Shape));
        assert_eq!(
            decode_batch(&unhex("92cb41da39de00200000c0")),
            Err(DecodeError::Shape)
        );
        assert_eq!(decode_batch(&unhex("c1")), Err(DecodeError::Marker(0xc1)));
        let deep: Vec<u8> = std::iter::repeat_n(0x91, 64).chain([0xc0]).collect();
        assert_eq!(decode_batch(&deep), Err(DecodeError::TooDeep));
        // A bin length far past the end must not allocate or panic.
        assert_eq!(
            decode_batch(&unhex("c6ffffffff")),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn chain_positions_follow_the_tree() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], true);
        ix.store(Some(h(2)), &[h(4)], true);
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Internal);
        assert_eq!(ix.position(h(3)), Position::Leaf);
        assert_eq!(ix.position(h(4)), Position::Leaf);
        assert_eq!(ix.position(h(99)), Position::Untracked);

        assert_eq!(ix.remove(h(3)), Position::Leaf);
        assert_eq!(ix.position(h(2)), Position::Internal, "4 is still on disk");
        assert_eq!(ix.remove(h(4)), Position::Leaf);
        assert_eq!(ix.position(h(2)), Position::Leaf);
        assert_eq!(ix.remove(h(2)), Position::Leaf);
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.remove(h(1)), Position::Root);
        assert_eq!(ix.len(), 0, "fully deleted chains leave nothing behind");
    }

    #[test]
    fn head_first_deletion_orphans_the_rest() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], true);
        assert_eq!(ix.remove(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Orphan);
        assert_eq!(ix.rank(h(2), 0, 3), Rank::Dead);
        assert_eq!(ix.remove(h(2)), Position::Orphan);
        assert_eq!(ix.remove(h(3)), Position::Orphan);
        assert_eq!(
            ix.len(),
            1,
            "2 keeps its parent link until the cap drops it"
        );
    }

    #[test]
    fn duplicate_stores_do_not_inflate_child_counts() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2)], true);
        ix.store(None, &[h(1), h(2)], true);
        ix.store(Some(h(1)), &[h(2)], true);
        assert_eq!(ix.remove(h(2)), Position::Leaf);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Childless);
    }

    #[test]
    fn restore_after_deletion_counts_again() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2)], true);
        ix.remove(h(2));
        ix.store(Some(h(1)), &[h(2)], true);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Interior);
        assert_eq!(ix.remove(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Orphan);
        ix.store(None, &[h(1)], true);
        assert_eq!(ix.position(h(2)), Position::Leaf, "parent rewritten");
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Interior);
    }

    #[test]
    fn late_parent_fills_in_a_placeholder_store() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1)], true);
        ix.store(None, &[h(2)], true);
        assert_eq!(ix.position(h(2)), Position::Root);
        ix.store(Some(h(1)), &[h(2)], true);
        assert_eq!(ix.position(h(2)), Position::Leaf);
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Interior);
    }

    #[test]
    fn rank_orders_dead_childless_overdue_interior() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], true);
        assert_eq!(ix.rank(h(3), 0, 2), Rank::Childless);
        assert_eq!(ix.rank(h(2), 0, 2), Rank::Interior);
        assert_eq!(ix.rank(h(2), 1, 2), Rank::Interior);
        assert_eq!(ix.rank(h(2), 2, 2), Rank::Overdue);
        assert_eq!(ix.rank(h(77), 0, 2), Rank::Childless, "untracked");
        ix.remove(h(1));
        assert_eq!(ix.rank(h(2), 0, 2), Rank::Dead);
        assert!(Rank::Dead < Rank::Childless);
        assert!(Rank::Childless < Rank::Overdue);
        assert!(Rank::Overdue < Rank::Interior);
    }

    #[test]
    fn cap_forgets_oldest_blocks_and_releases_their_parents() {
        let mut ix = ChainIndex::new(3);
        ix.store(None, &[h(1), h(2)], true);
        ix.store(None, &[h(10), h(11)], true);
        assert!(ix.len() <= 3);
        assert_eq!(ix.position(h(1)), Position::Untracked);
        assert_eq!(ix.position(h(11)), Position::Leaf);
        // 2 was forgotten too or its parent is gone; either way it is not Internal.
        assert_ne!(ix.position(h(2)), Position::Internal);

        let mut ix = ChainIndex::new(1000);
        for i in 0..10_000u64 {
            ix.store(None, &[h(i)], true);
            ix.remove(h(i));
        }
        assert_eq!(ix.len(), 0);
        let mut ix = ChainIndex::new(10);
        for i in 0..10_000u64 {
            ix.store(Some(h(i)), &[h(i + 1)], true);
        }
        assert!(ix.len() <= 10);
        assert!(ix.order.len() <= 21, "stale order entries are compacted");
    }

    #[test]
    fn chains_counts_deletions_and_ignores_non_storage_removals() {
        let chains = Chains::new(100, ChainPolicy::TailFirst, 2, None);
        chains.apply(&[stored(None, &[1, 2, 3], "GPU"), removed(&[3], "GPU")]);
        assert_eq!(
            chains.rank(h(2), 0),
            Rank::Interior,
            "GPU removal is not a disk removal"
        );
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.rank(h(2), 0), Rank::Childless);
        assert_eq!(chains.deleted(h(1)), Position::Root);
        assert_eq!(chains.deleted(h(2)), Position::Orphan);
        assert_eq!(chains.deleted(h(9)), Position::Untracked);
        let s = &chains.stats;
        assert_eq!(Stats::get(&s.blocks_stored), 3);
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_orphan), 1);
        assert_eq!(Stats::get(&s.deleted_untracked), 1);
        assert_eq!(Stats::get(&s.deleted_leaf), 0);
        assert_eq!(Stats::get(&s.undigested), 3, "test events carry int hashes");
    }

    #[test]
    fn a_parent_never_seen_on_disk_does_not_make_children_dead() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], false);
        assert_eq!(ix.position(h(2)), Position::Untracked);
        ix.store(None, &[h(2), h(3)], true);
        assert_eq!(
            ix.position(h(2)),
            Position::Root,
            "1 was never seen on disk: the known chain starts at 2"
        );
        assert_eq!(ix.rank(h(2), 0, 3), Rank::Interior);
        assert_eq!(ix.position(h(3)), Position::Leaf);
        ix.store(None, &[h(1)], true);
        assert_eq!(ix.position(h(2)), Position::Internal);
        assert_eq!(ix.position(h(1)), Position::Root);
        ix.remove(h(1));
        assert_eq!(ix.position(h(2)), Position::Orphan, "seen and then removed");
        assert_eq!(ix.rank(h(2), 0, 3), Rank::Dead);

        let mut ix = ChainIndex::new(100);
        ix.store(Some(h(99)), &[h(10), h(11)], true);
        assert_eq!(
            ix.rank(h(10), 0, 3),
            Rank::Interior,
            "parent unknown to kvreap"
        );
        assert_eq!(ix.position(h(10)), Position::Root);
    }

    #[test]
    fn disk_marks_before_parent_links_count_once() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(2)], true);
        ix.store(None, &[h(1)], true);
        ix.store(None, &[h(1), h(2)], false);
        ix.store(None, &[h(1), h(2)], false);
        assert_eq!(ix.position(h(2)), Position::Leaf);
        assert_eq!(ix.remove(h(2)), Position::Leaf);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Childless);
    }

    #[test]
    fn disk_medium_limits_which_stores_mark_disk() {
        let chains = Chains::new(100, ChainPolicy::TailFirst, 2, Some("STORAGE".into()));
        chains.apply(&[stored(None, &[1, 2, 3], "GPU")]);
        assert_eq!(
            chains.rank(h(1), 0),
            Rank::Childless,
            "GPU only: not on disk"
        );
        assert_eq!(Stats::get(&chains.stats.blocks_stored), 0);
        // The fs tier's own events carry only the hash.
        chains.apply(&[
            stored(None, &[1], "STORAGE"),
            stored(None, &[2], "STORAGE"),
            stored(None, &[3], "STORAGE"),
        ]);
        assert_eq!(Stats::get(&chains.stats.blocks_stored), 3);
        assert_eq!(chains.rank(h(1), 0), Rank::Interior);
        assert_eq!(chains.rank(h(3), 0), Rank::Childless);
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.rank(h(2), 0), Rank::Childless);
        assert_eq!(chains.deleted(h(2)), Position::Leaf);
        assert_eq!(chains.deleted(h(1)), Position::Root);
    }

    #[test]
    fn subtree_walks_on_disk_children_and_counts_leaves() {
        //   1 ── 2 ── 3
        //        └─── 4 ── 5
        //   6 (separate root)
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], true);
        ix.store(Some(h(2)), &[h(4), h(5)], true);
        ix.store(None, &[h(6)], true);
        for n in [1, 2, 3, 4, 5] {
            ix.set_digest(h(n), &[0xab, u8::try_from(n).expect("small")]);
        }
        assert_eq!(ix.shape(h(1), 100), (5, 2));
        assert_eq!(ix.shape(h(4), 100), (2, 1));
        assert_eq!(
            ix.shape(h(6), 100),
            (1, 1),
            "a childless block is its own leaf"
        );
        assert_eq!(ix.shape(h(1), 2), (2, 1), "budget stops the walk");
        let mut below: Vec<_> = ix.descendants(h(1), 100);
        below.sort_by_key(|d| d.hash.0);
        assert_eq!(
            below.iter().map(|d| d.hash.0).collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(below[0].file_name.as_deref(), Some("ab02.bin"));
        assert_eq!(ix.descendants(h(1), 2).len(), 2);
        assert!(ix.descendants(h(6), 100).is_empty());

        ix.remove(h(5));
        assert_eq!(ix.shape(h(1), 100), (4, 2), "4 is now a leaf");
        assert_eq!(ix.descendants(h(2), 100).len(), 2);
        ix.store(None, &[h(4)], false);
        assert_eq!(
            ix.descendants(h(2), 100).len(),
            2,
            "re-linking adds no duplicate child"
        );
    }

    #[test]
    fn leaf_edge_stops_at_the_last_fork_and_descends_shared_prefixes() {
        //   1 ── 2 ──┬── 3 ── 4        (system prompt 1-2, two conversations)
        //            └── 5 ── 6 ── 7
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3), h(4)], true);
        std::thread::sleep(Duration::from_millis(5));
        ix.store(Some(h(2)), &[h(5), h(6), h(7)], true);
        for n in 1..=7 {
            ix.set_digest(h(n), &[u8::try_from(n).expect("small")]);
        }
        let hashes = |e: LeafEdge| e.blocks.iter().map(|b| b.hash.0).collect::<Vec<_>>();
        assert_eq!(ix.leaf_edge(h(6), 100).map(hashes), Some(vec![5, 6, 7]));
        assert_eq!(ix.leaf_edge(h(4), 100).map(hashes), Some(vec![3, 4]));
        assert_eq!(
            ix.leaf_edge(h(1), 100).map(hashes),
            Some(vec![3, 4]),
            "a shared prefix maps to its oldest continuation, never to itself"
        );
        let edge = ix.leaf_edge(h(7), 100).expect("edge");
        assert_eq!(edge.blocks[0].file_name.as_deref(), Some("05.bin"));
        assert_eq!(
            edge.newest_store,
            ix.nodes.get(&h(7)).and_then(|n| n.stored_at)
        );

        for n in [4, 3] {
            ix.remove(h(n));
        }
        assert_eq!(
            ix.leaf_edge(h(1), 100).map(hashes),
            Some(vec![1, 2, 5, 6, 7]),
            "with one continuation left the prompt is part of its edge"
        );
        assert_eq!(ix.leaf_edge(h(99), 100), None);
        assert_eq!(ix.leaf_edge(h(1), 2), None, "budget");
    }

    #[test]
    fn leaf_edge_of_an_orphan_starts_at_the_missing_parent() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], true);
        ix.remove(h(1));
        let edge = ix.leaf_edge(h(3), 100).expect("edge");
        assert_eq!(
            edge.blocks.iter().map(|b| b.hash.0).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn subtree_key_prefers_dead_then_most_blocks_per_leaf() {
        // Shared prefix 1-2 with four continuations; unshared chain 10-11-12.
        let chains = Chains::new(100, ChainPolicy::Subtree, 2, None);
        chains.apply(&[
            stored(None, &[1, 2], "GPU"),
            stored(Some(2), &[3], "GPU"),
            stored(Some(2), &[4], "GPU"),
            stored(Some(2), &[5], "GPU"),
            stored(Some(2), &[6], "GPU"),
            stored(None, &[10, 11, 12], "GPU"),
        ]);
        let (shared, unshared, leaf) = (
            chains.subtree_key(h(1)),
            chains.subtree_key(h(10)),
            chains.subtree_key(h(12)),
        );
        assert!(unshared < shared, "3 blocks per leaf beats 6 / 4");
        assert!(unshared < leaf, "a whole chain beats its last block");
        assert!(
            shared < leaf,
            "6 blocks for 4 continuations beats 1 block for 1"
        );
        chains.deleted(h(10));
        let dead = chains.subtree_key(h(11));
        assert!(dead < unshared.min(shared).min(leaf), "dead blocks first");
    }

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

        use crate::chains::plan_peers;

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
        use crate::chains::{Live, Sequence};

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
        use crate::chains::{Live, Sequence};

        let mut join = Sequence::default();
        assert_eq!(
            join.live(41),
            Live::Replay { reset: false },
            "joined mid-stream"
        );
        assert_eq!(join.next(), 0);
        assert_eq!(join.advance(41), Some(41), "nothing replayed: 0..41 lost");

        let mut gap = Sequence::default();
        gap.advance(4);
        assert_eq!(gap.live(7), Live::Replay { reset: false });
        assert_eq!(gap.next(), 5);
        assert_eq!(gap.advance(5), Some(0), "replayed");
        assert_eq!(gap.advance(7), Some(1), "6 fell out of the replay buffer");
    }

    #[test]
    fn sequence_starts_over_when_the_publisher_restarts() {
        use crate::chains::{Live, Sequence};

        let mut seq = Sequence::default();
        for n in 0..10 {
            seq.live(n);
            seq.advance(n);
        }
        assert_eq!(seq.live(0), Live::Replay { reset: true });
        assert_eq!(seq.next(), 0);
        assert_eq!(seq.advance(0), Some(0));
        assert_eq!(seq.live(1), Live::Apply);
    }

    #[test]
    fn sequenced_frames_need_three_parts_and_an_eight_byte_seq() {
        use crate::chains::sequenced;

        let frames = |seq: &[u8]| vec![b"topic".to_vec(), seq.to_vec(), b"payload".to_vec()];
        assert_eq!(
            sequenced(frames(&7u64.to_be_bytes())),
            Some((7, b"payload".to_vec()))
        );
        assert_eq!(sequenced(frames(&[7])), None);
        assert_eq!(sequenced(vec![b"payload".to_vec()]), None);
    }

    /// `[ts, [BlockStored(hashes, parent, ..., "STORAGE")], dp_rank]` in the
    /// array-like encoding.
    fn stored_batch(parent: Option<u64>, hashes: &[u64]) -> Vec<u8> {
        let mut w = Vec::new();
        rmp::encode::write_array_len(&mut w, 3).expect("encode");
        rmp::encode::write_f64(&mut w, 1_760_000_000.5).expect("encode");
        rmp::encode::write_array_len(&mut w, 1).expect("encode");
        rmp::encode::write_array_len(&mut w, 7).expect("encode");
        rmp::encode::write_str(&mut w, "BlockStored").expect("encode");
        rmp::encode::write_array_len(&mut w, hashes.len() as u32).expect("encode");
        for hash in hashes {
            rmp::encode::write_uint(&mut w, *hash).expect("encode");
        }
        match parent {
            Some(p) => rmp::encode::write_uint(&mut w, p)
                .map(drop)
                .expect("encode"),
            None => rmp::encode::write_nil(&mut w).expect("encode"),
        }
        rmp::encode::write_array_len(&mut w, 0).expect("encode");
        rmp::encode::write_uint(&mut w, 16).expect("encode");
        rmp::encode::write_nil(&mut w).expect("encode");
        rmp::encode::write_str(&mut w, "STORAGE").expect("encode");
        rmp::encode::write_nil(&mut w).expect("encode");
        w
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
