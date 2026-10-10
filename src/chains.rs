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
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::config::ChainPolicy;
use crate::layout::BlockHash;
use crate::shutdown::Shutdown;
use crate::stats::Stats;

pub const INDEX_CAP: usize = 4 << 20;
const SUBTREE_BUDGET: usize = 4096;
const RECV_TIMEOUT_MS: i32 = 500;
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

    pub fn position(&self, hash: BlockHash) -> Position {
        let Some(node) = self.nodes.get(&hash).filter(|n| n.on_disk) else {
            return Position::Untracked;
        };
        match node.parent {
            None => Position::Root,
            Some(p) if !self.on_disk(p) => Position::Orphan,
            Some(_) if node.children_on_disk > 0 => Position::Internal,
            Some(_) => Position::Leaf,
        }
    }

    pub fn rank(&self, hash: BlockHash, deferrals: u32, max_deferrals: u32) -> Rank {
        let Some(node) = self.nodes.get(&hash).filter(|n| n.on_disk) else {
            return Rank::Childless;
        };
        if node.parent.is_some_and(|p| !self.on_disk(p)) {
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

/// Subscribes to every endpoint and feeds decoded batches into `chains` until shutdown.
pub fn subscribe(
    endpoints: &[String],
    chains: &Arc<Chains>,
    shutdown: &Shutdown,
) -> Result<(), zmq::Error> {
    let ctx = zmq::Context::new();
    let sub = ctx.socket(zmq::SUB)?;
    sub.set_subscribe(b"")?;
    sub.set_rcvtimeo(RECV_TIMEOUT_MS)?;
    sub.set_rcvhwm(0)?;
    for endpoint in endpoints {
        sub.connect(endpoint)?;
        tracing::info!(endpoint, "subscribed to KV cache events");
    }
    while !shutdown.is_set() {
        let frames = match sub.recv_multipart(0) {
            Ok(f) => f,
            Err(zmq::Error::EAGAIN) => continue,
            Err(e) => {
                tracing::warn!(error = %e, "KV events receive failed");
                shutdown.wait(Duration::from_millis(100));
                continue;
            }
        };
        let Some(payload) = frames.last() else {
            continue;
        };
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
    Ok(())
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
    fn parent_links_without_disk_marks_make_children_orphans() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], false);
        assert_eq!(ix.position(h(2)), Position::Untracked);
        ix.store(None, &[h(2), h(3)], true);
        assert_eq!(ix.position(h(2)), Position::Orphan, "1 never reached disk");
        assert_eq!(ix.rank(h(2), 0, 3), Rank::Dead);
        assert_eq!(ix.position(h(3)), Position::Leaf);
        ix.store(None, &[h(1)], true);
        assert_eq!(ix.position(h(2)), Position::Internal);
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.rank(h(1), 0, 3), Rank::Interior);
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

    #[test]
    fn subscriber_applies_batches_from_a_real_pub_socket() {
        let ctx = zmq::Context::new();
        let publisher = ctx.socket(zmq::PUB).expect("pub");
        publisher.bind("tcp://127.0.0.1:*").expect("bind");
        let endpoint = publisher
            .get_last_endpoint()
            .expect("endpoint")
            .expect("utf8");
        let chains = Arc::new(Chains::new(100, ChainPolicy::TailFirst, 2, None));
        let shutdown = Arc::new(Shutdown::default());
        let handle = {
            let (chains, shutdown) = (Arc::clone(&chains), Arc::clone(&shutdown));
            std::thread::spawn(move || subscribe(&[endpoint], &chains, &shutdown))
        };
        let payload = unhex(GOLDEN_V031);
        let deadline = Instant::now() + Duration::from_secs(10);
        while Stats::get(&chains.stats.batches) == 0 && Instant::now() < deadline {
            publisher
                .send_multipart([b"".as_slice(), &1u64.to_be_bytes(), &payload], 0)
                .expect("send");
            std::thread::sleep(Duration::from_millis(50));
        }
        publisher
            .send_multipart([b"".as_slice(), &2u64.to_be_bytes(), b"\xc1"], 0)
            .expect("send");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Stats::get(&chains.stats.decode_errors) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        shutdown.trigger();
        handle.join().expect("join").expect("subscribe");
        assert!(Stats::get(&chains.stats.batches) >= 1);
        assert_eq!(Stats::get(&chains.stats.decode_errors), 1);
        assert_eq!(
            chains.rank(h(0x12), 0),
            Rank::Childless,
            "0x13 removed from STORAGE"
        );
        assert_eq!(chains.deleted(h(0x11)), Position::Root);
    }
}
