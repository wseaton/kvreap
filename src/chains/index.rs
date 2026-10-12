//! The `block -> parent` tree of blocks vLLM has announced, with which of them
//! are on disk.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use crate::layout::BlockHash;

/// Blocks walked per leaf edge or subtree.
pub const WALK_BUDGET: usize = 4096;

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

/// What the index knows about a block's file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residence {
    /// Never seen on disk: only a parent link or a store on another medium.
    Unknown,
    OnDisk,
    /// Seen on disk and then removed. A block whose parent was never seen on
    /// disk (vLLM wrote it before kvreap started) is not dead; one whose
    /// parent is gone, and not held by a backing tier, is.
    Gone,
}

/// How a store event marks its blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    /// The blocks were written to the tier kvreap evicts.
    OnDisk,
    /// The blocks were written to a tier that serves the lookups this tier
    /// misses, so a block here whose parent is only there is still reachable.
    Backing,
    /// The blocks were computed into or loaded back to the GPU or CPU tier:
    /// for a block already on disk here, its prefix was just used.
    Used,
    /// Another tier's copy of the blocks; only their parent links count.
    LinksOnly,
}

/// Whether the backing tier holds a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backup {
    Missing,
    Held,
}

#[derive(Debug, Clone)]
struct Node {
    parent: Option<BlockHash>,
    /// Children on disk whose parent link points here.
    children: Vec<BlockHash>,
    residence: Residence,
    backup: Backup,
    generation: u64,
    /// Full block hash, which names the block's file.
    digest: Option<Arc<[u8]>>,
    /// When the block was last written to disk or used from any tier.
    used_at: Option<SystemTime>,
    /// Its key in `ChainIndex::leaves` while it is a named on-disk leaf.
    leaf_key: Option<SystemTime>,
}

/// A radix-tree leaf edge: the on-disk blocks from just below the last fork
/// (or a chain root, or a missing parent) down to a leaf, root side first.
/// Only a leaf edge serves a single cached prefix, so it is the unit of
/// connectivity-based eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEdge {
    pub blocks: Vec<Descendant>,
    /// Most recent write or use of any block in the edge.
    pub newest_use: SystemTime,
}

/// A block below a subtree root, with its digest if known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descendant {
    pub hash: BlockHash,
    pub digest: Option<Arc<[u8]>>,
}

impl Descendant {
    /// `<hex digest>.bin`, the block's file name.
    pub fn file_name(&self) -> Option<String> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = self.digest.as_deref()?;
        let mut name = String::with_capacity(digest.len() * 2 + 4);
        for b in digest {
            name.push(char::from(HEX[usize::from(b >> 4)]));
            name.push(char::from(HEX[usize::from(b & 0xf)]));
        }
        name.push_str(".bin");
        Some(name)
    }
}

/// Bounded `block -> parent` map with on-disk child counts. When over `cap`,
/// the oldest-inserted blocks are forgotten first.
///
/// `leaves` orders every named on-disk leaf by a key that is never later than
/// the newest use on its leaf edge: a leaf enters at its own last use, a use
/// of the leaf raises the key, a use further up the edge is found when the
/// leaf reaches the front ([`ChainIndex::oldest_leaf`]), and a fork or a
/// removal that cuts the edge drops the key back to the leaf's own last use.
///
/// ```text
///   leaves: (t1, a3) (t4, b2) (t9, c1)        t = key, oldest first
///   s0 ── s1 ──┬── a1 ── a2 ── a3            edge [a1 a2 a3], newest use t1
///              └── b1 ── b2                  edge [b1 b2],    newest use t4
///   c1                                       edge [c1],       newest use t9
/// ```
#[derive(Debug)]
pub struct ChainIndex {
    cap: usize,
    nodes: HashMap<BlockHash, Node>,
    order: VecDeque<(BlockHash, u64)>,
    next_generation: u64,
    on_disk: usize,
    leaves: BTreeSet<(SystemTime, BlockHash)>,
    /// Edge cuts so far: a raise found before a cut may be stale.
    cuts: u64,
    /// On-disk blocks whose parent went unreachable; checked when taken.
    dead: BTreeSet<BlockHash>,
}

/// The leaf whose edge was used least recently, and the leaves whose keys
/// trailed their edge's newest use, with that use, as of `cuts`.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Oldest {
    pub leaf: Option<(BlockHash, SystemTime)>,
    pub behind: Vec<(BlockHash, SystemTime)>,
    pub cuts: u64,
}

impl ChainIndex {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            nodes: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            on_disk: 0,
            leaves: BTreeSet::new(),
            cuts: 0,
            dead: BTreeSet::new(),
        }
    }

    /// Blocks marked on disk.
    pub fn on_disk_count(&self) -> usize {
        self.on_disk
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
            residence: Residence::Unknown,
            backup: Backup::Missing,
            generation,
            digest: None,
            used_at: None,
            leaf_key: None,
        })
    }

    /// Removes `hash` from the map and from the leaf and dead sets.
    fn forget(&mut self, hash: BlockHash) -> Option<Node> {
        let node = self.nodes.remove(&hash)?;
        if let Some(key) = node.leaf_key {
            self.leaves.remove(&(key, hash));
        }
        self.dead.remove(&hash);
        Some(node)
    }

    /// Puts `hash` in `leaves` while it is a named on-disk block with no
    /// on-disk child, and out of it otherwise.
    fn sync_leaf(&mut self, hash: BlockHash) {
        let Some(n) = self.nodes.get_mut(&hash) else {
            return;
        };
        let leaf = n.residence == Residence::OnDisk && n.children.is_empty() && n.digest.is_some();
        let key = leaf.then(|| n.used_at.max(n.leaf_key).unwrap_or(SystemTime::UNIX_EPOCH));
        if key == n.leaf_key {
            return;
        }
        if let Some(old) = n.leaf_key {
            self.leaves.remove(&(old, hash));
        }
        if let Some(new) = key {
            self.leaves.insert((new, hash));
        }
        n.leaf_key = key;
    }

    fn child_stored(&mut self, parent: BlockHash, child: BlockHash) {
        let children = &mut self.node_mut(parent).children;
        if children.contains(&child) {
            return;
        }
        children.push(child);
        if let [first, _] = children.as_slice() {
            let first = *first;
            self.cut_above(first);
        }
    }

    /// The leaf edge through `hash` just lost the blocks above `hash`: drops
    /// a key raised by a use up there back to the leaf's own last use.
    fn cut_above(&mut self, hash: BlockHash) {
        self.cuts += 1;
        let mut leaf = hash;
        for _ in 0..WALK_BUDGET {
            match self.nodes.get(&leaf).map(|n| n.children.as_slice()) {
                Some([only]) => leaf = *only,
                Some([]) => break,
                _ => return,
            }
        }
        let Some(n) = self.nodes.get_mut(&leaf) else {
            return;
        };
        let (Some(key), Some(own)) = (n.leaf_key, n.used_at) else {
            return;
        };
        if key > own {
            self.leaves.remove(&(key, leaf));
            self.leaves.insert((own, leaf));
            n.leaf_key = Some(own);
        }
    }

    fn child_removed(&mut self, parent: Option<BlockHash>, child: BlockHash) {
        let Some(p) = parent else {
            return;
        };
        let Some(n) = self.nodes.get_mut(&p) else {
            return;
        };
        n.children.retain(|c| *c != child);
        if n.residence != Residence::OnDisk && n.children.is_empty() && n.parent.is_none() {
            self.forget(p);
        } else {
            self.sync_leaf(p);
        }
    }

    /// Records `hashes` as a chain hanging off `parent`.
    pub fn store(&mut self, parent: Option<BlockHash>, hashes: &[BlockHash], store: Store) {
        let mut linked = Vec::new();
        let mut parent = parent;
        for &hash in hashes {
            linked.extend(self.store_one(hash, parent, store));
            parent = Some(hash);
        }
        for &hash in hashes.iter().chain(&linked) {
            self.sync_leaf(hash);
        }
        for &hash in hashes {
            if self.position(hash) == Position::Orphan {
                self.dead.insert(hash);
            }
        }
        self.enforce_cap();
    }

    /// Returns the parent `hash` was linked under as an on-disk child.
    fn store_one(
        &mut self,
        hash: BlockHash,
        parent: Option<BlockHash>,
        store: Store,
    ) -> Option<BlockHash> {
        let n = self.node_mut(hash);
        let parent = n.parent.or(parent);
        n.parent = parent;
        let newly_on_disk = store == Store::OnDisk && n.residence != Residence::OnDisk;
        match store {
            Store::OnDisk if newly_on_disk => {
                n.residence = Residence::OnDisk;
                n.used_at = Some(SystemTime::now());
            }
            Store::Used if n.residence == Residence::OnDisk => {
                n.used_at = Some(SystemTime::now());
            }
            Store::Backing => n.backup = Backup::Held,
            Store::OnDisk | Store::Used | Store::LinksOnly => {}
        }
        let residence = n.residence;
        if newly_on_disk {
            self.on_disk += 1;
        }
        let (Residence::OnDisk, Some(p)) = (residence, parent) else {
            return None;
        };
        self.child_stored(p, hash);
        Some(p)
    }

    /// Records the full hash that names `hash`'s file.
    pub fn set_digest(&mut self, hash: BlockHash, digest: &[u8]) {
        if let Some(n) = self.nodes.get_mut(&hash)
            && n.digest.is_none()
        {
            n.digest = Some(digest.into());
            self.sync_leaf(hash);
        }
    }

    fn on_disk_children(&self, hash: BlockHash) -> impl Iterator<Item = BlockHash> + '_ {
        self.nodes
            .get(&hash)
            .map(|n| n.children.as_slice())
            .unwrap_or_default()
            .iter()
            .copied()
    }

    fn descendant(&self, hash: BlockHash) -> Descendant {
        let digest = self.nodes.get(&hash).and_then(|n| n.digest.clone());
        Descendant { hash, digest }
    }

    /// Visits the leaf edge `hash` maps to from its leaf up to its top;
    /// `None` when `hash` is not on disk or the walk exceeds `budget`.
    fn walk_edge(
        &self,
        hash: BlockHash,
        budget: usize,
        mut visit: impl FnMut(BlockHash),
    ) -> Option<()> {
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
                .min_by_key(|c| self.nodes.get(c).and_then(|n| n.used_at));
            match oldest_child {
                Some(c) => leaf = c,
                None => break,
            }
        }
        visit(leaf);
        let (mut top, mut len) = (leaf, 1);
        while let Some(p) = self.nodes.get(&top).and_then(|n| n.parent) {
            let single = self.on_disk(p) && self.on_disk_children(p).take(2).count() == 1;
            if !single || len >= budget {
                break;
            }
            visit(p);
            top = p;
            len += 1;
        }
        Some(())
    }

    /// The newest write or use on the leaf edge `hash` maps to, without
    /// building the edge.
    pub fn edge_newest(&self, hash: BlockHash, budget: usize) -> Option<SystemTime> {
        let mut newest = None;
        self.walk_edge(hash, budget, |b| {
            newest = newest.max(self.nodes.get(&b).and_then(|n| n.used_at));
        })?;
        newest
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
        let mut blocks = Vec::new();
        let mut newest_use = None;
        self.walk_edge(hash, budget, |b| {
            newest_use = newest_use.max(self.nodes.get(&b).and_then(|n| n.used_at));
            blocks.push(self.descendant(b));
        })?;
        blocks.reverse();
        Some(LeafEdge {
            blocks,
            newest_use: newest_use?,
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

    /// Scans `leaves` oldest key first for the leaf edge whose newest use is
    /// oldest, skipping leaves `skip` rejects. A leaf's key never runs ahead
    /// of its edge, so the scan stops at the first key past the best edge
    /// found, or after walking `scan` edges.
    pub fn oldest_leaf(
        &self,
        budget: usize,
        scan: usize,
        skip: impl Fn(BlockHash) -> bool,
    ) -> Oldest {
        let mut out = Oldest {
            cuts: self.cuts,
            ..Oldest::default()
        };
        let mut walked = 0;
        for &(key, leaf) in &self.leaves {
            if walked >= scan || out.leaf.is_some_and(|(_, best)| key >= best) {
                break;
            }
            if skip(leaf) {
                continue;
            }
            walked += 1;
            let Some(newest) = self.edge_newest(leaf, budget) else {
                continue;
            };
            if newest > key {
                out.behind.push((leaf, newest));
            }
            if out.leaf.is_none_or(|(_, best)| newest < best) {
                out.leaf = Some((leaf, newest));
            }
        }
        out
    }

    /// Moves the leaves of `oldest.behind` up to the uses found on their
    /// edges, unless an edge was cut since.
    pub fn raise(&mut self, oldest: &Oldest) {
        if oldest.cuts != self.cuts {
            return;
        }
        for &(leaf, newest) in &oldest.behind {
            self.raise_one(leaf, newest);
        }
    }

    fn raise_one(&mut self, leaf: BlockHash, newest: SystemTime) {
        let Some(n) = self.nodes.get_mut(&leaf) else {
            return;
        };
        let Some(key) = n.leaf_key.filter(|k| *k < newest) else {
            return;
        };
        self.leaves.remove(&(key, leaf));
        self.leaves.insert((newest, leaf));
        n.leaf_key = Some(newest);
    }

    /// The first named dead block (on disk, parent unreachable) that `claim`
    /// accepts, with its claim. Drops entries that are no longer dead.
    pub fn take_dead<C>(
        &mut self,
        mut claim: impl FnMut(BlockHash) -> Option<C>,
    ) -> Option<(Descendant, C)> {
        let mut stale = Vec::new();
        let mut found = None;
        for &hash in &self.dead {
            let named = self.nodes.get(&hash).is_some_and(|n| n.digest.is_some());
            if !named || self.position(hash) != Position::Orphan {
                stale.push(hash);
            } else if let Some(c) = claim(hash) {
                found = Some((self.descendant(hash), c));
                break;
            }
        }
        for hash in stale {
            self.dead.remove(&hash);
        }
        found
    }

    pub fn has_dead(&self) -> bool {
        !self.dead.is_empty()
    }

    /// Whether `hash` is on disk with a name, so the leaf order covers it.
    pub fn named_on_disk(&self, hash: BlockHash) -> bool {
        self.nodes
            .get(&hash)
            .is_some_and(|n| n.residence == Residence::OnDisk && n.digest.is_some())
    }

    /// Marks `hash` gone from disk and returns where it sat in its chain.
    pub fn remove(&mut self, hash: BlockHash) -> Position {
        let position = self.position(hash);
        let Some(node) = self.nodes.get_mut(&hash) else {
            return position;
        };
        if node.residence != Residence::OnDisk {
            return position;
        }
        node.residence = Residence::Gone;
        self.on_disk = self.on_disk.saturating_sub(1);
        let parent = node.parent;
        let children = node.children.clone();
        if node.backup == Backup::Missing {
            self.dead.extend(children.iter().copied());
        }
        if children.is_empty() {
            self.forget(hash);
        } else {
            self.dead.remove(&hash);
        }
        for child in children {
            self.cut_above(child);
        }
        self.child_removed(parent, hash);
        position
    }

    fn residence(&self, hash: BlockHash) -> Residence {
        self.nodes
            .get(&hash)
            .map_or(Residence::Unknown, |n| n.residence)
    }

    pub fn on_disk(&self, hash: BlockHash) -> bool {
        self.residence(hash) == Residence::OnDisk
    }

    /// Records that the backing tier dropped `hash`.
    pub fn unback(&mut self, hash: BlockHash) {
        if let Some(n) = self.nodes.get_mut(&hash) {
            n.backup = Backup::Missing;
            if n.residence == Residence::Gone {
                self.dead.extend(n.children.iter().copied());
            }
        }
    }

    /// Gone from this tier and not held by the backing tier, so no prefix
    /// lookup reaches past it.
    fn unreachable(&self, hash: BlockHash) -> bool {
        self.nodes
            .get(&hash)
            .is_some_and(|n| n.residence == Residence::Gone && n.backup == Backup::Missing)
    }

    pub fn position(&self, hash: BlockHash) -> Position {
        let Some(node) = self
            .nodes
            .get(&hash)
            .filter(|n| n.residence == Residence::OnDisk)
        else {
            return Position::Untracked;
        };
        match node.parent {
            Some(p) if self.unreachable(p) => Position::Orphan,
            Some(p) if self.on_disk(p) && !node.children.is_empty() => Position::Internal,
            Some(p) if self.on_disk(p) => Position::Leaf,
            _ => Position::Root,
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
            if let Some(node) = current.then(|| self.forget(hash)).flatten()
                && node.residence == Residence::OnDisk
            {
                self.on_disk = self.on_disk.saturating_sub(1);
                self.child_removed(node.parent, hash);
            }
        }
        if self.order.len() > self.cap.saturating_mul(2) {
            let nodes = &self.nodes;
            self.order
                .retain(|(h, g)| nodes.get(h).is_some_and(|n| n.generation == *g));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::chains::index::{ChainIndex, LeafEdge, Position, Residence, Store, WALK_BUDGET};
    use crate::chains::testing::h;
    use crate::layout::BlockHash;

    fn children(ix: &ChainIndex, hash: BlockHash) -> usize {
        ix.nodes.get(&hash).map_or(0, |n| n.children.len())
    }

    /// Stores `chain` on disk under `parent` with a digest for every block.
    fn named(ix: &mut ChainIndex, parent: Option<u64>, chain: &[u64]) {
        let hashes: Vec<BlockHash> = chain.iter().copied().map(h).collect();
        ix.store(parent.map(h), &hashes, Store::OnDisk);
        for &n in chain {
            ix.set_digest(h(n), &n.to_be_bytes());
        }
    }

    fn used(ix: &mut ChainIndex, n: u64) {
        tick();
        ix.store(None, &[h(n)], Store::Used);
    }

    fn tick() {
        std::thread::sleep(Duration::from_millis(2));
    }

    /// Leaves oldest key first.
    fn leaves(ix: &ChainIndex) -> Vec<u64> {
        ix.leaves.iter().map(|(_, l)| l.0).collect()
    }

    fn oldest(ix: &ChainIndex) -> Option<u64> {
        ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false)
            .leaf
            .map(|(l, _)| l.0)
    }

    fn key(ix: &ChainIndex, n: u64) -> Option<SystemTime> {
        ix.nodes.get(&h(n)).and_then(|node| node.leaf_key)
    }

    /// `leaves` holds exactly the named on-disk blocks with no on-disk
    /// child, each under the key its node records, and no key is later than
    /// its edge's newest use.
    fn check_leaves(ix: &ChainIndex) {
        let want: BTreeSet<BlockHash> = ix
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.residence == Residence::OnDisk && n.children.is_empty() && n.digest.is_some()
            })
            .map(|(hash, _)| *hash)
            .collect();
        let have: BTreeSet<BlockHash> = ix.leaves.iter().map(|(_, l)| *l).collect();
        assert_eq!(have, want);
        assert_eq!(ix.leaves.len(), want.len(), "one entry per leaf");
        for &(key, leaf) in &ix.leaves {
            assert_eq!(ix.nodes.get(&leaf).and_then(|n| n.leaf_key), Some(key));
            let newest = ix.edge_newest(leaf, WALK_BUDGET).expect("on disk");
            assert!(key <= newest, "leaf {leaf:?} keyed past its edge");
        }
        for (hash, n) in &ix.nodes {
            if !want.contains(hash) {
                assert_eq!(n.leaf_key, None, "{hash:?} is not a leaf");
            }
        }
    }

    #[test]
    fn chain_positions_follow_the_tree() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], Store::OnDisk);
        ix.store(Some(h(2)), &[h(4)], Store::OnDisk);
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
        ix.store(None, &[h(1), h(2), h(3)], Store::OnDisk);
        assert_eq!(ix.remove(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Orphan);
        assert_eq!(ix.position(h(2)), Position::Orphan);
        assert_eq!(ix.remove(h(2)), Position::Orphan);
        assert_eq!(ix.remove(h(3)), Position::Orphan);
        assert_eq!(
            ix.len(),
            1,
            "2 keeps its parent link until the cap drops it"
        );
    }

    #[test]
    fn a_backed_parent_keeps_its_children_reachable() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], Store::OnDisk);
        ix.store(None, &[h(1), h(2)], Store::Backing);
        assert_eq!(
            ix.position(h(1)),
            Position::Root,
            "backing does not move blocks"
        );
        assert_eq!(ix.position(h(2)), Position::Internal);
        assert_eq!(ix.remove(h(2)), Position::Internal);
        assert_eq!(
            ix.position(h(3)),
            Position::Root,
            "a lookup that misses 2 here finds it on the backing tier"
        );
        assert_eq!(ix.remove(h(1)), Position::Root);
        ix.unback(h(2));
        assert_eq!(ix.position(h(3)), Position::Orphan, "gone from both tiers");
        ix.store(None, &[h(4)], Store::Backing);
        assert!(!ix.on_disk(h(4)), "a backing store is not a store here");
        assert_eq!(ix.remove(h(4)), Position::Untracked);
    }

    #[test]
    fn duplicate_stores_do_not_inflate_child_counts() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2)], Store::OnDisk);
        ix.store(None, &[h(1), h(2)], Store::OnDisk);
        ix.store(Some(h(1)), &[h(2)], Store::OnDisk);
        assert_eq!(ix.remove(h(2)), Position::Leaf);
        assert_eq!(children(&ix, h(1)), 0);
    }

    #[test]
    fn restore_after_deletion_counts_again() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2)], Store::OnDisk);
        ix.remove(h(2));
        ix.store(Some(h(1)), &[h(2)], Store::OnDisk);
        assert_eq!(children(&ix, h(1)), 1);
        assert_eq!(ix.remove(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Orphan);
        ix.store(None, &[h(1)], Store::OnDisk);
        assert_eq!(ix.position(h(2)), Position::Leaf, "parent rewritten");
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(children(&ix, h(1)), 1);
    }

    #[test]
    fn late_parent_fills_in_a_placeholder_store() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1)], Store::OnDisk);
        ix.store(None, &[h(2)], Store::OnDisk);
        assert_eq!(ix.position(h(2)), Position::Root);
        ix.store(Some(h(1)), &[h(2)], Store::OnDisk);
        assert_eq!(ix.position(h(2)), Position::Leaf);
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(children(&ix, h(1)), 1);
    }

    #[test]
    fn cap_forgets_oldest_blocks_and_releases_their_parents() {
        let mut ix = ChainIndex::new(3);
        ix.store(None, &[h(1), h(2)], Store::OnDisk);
        ix.store(None, &[h(10), h(11)], Store::OnDisk);
        assert!(ix.len() <= 3);
        assert_eq!(ix.position(h(1)), Position::Untracked);
        assert_eq!(ix.position(h(11)), Position::Leaf);
        // 2 was forgotten too or its parent is gone; either way it is not Internal.
        assert_ne!(ix.position(h(2)), Position::Internal);

        let mut ix = ChainIndex::new(1000);
        for i in 0..10_000u64 {
            ix.store(None, &[h(i)], Store::OnDisk);
            ix.remove(h(i));
        }
        assert_eq!(ix.len(), 0);
        let mut ix = ChainIndex::new(10);
        for i in 0..10_000u64 {
            ix.store(Some(h(i)), &[h(i + 1)], Store::OnDisk);
        }
        assert!(ix.len() <= 10);
        assert!(ix.order.len() <= 21, "stale order entries are compacted");
    }

    #[test]
    fn a_parent_never_seen_on_disk_does_not_make_children_dead() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], Store::LinksOnly);
        assert_eq!(ix.position(h(2)), Position::Untracked);
        ix.store(None, &[h(2), h(3)], Store::OnDisk);
        assert_eq!(
            ix.position(h(2)),
            Position::Root,
            "1 was never seen on disk: the known chain starts at 2"
        );
        assert_eq!(children(&ix, h(2)), 1);
        assert_eq!(ix.position(h(3)), Position::Leaf);
        ix.store(None, &[h(1)], Store::OnDisk);
        assert_eq!(ix.position(h(2)), Position::Internal);
        assert_eq!(ix.position(h(1)), Position::Root);
        ix.remove(h(1));
        assert_eq!(ix.position(h(2)), Position::Orphan, "seen and then removed");
        assert_eq!(ix.position(h(2)), Position::Orphan);

        let mut ix = ChainIndex::new(100);
        ix.store(Some(h(99)), &[h(10), h(11)], Store::OnDisk);
        assert_eq!(
            ix.position(h(10)),
            Position::Root,
            "parent unknown to kvreap"
        );
        assert_eq!(ix.position(h(10)), Position::Root);
    }

    #[test]
    fn disk_marks_before_parent_links_count_once() {
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(2)], Store::OnDisk);
        ix.store(None, &[h(1)], Store::OnDisk);
        ix.store(None, &[h(1), h(2)], Store::LinksOnly);
        ix.store(None, &[h(1), h(2)], Store::LinksOnly);
        assert_eq!(ix.position(h(2)), Position::Leaf);
        assert_eq!(ix.remove(h(2)), Position::Leaf);
        assert_eq!(children(&ix, h(1)), 0);
    }

    #[test]
    fn a_shared_prefix_forgets_conversations_that_left_the_disk() {
        let mut ix = ChainIndex::new(100_000);
        ix.store(None, &[h(1)], Store::OnDisk);
        for c in 0..1000u64 {
            ix.store(Some(h(1)), &[h(10_000 + c)], Store::LinksOnly);
            ix.store(Some(h(1)), &[h(10_000 + c)], Store::OnDisk);
            ix.remove(h(10_000 + c));
        }
        ix.store(Some(h(1)), &[h(2)], Store::OnDisk);
        assert_eq!(
            ix.nodes.get(&h(1)).map(|n| n.children.clone()),
            Some(vec![h(2)])
        );
        assert_eq!(ix.position(h(1)), Position::Root);
        assert_eq!(ix.position(h(2)), Position::Leaf);
    }

    #[test]
    fn descendants_walk_on_disk_children() {
        //   1 ── 2 ── 3
        //        └─── 4 ── 5
        //   6 (separate root)
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3)], Store::OnDisk);
        ix.store(Some(h(2)), &[h(4), h(5)], Store::OnDisk);
        ix.store(None, &[h(6)], Store::OnDisk);
        for n in [1, 2, 3, 4, 5] {
            ix.set_digest(h(n), &[0xab, u8::try_from(n).expect("small")]);
        }
        let mut below: Vec<_> = ix.descendants(h(1), 100);
        below.sort_by_key(|d| d.hash.0);
        assert_eq!(
            below.iter().map(|d| d.hash.0).collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(below[0].file_name().as_deref(), Some("ab02.bin"));
        assert_eq!(ix.descendants(h(1), 2).len(), 2);
        assert!(ix.descendants(h(6), 100).is_empty());

        ix.remove(h(5));
        assert_eq!(ix.descendants(h(2), 100).len(), 2);
        ix.store(None, &[h(4)], Store::LinksOnly);
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
        ix.store(None, &[h(1), h(2), h(3), h(4)], Store::OnDisk);
        std::thread::sleep(Duration::from_millis(5));
        ix.store(Some(h(2)), &[h(5), h(6), h(7)], Store::OnDisk);
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
        assert_eq!(edge.blocks[0].file_name().as_deref(), Some("05.bin"));
        assert_eq!(
            Some(edge.newest_use),
            ix.nodes.get(&h(7)).and_then(|n| n.used_at)
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
        ix.store(None, &[h(1), h(2), h(3)], Store::OnDisk);
        ix.remove(h(1));
        let edge = ix.leaf_edge(h(3), 100).expect("edge");
        assert_eq!(
            edge.blocks.iter().map(|b| b.hash.0).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn edge_newest_agrees_with_the_built_edge_everywhere() {
        //   1 ── 2 ──┬── 3 ── 4
        //            └── 5 ── 6 ── 7
        let mut ix = ChainIndex::new(100);
        ix.store(None, &[h(1), h(2), h(3), h(4)], Store::OnDisk);
        std::thread::sleep(Duration::from_millis(5));
        ix.store(Some(h(2)), &[h(5), h(6), h(7)], Store::OnDisk);
        for n in 1..=7 {
            assert_eq!(
                ix.edge_newest(h(n), 100),
                ix.leaf_edge(h(n), 100).map(|e| e.newest_use),
                "block {n}"
            );
        }
        assert_eq!(ix.edge_newest(h(99), 100), None);
        assert_eq!(ix.edge_newest(h(1), 2), None, "budget");
    }

    #[test]
    fn file_names_are_lowercase_hex_digests() {
        use crate::chains::Descendant;

        let d = Descendant {
            hash: h(1),
            digest: Some(Arc::from(&[0x00, 0x0f, 0xa0, 0xff][..])),
        };
        assert_eq!(d.file_name().as_deref(), Some("000fa0ff.bin"));
        assert_eq!(
            Descendant {
                hash: h(1),
                digest: None
            }
            .file_name(),
            None
        );
        let full: Vec<u8> = (0..32).collect();
        let name: String = full.iter().map(|b| format!("{b:02x}")).collect::<String>() + ".bin";
        let d = Descendant {
            hash: h(1),
            digest: Some(Arc::from(full.as_slice())),
        };
        assert_eq!(d.file_name(), Some(name));
    }

    #[test]
    fn leaves_follow_stores_extensions_forks_and_removals() {
        //   1 ── 2 ──┬── 3 ── 4
        //            └── 5
        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2, 3]);
        assert_eq!(leaves(&ix), vec![3]);
        ix.store(None, &[h(9)], Store::OnDisk);
        assert_eq!(
            leaves(&ix),
            vec![3],
            "a block without a name has no file to delete"
        );
        ix.set_digest(h(9), &[9]);
        assert_eq!(leaves(&ix), vec![3, 9]);
        tick();
        named(&mut ix, Some(3), &[4]);
        assert_eq!(
            leaves(&ix),
            vec![9, 4],
            "4 extends the chain and replaces 3"
        );
        tick();
        named(&mut ix, Some(2), &[5]);
        assert_eq!(leaves(&ix), vec![9, 4, 5], "a fork adds a leaf");
        check_leaves(&ix);
        ix.remove(h(4));
        assert_eq!(
            leaves(&ix),
            vec![3, 9, 5],
            "3 is a leaf again at its own last use"
        );
        ix.remove(h(5));
        ix.remove(h(9));
        assert_eq!(leaves(&ix), vec![3]);
        ix.store(None, &[h(1), h(2), h(3)], Store::LinksOnly);
        assert_eq!(leaves(&ix), vec![3], "links add no leaf");
        for n in [3, 2, 1] {
            ix.remove(h(n));
            check_leaves(&ix);
        }
        assert!(ix.leaves.is_empty());
        assert_eq!(ix.len(), 0);
    }

    #[test]
    fn a_use_of_the_leaf_moves_it_back_at_once() {
        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2]);
        tick();
        named(&mut ix, None, &[3, 4]);
        assert_eq!(leaves(&ix), vec![2, 4]);
        used(&mut ix, 2);
        assert_eq!(leaves(&ix), vec![4, 2]);
        let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
        assert_eq!(scan.leaf.map(|(l, _)| l), Some(h(4)));
        assert!(scan.behind.is_empty(), "both keys are current");
        check_leaves(&ix);
    }

    #[test]
    fn a_use_up_the_edge_is_found_when_its_leaf_reaches_the_front() {
        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2]);
        tick();
        named(&mut ix, None, &[3, 4]);
        used(&mut ix, 1);
        assert_eq!(leaves(&ix), vec![2, 4], "the key of 2 trails its edge");
        let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
        assert_eq!(
            scan.leaf.map(|(l, _)| l),
            Some(h(4)),
            "1-2 was used after 3-4 was written"
        );
        assert_eq!(scan.behind.len(), 1);
        assert_eq!(scan.behind[0].0, h(2));
        ix.raise(&scan);
        assert_eq!(leaves(&ix), vec![4, 2]);
        check_leaves(&ix);
        used(&mut ix, 3);
        assert_eq!(
            ix.oldest_leaf(WALK_BUDGET, 1, |_| false)
                .leaf
                .map(|(l, _)| l),
            Some(h(4)),
            "a scan of one edge takes the front leaf"
        );
        assert_eq!(oldest(&ix), Some(2));
        assert_eq!(
            ix.oldest_leaf(WALK_BUDGET, usize::MAX, |l| l == h(2))
                .leaf
                .map(|(l, _)| l),
            Some(h(4)),
            "skipped leaves are not taken"
        );
    }

    #[test]
    fn a_fork_or_a_removal_drops_a_raised_key_to_its_leaf() {
        //   1 ── 2 ── 3 ── 4        1 used last, then 9 forks off 2 and cuts
        //        └── 9              [3 4] off the use of 1
        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2, 3, 4]);
        tick();
        named(&mut ix, None, &[7]);
        used(&mut ix, 1);
        let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
        ix.raise(&scan);
        assert_eq!(leaves(&ix), vec![7, 4]);
        tick();
        named(&mut ix, Some(2), &[9]);
        assert_eq!(
            leaves(&ix),
            vec![4, 7, 9],
            "[3 4] was last used when written"
        );
        assert_eq!(oldest(&ix), Some(4));
        check_leaves(&ix);

        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2, 3, 4]);
        tick();
        named(&mut ix, None, &[7]);
        used(&mut ix, 1);
        let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
        ix.raise(&scan);
        ix.remove(h(2));
        assert_eq!(
            leaves(&ix),
            vec![4, 7, 1],
            "the use of 1 no longer reaches [3 4]"
        );
        check_leaves(&ix);
    }

    #[test]
    fn a_raise_found_before_a_cut_is_dropped() {
        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2, 3]);
        tick();
        named(&mut ix, None, &[7]);
        used(&mut ix, 1);
        let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
        assert_eq!(scan.behind.len(), 1);
        named(&mut ix, Some(1), &[8]);
        ix.raise(&scan);
        assert_eq!(key(&ix, 3), ix.nodes.get(&h(3)).and_then(|n| n.used_at));
        check_leaves(&ix);
    }

    #[test]
    fn cap_eviction_keeps_the_leaf_set_consistent() {
        let mut ix = ChainIndex::new(5);
        for c in 0..20u64 {
            named(&mut ix, None, &[c * 10, c * 10 + 1, c * 10 + 2]);
            check_leaves(&ix);
        }
        assert!(ix.len() <= 5);
        assert!(ix.leaves.len() <= 2);
        assert_eq!(leaves(&ix).last(), Some(&192), "the newest chain is kept");
    }

    #[test]
    fn dead_blocks_come_from_lost_parents_and_leave_when_found_again() {
        fn take(ix: &mut ChainIndex) -> Option<u64> {
            ix.take_dead(|_| Some(())).map(|(d, ())| d.hash.0)
        }

        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2, 3]);
        assert!(!ix.has_dead());
        ix.remove(h(1));
        assert_eq!(take(&mut ix), Some(2));
        assert!(
            ix.take_dead(|_| None::<()>).is_none(),
            "another worker holds it"
        );
        named(&mut ix, Some(1), &[5]);
        assert_eq!(take(&mut ix), Some(2), "2 first, then 5 under gone 1");
        ix.remove(h(2));
        assert_eq!(
            take(&mut ix),
            Some(3),
            "a dead block's removal passes it on"
        );
        named(&mut ix, None, &[1]);
        assert_eq!(take(&mut ix), Some(3), "2 is still gone");
        ix.remove(h(3));
        assert_eq!(take(&mut ix), None, "1 is back: 5 is reachable");
        assert!(
            !ix.has_dead(),
            "stale entries are dropped as they are found"
        );

        let mut ix = ChainIndex::new(100);
        named(&mut ix, None, &[1, 2]);
        ix.store(None, &[h(1)], Store::Backing);
        ix.remove(h(1));
        assert_eq!(take(&mut ix), None, "the backing tier still serves 1");
        ix.unback(h(1));
        assert_eq!(take(&mut ix), Some(2));
    }

    #[test]
    fn random_operations_keep_the_order_exact() {
        let mut rng = fastrand::Rng::with_seed(7);
        let mut ix = ChainIndex::new(10_000);
        for step in 0..3000 {
            let n = rng.u64(0..80);
            match rng.u8(0..10) {
                0..=3 => {
                    let parent = rng.bool().then(|| rng.u64(0..80));
                    let len = rng.u64(1..5);
                    let chain: Vec<u64> = (0..len).map(|i| (n + i * 7) % 80).collect();
                    if chain.iter().any(|c| Some(*c) == parent) {
                        continue;
                    }
                    let hashes: Vec<BlockHash> = chain.iter().copied().map(h).collect();
                    ix.store(parent.map(h), &hashes, Store::OnDisk);
                    for &c in &chain {
                        if c % 13 != 0 {
                            ix.set_digest(h(c), &c.to_be_bytes());
                        }
                    }
                }
                4..=5 => ix.store(None, &[h(n)], Store::Used),
                6..=7 => {
                    ix.remove(h(n));
                }
                8 => {
                    let scan = ix.oldest_leaf(WALK_BUDGET, usize::MAX, |_| false);
                    ix.raise(&scan);
                }
                _ => {
                    if rng.bool() {
                        ix.store(None, &[h(n)], Store::Backing);
                    } else {
                        ix.unback(h(n));
                    }
                }
            }
            check_leaves(&ix);
            let brute = ix
                .leaves
                .iter()
                .filter_map(|(_, l)| ix.edge_newest(*l, WALK_BUDGET))
                .min();
            let found = ix
                .oldest_leaf(WALK_BUDGET, usize::MAX, |_| false)
                .leaf
                .map(|(_, t)| t);
            assert_eq!(found, brute, "step {step}");
            let orphans: Vec<BlockHash> = ix
                .nodes
                .keys()
                .copied()
                .filter(|&b| ix.position(b) == Position::Orphan)
                .collect();
            for b in orphans {
                assert!(ix.dead.contains(&b), "step {step}: dead {b:?} not queued");
            }
        }
    }
}
