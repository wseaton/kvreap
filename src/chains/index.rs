//! The `block -> parent` tree of blocks vLLM has announced, with which of them
//! are on disk.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use crate::layout::BlockHash;

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
    /// parent is gone is.
    Gone,
}

/// How a store event marks its blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    /// The blocks were written to the tier kvreap evicts.
    OnDisk,
    /// The blocks live elsewhere (GPU, CPU); only their parent links count.
    LinksOnly,
}

#[derive(Debug, Clone)]
struct Node {
    parent: Option<BlockHash>,
    /// Every child ever linked; filter by residence when walking.
    children: Vec<BlockHash>,
    children_on_disk: u32,
    residence: Residence,
    generation: u64,
    /// Full block hash, which names the block's file.
    digest: Option<Arc<[u8]>>,
    /// When the block was last marked on disk.
    stored_at: Option<Instant>,
}

/// A radix-tree leaf edge: the on-disk blocks from just below the last fork
/// (or a chain root, or a missing parent) down to a leaf, root side first.
/// Only a leaf edge serves a single cached prefix, so it is the unit of
/// connectivity-based eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEdge {
    pub blocks: Vec<Descendant>,
    /// Most recent on-disk mark of any block in the edge.
    pub newest_store: Instant,
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
            residence: Residence::Unknown,
            generation,
            digest: None,
            stored_at: None,
        })
    }

    fn child_stored(&mut self, parent: Option<BlockHash>) {
        if let Some(p) = parent {
            let n = self.node_mut(p);
            n.children_on_disk = n.children_on_disk.saturating_add(1);
        }
    }

    fn child_removed(&mut self, parent: Option<BlockHash>) {
        let Some(p) = parent else {
            return;
        };
        if let Some(n) = self.nodes.get_mut(&p) {
            n.children_on_disk = n.children_on_disk.saturating_sub(1);
            if n.residence != Residence::OnDisk && n.children_on_disk == 0 && n.parent.is_none() {
                self.nodes.remove(&p);
            }
        }
    }

    /// Records `hashes` as a chain hanging off `parent`.
    pub fn store(&mut self, parent: Option<BlockHash>, hashes: &[BlockHash], store: Store) {
        let mut parent = parent;
        for &hash in hashes {
            self.store_one(hash, parent, store);
            parent = Some(hash);
        }
        self.enforce_cap();
    }

    fn store_one(&mut self, hash: BlockHash, parent: Option<BlockHash>, store: Store) {
        let n = self.node_mut(hash);
        let was_on_disk = n.residence == Residence::OnDisk;
        let old_parent = n.parent;
        let parent = old_parent.or(parent);
        n.parent = parent;
        if store == Store::OnDisk && !was_on_disk {
            n.residence = Residence::OnDisk;
            n.stored_at = Some(Instant::now());
        }
        let now_on_disk = n.residence == Residence::OnDisk;
        let gained_parent = old_parent.is_none() && parent.is_some();
        if let (true, Some(p)) = (gained_parent, parent) {
            let siblings = &mut self.node_mut(p).children;
            if !siblings.contains(&hash) {
                siblings.push(hash);
            }
        }
        let counted = was_on_disk && !gained_parent;
        if now_on_disk && !counted {
            self.child_stored(parent);
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
                .min_by_key(|c| self.nodes.get(c).and_then(|n| n.stored_at));
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

    /// The newest on-disk mark on the leaf edge `hash` maps to, without
    /// building the edge.
    pub fn edge_newest(&self, hash: BlockHash, budget: usize) -> Option<Instant> {
        let mut newest = None;
        self.walk_edge(hash, budget, |b| {
            newest = newest.max(self.nodes.get(&b).and_then(|n| n.stored_at));
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
        let mut newest_store = None;
        self.walk_edge(hash, budget, |b| {
            newest_store = newest_store.max(self.nodes.get(&b).and_then(|n| n.stored_at));
            blocks.push(self.descendant(b));
        })?;
        blocks.reverse();
        Some(LeafEdge {
            blocks,
            newest_store: newest_store?,
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
        if node.residence != Residence::OnDisk {
            return position;
        }
        node.residence = Residence::Gone;
        let (parent, children) = (node.parent, node.children_on_disk);
        if children == 0 {
            self.nodes.remove(&hash);
        }
        self.child_removed(parent);
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

    fn gone(&self, hash: BlockHash) -> bool {
        self.residence(hash) == Residence::Gone
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
            Some(p) if self.gone(p) => Position::Orphan,
            Some(p) if self.on_disk(p) && node.children_on_disk > 0 => Position::Internal,
            Some(p) if self.on_disk(p) => Position::Leaf,
            _ => Position::Root,
        }
    }

    /// On disk with a parent kvreap saw on disk and then removed:
    /// unreachable by a prefix lookup.
    pub fn is_dead(&self, hash: BlockHash) -> bool {
        self.on_disk(hash)
            && self
                .nodes
                .get(&hash)
                .and_then(|n| n.parent)
                .is_some_and(|p| self.gone(p))
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
                && node.residence == Residence::OnDisk
            {
                self.child_removed(node.parent);
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
    use std::sync::Arc;
    use std::time::Duration;

    use crate::chains::index::{ChainIndex, LeafEdge, Position, Store};
    use crate::chains::testing::h;
    use crate::layout::BlockHash;

    fn children(ix: &ChainIndex, hash: BlockHash) -> u32 {
        ix.nodes.get(&hash).map_or(0, |n| n.children_on_disk)
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
        assert!(ix.is_dead(h(2)));
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
        assert!(ix.is_dead(h(2)));

        let mut ix = ChainIndex::new(100);
        ix.store(Some(h(99)), &[h(10), h(11)], Store::OnDisk);
        assert!(!ix.is_dead(h(10)), "parent unknown to kvreap");
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
            Some(edge.newest_store),
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
                ix.leaf_edge(h(n), 100).map(|e| e.newest_store),
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
}
