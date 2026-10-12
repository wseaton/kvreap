//! The pool of eviction candidates a worker has sampled.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use crate::layout::BlockHash;
use crate::worker::buckets::RankDir;

#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub(crate) path: PathBuf,
    pub(crate) rank: Arc<RankDir>,
    pub(crate) hash: BlockHash,
    pub(crate) size: u64,
    /// When the block was last useful: its atime, or with the chain index
    /// the newest write or use on its leaf edge. The pool evicts the earliest first.
    pub(crate) last_used: SystemTime,
    pub(crate) sampled_at: Instant,
}

/// Candidates ordered by `last_used`, capped at `cap` by dropping the latest.
#[derive(Debug)]
pub(crate) struct Pool {
    pub(crate) cap: usize,
    pub(crate) seq: u64,
    pub(crate) by_age: BTreeMap<(SystemTime, u64), Candidate>,
    pub(crate) keys: HashMap<PathBuf, (SystemTime, u64)>,
}

impl Pool {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            seq: 0,
            by_age: BTreeMap::new(),
            keys: HashMap::new(),
        }
    }

    /// Inserts or replaces `c`.
    pub(crate) fn insert(&mut self, c: Candidate) {
        if let Some(old) = self.keys.remove(&c.path) {
            self.by_age.remove(&old);
        }
        self.seq += 1;
        let key = (c.last_used, self.seq);
        self.keys.insert(c.path.clone(), key);
        self.by_age.insert(key, c);
        while self.by_age.len() > self.cap {
            if let Some((_, youngest)) = self.by_age.pop_last() {
                self.keys.remove(&youngest.path);
            }
        }
    }

    pub(crate) fn pop_oldest(&mut self) -> Option<Candidate> {
        let (_, c) = self.by_age.pop_first()?;
        self.keys.remove(&c.path);
        Some(c)
    }

    pub(crate) fn clear(&mut self) {
        self.by_age.clear();
        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime};

    use crate::layout::BlockHash;
    use crate::worker::buckets::RankDir;
    use crate::worker::pool::{Candidate, Pool};

    fn candidate(path: &str, age_secs: u64) -> Candidate {
        Candidate {
            path: PathBuf::from(path),
            rank: Arc::new(RankDir {
                path: PathBuf::from("/r"),
                model_base: None,
            }),
            hash: BlockHash(0),
            size: 1,
            last_used: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000 - age_secs),
            sampled_at: Instant::now(),
        }
    }

    #[test]
    fn pool_pops_oldest_first_and_drops_youngest_over_cap() {
        let mut pool = Pool::new(3);
        pool.insert(candidate("/a", 10));
        pool.insert(candidate("/b", 50));
        pool.insert(candidate("/c", 30));
        pool.insert(candidate("/d", 40));
        assert_eq!(pool.by_age.len(), 3);
        let order: Vec<_> = std::iter::from_fn(|| pool.pop_oldest())
            .map(|c| c.path)
            .collect();
        assert_eq!(
            order,
            vec![
                PathBuf::from("/b"),
                PathBuf::from("/d"),
                PathBuf::from("/c")
            ]
        );
    }

    #[test]
    fn pool_reinsert_replaces_existing_entry() {
        let mut pool = Pool::new(10);
        pool.insert(candidate("/a", 10));
        pool.insert(candidate("/b", 20));
        pool.insert(candidate("/a", 99));
        assert_eq!(pool.by_age.len(), 2);
        assert_eq!(pool.pop_oldest().map(|c| c.path), Some(PathBuf::from("/a")));
    }
}
