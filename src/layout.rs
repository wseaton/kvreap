//! The llmd_fs_backend on-disk layout:
//!
//! ```text
//! <cache>/<model>_<digest>/config.json            {"model_name": ...}
//! <cache>/<model>_<digest>_r<rank>/<hhh>/<hh>_g<group>/<hash:016x>.bin
//!         \_______ rank dir ______/ \bucket/ \_ leaf _/
//! ```

use std::fs;
use std::path::{Path, PathBuf};

const HEX_MODULO_BASE: u8 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockHash(pub u64);

impl BlockHash {
    pub fn from_file_name(name: &str) -> Option<Self> {
        let hex = name.strip_suffix(".bin")?;
        if hex.len() != 16 {
            return None;
        }
        u64::from_str_radix(hex, 16).ok().map(Self)
    }
}

/// The `[min, max]` range of `bucket % 16` values a worker owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    min: u8,
    max: u8,
}

impl Shard {
    /// Same split as the Python crawler's `get_hex_modulo_ranges`.
    pub fn split(workers: u8) -> Vec<Self> {
        let per = HEX_MODULO_BASE / workers.max(1);
        (0..workers)
            .map(|i| Self {
                min: i * per,
                max: i * per + per - 1,
            })
            .collect()
    }

    pub fn owns_bucket(self, name: &str, bucket_len: usize) -> bool {
        if name.len() != bucket_len {
            return false;
        }
        match u32::from_str_radix(name, 16) {
            Ok(v) => {
                let m = (v % u32::from(HEX_MODULO_BASE)) as u8;
                (self.min..=self.max).contains(&m)
            }
            Err(_) => false,
        }
    }
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}-{:x}", self.min, self.max)
    }
}

fn is_hex_name(name: &str) -> bool {
    (2..=4).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Directory holding `config.json` for a rank dir: `<name>_r<digits>` -> sibling `<name>`.
pub fn model_base_dir(rank_dir: &Path) -> Option<PathBuf> {
    let name = rank_dir.file_name()?.to_str()?;
    let (base, rank) = name.rsplit_once("_r")?;
    if base.is_empty() || rank.is_empty() || !rank.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(rank_dir.with_file_name(base))
}

/// Directories that directly contain hex buckets, found by walking down from
/// `cache` without following symlinks (matches the Python `_iter_rank_dirs`).
pub fn discover_rank_dirs(cache: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![cache.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let path = entry.path();
            if has_hex_child_dir(&path) {
                found.push(path);
            } else {
                stack.push(path);
            }
        }
    }
    found.sort();
    found
}

fn has_hex_child_dir(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.file_name().to_str().is_some_and(is_hex_name) && e.file_type().is_ok_and(|t| t.is_dir())
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::layout::{BlockHash, Shard, discover_rank_dirs, model_base_dir};

    #[test]
    fn block_hash_parses_16_hex_digit_bin_names() {
        assert_eq!(
            BlockHash::from_file_name("abcdef0123456789.bin"),
            Some(BlockHash(0xabcdef0123456789))
        );
        assert_eq!(
            BlockHash::from_file_name("ffffffffffffffff.bin"),
            Some(BlockHash(u64::MAX))
        );
        assert_eq!(
            BlockHash::from_file_name("0000000000000000.bin"),
            Some(BlockHash(0))
        );
    }

    #[test]
    fn block_hash_rejects_other_names() {
        for name in [
            "abcdef0123456789",
            "abcdef0123456789.txt",
            "abcdef.bin",
            "abcdef01234567890.bin",
            "ghijklmnopqrstuv.bin",
            "abcdef0123456789.bin_123.tmp",
        ] {
            assert_eq!(BlockHash::from_file_name(name), None, "{name}");
        }
    }

    #[test]
    fn shard_split_matches_python_ranges() {
        let eight = Shard::split(8);
        assert_eq!(eight.len(), 8);
        assert_eq!(eight[0].to_string(), "0-1");
        assert_eq!(eight[7].to_string(), "e-f");
        assert_eq!(Shard::split(1)[0].to_string(), "0-f");
        let sixteen = Shard::split(16);
        assert!(
            sixteen
                .iter()
                .enumerate()
                .all(|(i, s)| s.to_string() == format!("{i:x}-{i:x}"))
        );
    }

    #[test]
    fn shard_ownership_uses_bucket_mod_16() {
        let s = Shard::split(8)[6]; // c-d
        assert!(s.owns_bucket("abc", 3)); // 0xabc % 16 = 12
        assert!(s.owns_bucket("00d", 3));
        assert!(!s.owns_bucket("def", 3)); // 15
        assert!(!s.owns_bucket("abc", 4));
        assert!(!s.owns_bucket("xyz", 3));
    }

    #[test]
    fn every_bucket_owned_by_exactly_one_shard() {
        let shards = Shard::split(4);
        for v in 0u32..4096 {
            let name = format!("{v:03x}");
            assert_eq!(
                shards.iter().filter(|s| s.owns_bucket(&name, 3)).count(),
                1,
                "{name}"
            );
        }
    }

    #[test]
    fn model_base_dir_strips_rank_suffix() {
        assert_eq!(
            model_base_dir(Path::new("/c/meta-llama_Llama-3.1-8B_fedcba987654_r0")),
            Some(Path::new("/c/meta-llama_Llama-3.1-8B_fedcba987654").to_path_buf())
        );
        assert_eq!(
            model_base_dir(Path::new("/c/my_r2d2_model_abc_r12")),
            Some(Path::new("/c/my_r2d2_model_abc").to_path_buf())
        );
        assert_eq!(model_base_dir(Path::new("/c/model_abc")), None);
        assert_eq!(model_base_dir(Path::new("/c/model_rx")), None);
        assert_eq!(model_base_dir(Path::new("/c/_r0")), None);
    }

    #[test]
    fn discovers_flat_and_nested_rank_dirs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path();
        fs::create_dir_all(cache.join("model_abc_r0/abc/de_g0")).expect("mkdir");
        fs::create_dir_all(cache.join("model_abc_r1/000/00_g0")).expect("mkdir");
        fs::create_dir_all(cache.join("extra/nested/model_def_r0/fff/ff_g0")).expect("mkdir");
        fs::create_dir_all(cache.join("model_abc")).expect("mkdir");
        fs::write(cache.join("model_abc/config.json"), "{}").expect("write");

        let found = discover_rank_dirs(cache);
        let rel: Vec<_> = found
            .iter()
            .map(|p| {
                p.strip_prefix(cache)
                    .expect("under cache")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(
            rel,
            vec!["extra/nested/model_def_r0", "model_abc_r0", "model_abc_r1"]
        );
    }

    #[test]
    fn discovery_ignores_files_named_like_buckets() {
        let tmp = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(tmp.path().join("model_abc_r0")).expect("mkdir");
        fs::write(tmp.path().join("model_abc_r0/abc"), "not a dir").expect("write");
        assert!(discover_rank_dirs(tmp.path()).is_empty());
    }

    #[test]
    fn discovery_of_missing_cache_is_empty() {
        let tmp = tempfile::tempdir().expect("tempdir");
        assert!(discover_rank_dirs(&tmp.path().join("missing")).is_empty());
    }
}
