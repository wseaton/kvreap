//! Fixtures shared by the chain modules' tests.

use crate::chains::decode::{KvEvent, Locality};
use crate::layout::BlockHash;

pub(crate) fn h(n: u64) -> BlockHash {
    BlockHash(n)
}

pub(crate) fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

// Generated with msgspec from vLLM v0.31.0's kv_events.py structs: one
// BlockStored of [0x11, 0x12] (root), one of [0x13] with parent 0x12, a GPU
// BlockRemoved of 0x12, a STORAGE BlockRemoved of 0x13, AllBlocksCleared.
// Hashes are 32 bytes (0xaa * 24 + the u64).
pub(crate) const GOLDEN_V031: &str = "93cb41da39de00200000958aa474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657392c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000011c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f696473dc0020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1faa626c6f636b5f73697a6510a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c0a967726f75705f69647800b26b765f63616368655f737065635f6b696e64ae66756c6c5f617474656e74696f6e88a474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000013b1706172656e745f626c6f636b5f68617368c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012a9746f6b656e5f696473dc0010000102030405060708090a0b0c0d0e0faa626c6f636b5f73697a6510a76c6f72615f6964c0a66d656469756da3475055a96c6f72615f6e616d65c083a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000012a66d656469756da347505584a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000013a66d656469756da753544f52414745a967726f75705f6964780081a474797065b0416c6c426c6f636b73436c656172656400";
// Same, with array_like tagged events (vLLM <= 0.11) and int hashes.
pub(crate) const GOLDEN_LEGACY: &str = "93cb41da39de002000009297ab426c6f636b53746f7265649221222092010210c0a347505593ac426c6f636b52656d6f7665649122a753544f52414745c0";

// vLLM v0.31.0 structs as the fs tier publishes them with `locality` set:
// STORAGE BlockStored of 0x31 at LOCAL, of 0x31 at REMOTE, of 0x32 with no
// locality, and a REMOTE STORAGE BlockRemoved of 0x31.
pub(crate) const GOLDEN_V031_TIERED: &str = "93cb41da39de00200000948aa474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000031b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647390aa626c6f636b5f73697a6500a76c6f72615f6964c0a66d656469756da753544f52414745a96c6f72615f6e616d65c0a967726f75705f69647800a86c6f63616c697479a54c4f43414c8aa474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000031b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647390aa626c6f636b5f73697a6500a76c6f72615f6964c0a66d656469756da753544f52414745a96c6f72615f6e616d65c0a967726f75705f69647800a86c6f63616c697479a652454d4f544589a474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000032b1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647390aa626c6f636b5f73697a6500a76c6f72615f6964c0a66d656469756da753544f52414745a96c6f72615f6e616d65c0a967726f75705f6964780085a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391c420aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0000000000000031a66d656469756da753544f52414745a967726f75705f69647800a86c6f63616c697479a652454d4f5445c0";
// Same events with the structs array_like and int hashes.
pub(crate) const GOLDEN_TIERED_ARRAY: &str = "93cb41da39de00200000949fab426c6f636b53746f7265649131c09000c0a753544f52414745c0c000c0c0a54c4f43414cc0c09fab426c6f636b53746f7265649131c09000c0a753544f52414745c0c000c0c0a652454d4f5445c0c09fab426c6f636b53746f7265649132c09000c0a753544f52414745c0c000c0c0c0c0c096ac426c6f636b52656d6f7665649131a753544f5241474500a652454d4f5445c0c0";

pub(crate) fn stored(parent: Option<u64>, hashes: &[u64], medium: &str) -> KvEvent {
    KvEvent::Stored {
        parent: parent.map(BlockHash),
        hashes: hashes.iter().copied().map(BlockHash).collect(),
        digests: Vec::new(),
        medium: Some(medium.to_string()),
        locality: None,
    }
}

pub(crate) fn removed(hashes: &[u64], medium: &str) -> KvEvent {
    KvEvent::Removed {
        hashes: hashes.iter().copied().map(BlockHash).collect(),
        medium: Some(medium.to_string()),
        locality: None,
    }
}

pub(crate) fn at(mut e: KvEvent, tier: Locality) -> KvEvent {
    if let KvEvent::Stored { locality, .. } | KvEvent::Removed { locality, .. } = &mut e {
        *locality = Some(tier);
    }
    e
}

pub(crate) fn digest(n: u64) -> Vec<u8> {
    let mut d = vec![0xaa; 24];
    d.extend(n.to_be_bytes());
    d
}

pub(crate) fn with_digests(mut e: KvEvent) -> KvEvent {
    if let KvEvent::Stored {
        hashes, digests, ..
    } = &mut e
    {
        *digests = hashes.iter().map(|h| digest(h.0)).collect();
    }
    e
}

/// `[ts, [BlockStored(hashes, parent, ..., "STORAGE")], dp_rank]` in the
/// array-like encoding.
pub(crate) fn stored_batch(parent: Option<u64>, hashes: &[u64]) -> Vec<u8> {
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
