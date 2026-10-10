//! Decoder for vLLM's KV cache event batches.
//!
//! Wire format (vLLM `ZmqEventPublisher`): frames `[topic, seq u64 BE, payload]`,
//! payload = msgpack `EventBatch` `[ts, [event, ...], dp_rank?]`. Events are
//! msgspec tagged structs: maps keyed by field name with `"type"` as the tag
//! (vLLM >= 0.12), or arrays with the tag first (older releases).

use crate::layout::BlockHash;

const MAX_DEPTH: usize = 16;
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

#[cfg(test)]
mod tests {
    use crate::chains::decode::{DecodeError, KvEvent, decode_batch};
    use crate::chains::testing::{
        GOLDEN_LEGACY, GOLDEN_V031, removed, stored, unhex, with_digests,
    };

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
}
