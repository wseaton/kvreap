# kvreap

**Status: tech preview.**

Evictor for vLLM's filesystem KV-cache tier (the llm-d `pvc_evictor` and
vLLM's fs offload tier). When the cache volume passes `CLEANUP_THRESHOLD`,
kvreap deletes block files until usage is back under `TARGET_THRESHOLD`.

It is a drop-in replacement image for the llm-d `pvc_evictor`
(`kv_connectors/pvc_evictor` in llm-d-kv-cache): it reads the same
environment variables and works with the existing Helm chart unchanged:

```bash
helm upgrade pvc-evictor ./helm --reuse-values \
  --set image.repository=quay.io/wseaton/kvreap --set image.tag=<tag>
```

Pointed at vLLM's KV cache events, it evicts by prefix chain instead of by
file age, which is where most of the gain comes from.

## Results

Qwen2.5-72B, TP=4, 40-minute agentic multi-turn load, fs tier at 85% / 70%
thresholds. Full reports: [VAST](benchmarks/final-vast.md),
[node-local NVMe](benchmarks/final-nvme.md).

| shared VAST PVC | Python `pvc_evictor` | kvreap, chain-aware |
|---|---|---|
| requests completed | 2,748 | **4,345** (+58%) |
| prefix tokens served from offload | 35.5M | **70.8M** (2.0×) |
| returning-turn TTFT p90 / p99 | 6.3 s / 18.1 s | **0.94 s / 3.2 s** |
| KV rewritten to storage | 2.19 TB | **0.90 TB** |
| evictor NFS metadata ops/s | 14,765 | **704** |

On node-local NVMe through a hostPath the Python evictor never runs at all:
`statvfs` reports the whole node disk, so usage never reaches the threshold
([proof](benchmarks/hostpath-proof/)). kvreap sizes the budget from
`CAPACITY_BYTES`; there, chain-aware eviction completes 27% more requests than
kvreap's own plain LRU and halves returning-turn TTFT p99.

## How it works

**Sampling instead of crawling.** Block files are hash-distributed, so a
random `<rank>/<hhh>/` bucket is a uniform sample. Each round `statx`es one
bucket into a pool of the oldest cold files and evicts half as many files as
it sampled. Cost scales with the bytes to free, not with the size of the
tree. While usage is below `CLEANUP_THRESHOLD` kvreap only calls `statvfs`
(or, with `CAPACITY_BYTES`, samples ten buckets per second).

**Chain-aware eviction.** A prefix-cache lookup stops at the first missing
block, so deleting block *k* of a conversation makes every later block
useless, and the next turn recomputes and rewrites all of it. With
`KV_EVENTS_ENDPOINTS` set, kvreap subscribes to vLLM's `BlockStored` /
`BlockRemoved` events and keeps the `block -> parent` tree in memory. A
sampled block then stands for the radix-tree leaf edge it belongs to: the
unshared tail from the last fork down to a leaf.

```text
  system ── s1 ──┬── a1 ── a2 ── a3      session A, last written 20 min ago
                 └── b1 ── b2            session B, written 5 s ago

  evict [a1 a2 a3] whole, leaf first, before [b1 b2];
  never s1 while it has two continuations on disk
```

Edges go least recently written first, regardless of atime: a live session's
first turn is among the oldest files on disk but sorts by its latest turn.
Edges written within the hot threshold wait while usage is below the pressure
line, halfway from `CLEANUP_THRESHOLD` to the emergency band (91% with the
defaults), so the sampler looks for finished sessions first; above it,
eviction is plain LRU over edges.
Blocks whose parent is already gone
are unreachable and go first, with everything below them. The subscriber
follows the llm-d router's protocol: it replays from vLLM's `replay_endpoint`
on connect, on a sequence gap and on a publisher restart, so a dropped
connection does not leave the index stale. Batches that fall out of vLLM's
replay buffer are counted (`kvreap_kv_events_lost`). Block files the index
does not know (written before kvreap started, or in a lost batch) are evicted
before any known edge.

**Pacing.** Every `readdir`, `statx` and `unlink` of sampling and deletion
goes through one budget:
at most `2 × DELETION_MAX_FILES_PER_SECOND` ops/s (0 = no cap), cut by AIMD
when observed latency rises 3× above its rolling baseline. Pacing is dropped
in the emergency band. Unlinks for one edge run up to `DELETE_FANOUT` at a
time, since on NFS throughput is bound by round trips, not by the server.

**Filesystem hygiene.** `statx(AT_STATX_DONT_SYNC)` relative to directory fds,
so stats are served from cached attributes rather than forcing cap recalls
on a concurrent writer. At startup a probe file checks whether atime moves;
on `noatime` mounts (and vLLM 0.31 never refreshes atime on cache hits)
"hot" means recently written.

## Deploying

- **Helm, existing `pvc_evictor` chart:** swap the image as above. Add
  `KV_EVENTS_ENDPOINTS` (and `KV_EVENTS_DISK_MEDIUM=STORAGE` for vLLM's fs
  tier) through the chart's extra env to turn on chain-aware eviction.
- **KServe `LLMInferenceService`:** kustomize components in
  [deploy/kserve/](deploy/kserve/README.md), including a preset that
  enables vLLM's KV events and replay endpoint.
- **Node-local NVMe (hostPath):** run kvreap on vLLM's node with
  `CAPACITY_BYTES` set to the cache budget. See
  [docs/filesystems.md](docs/filesystems.md).

vLLM must publish digest block hashes
(`VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES=0`), because the file names are the
digests; `kvreap_blocks_undigested` counts blocks it could not name.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `PVC_MOUNT_PATH` | `/kv-cache` | |
| `CACHE_DIRECTORY` | `kv/model-cache/models` | relative to the mount |
| `CLEANUP_THRESHOLD` / `TARGET_THRESHOLD` | `85` / `70` | start / stop eviction (% used) |
| `NUM_CRAWLER_PROCESSES` | `8` | worker threads; 1, 2, 4, 8 or 16 |
| `LOGGER_INTERVAL_SECONDS` | `0.5` | usage poll interval |
| `DELETION_MAX_FILES_PER_SECOND` | `0` | 0 = no cap (AIMD still applies) |
| `DELETE_FANOUT` | `16` | unlinks in flight per worker while deleting an edge or subtree |
| `FILE_ACCESS_TIME_THRESHOLD_MINUTES` | `60` | files accessed (on `noatime` mounts, written) more recently are never sampled for eviction |
| `ENABLE_DIR_CLEANUP` / `DIR_CLEANUP_TTL_SECONDS` | `true` / `120` | remove leaf and bucket dirs that sampling finds empty and unchanged for the TTL |
| `HEX_BUCKET_LEN` | `3` | |
| `CAPACITY_BYTES` | unset | volume size in bytes. When set, used bytes are estimated from bucket samples instead of `statvfs`, for volumes whose `statvfs` reports another filesystem (hostPath on a shared node disk). Warns at startup if `statvfs` reports more than 10× this |
| `KV_EVENTS_ENDPOINTS` | unset | comma-separated vLLM `--kv-events-config` PUB endpoints; a hostname resolving to several addresses (a headless Service) subscribes to each pod. Enables chain-aware eviction |
| `KV_EVENTS_REPLAY_PORT` | unset | port of each publisher's `replay_endpoint`; without it gaps are counted but not repaired |
| `KV_EVENTS_DISK_MEDIUM` | unset | only `BlockStored` events of this medium mark a block on disk; `STORAGE` for vLLM's fs tier |
| `METRICS_ADDR` | unset | `host:port` to serve Prometheus metrics on `/metrics` |
| `DRY_RUN`, `LOG_LEVEL`, `LOG_FILE_PATH` | | as in the Python evictor |
| `FILE_QUEUE_MAXSIZE`, `FILE_QUEUE_MIN_SIZE`, `STORAGE_EVENTS_ENDPOINT`, `DELETION_BATCH_SIZE` | | accepted and ignored (logged at startup) |

## Metrics

With `METRICS_ADDR` set, kvreap serves Prometheus text on `/metrics` over
plain HTTP from the standard library (no HTTP or TLS dependency). It covers
usage and mode, bytes freed, per-op metadata latency histograms, the current
op-rate limit, prune durations, and, with KV events, index size, event
gaps, replays, lost batches, and deletions by chain position
(`kvreap_chain_deletions{position="internal"}` counts deletions that cut a
live chain). The same counters go to the log every status interval.

## Security

- KV events carry prompt token ids. kvreap implements no ZMQ authentication
  or encryption; keep the events port reachable only by kvreap and the
  router, through the namespace boundary or a NetworkPolicy
  ([deploy/kserve/](deploy/kserve/README.md) has both cases).
- kvreap links no cryptographic code, so it passes FIPS `check-payload`
  scans; a test scans the binary for crypto and libzmq symbols. ZMQ is the
  pure-Rust `zeromq` crate.
- kvreap needs write access to the cache directories vLLM creates: the same
  uid as vLLM or a shared group.

## Known limitations

- `CAPACITY_BYTES` estimates usage from bucket samples. In the NVMe run the
  estimate trailed the disk (buckets were listed every 60 s and sampled once
  a second, now every 5 s and ten a second) and the true footprint peaked at
  111% of the budget. Not yet re-measured; leave headroom between the budget
  and the disk.
- kvreap does not publish `BlockRemoved` events for the files it deletes.
  The Python evictor's events go to the `SHARED_STORAGE` topic, which only
  the old `llmd_fs_backend` (<= 0.23) used; vLLM 0.31's fs tier publishes
  under its own pod identity, so nothing consumes them.
- One cache tier per kvreap. Tiered NVMe + shared storage needs one kvreap
  per tier.

## Development

```bash
cargo test                 # unit and process-level e2e tests
tests/kind/e2e.sh          # kind cluster: fake vLLM, toxiproxy cut, KServe overlay
```

The container build runs the test suite on Linux, which exercises the
`statx` path. Benchmarks and the harness that produced them are in
[benchmarks/](benchmarks/README.md) and [bench/](bench/).
