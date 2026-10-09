# kvreap

Drop-in replacement image for the llm-d `pvc_evictor`
(`kv_connectors/pvc_evictor` in llm-d-kv-cache). It reads the same environment
variables and works with the existing Helm chart unchanged:

```bash
helm upgrade pvc-evictor ./helm --reuse-values \
  --set image.repository=quay.io/wseaton/kvreap --set image.tag=<tag>
```

## What differs

The Python evictor finds candidates by crawling the whole cache tree. On
ReadWriteMany storage (CephFS, NFS, ...) every `readdir`/`stat`/`unlink` is a
metadata operation that competes with vLLM's own lookups and writes. kvreap:

- **Does nothing while usage is below `CLEANUP_THRESHOLD`** beyond `statvfs`.
- **Samples instead of crawling.** Block files are hash-distributed, so a
  random `<rank>/<hhh>/` bucket is a uniform sample. Each round `statx`es one
  bucket into a pool of the oldest cold files and unlinks the oldest half as
  many as it sampled. Cost scales with bytes to free, and eviction is
  approximately LRU instead of crawl order.
- **Paces every metadata op** through one budget: at most
  `2 x DELETION_MAX_FILES_PER_SECOND` ops/s (0 = no cap), reduced further by
  AIMD when observed `readdir`/`statx`/`unlink` latency rises 3x above its
  rolling baseline. At 97% usage pacing is dropped.
- **Uses `statx(AT_STATX_DONT_SYNC)`** relative to directory fds, so stats can
  be answered from cached attributes instead of forcing cap recalls or flushes
  on a concurrent writer.
- One process, one thread per shard (`NUM_CRAWLER_PROCESSES`), no `xargs`.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `PVC_MOUNT_PATH` | `/kv-cache` | |
| `CACHE_DIRECTORY` | `kv/model-cache/models` | relative to the mount |
| `CLEANUP_THRESHOLD` / `TARGET_THRESHOLD` | `85` / `70` | start / stop eviction (% used) |
| `NUM_CRAWLER_PROCESSES` | `8` | worker threads; 1, 2, 4, 8 or 16 |
| `LOGGER_INTERVAL_SECONDS` | `0.5` | `statvfs` poll interval |
| `DELETION_MAX_FILES_PER_SECOND` | `0` | 0 = no cap (AIMD still applies) |
| `FILE_ACCESS_TIME_THRESHOLD_MINUTES` | `60` | never delete files accessed more recently |
| `DELETION_BATCH_SIZE` | `100` | `BlockRemoved` events per message |
| `ENABLE_DIR_CLEANUP` / `DIR_CLEANUP_TTL_SECONDS` | `true` / `120` | rmdir empty buckets older than the TTL |
| `HEX_BUCKET_LEN` | `3` | |
| `STORAGE_EVENTS_ENDPOINT` | unset | ZMQ PUB bind address for `BlockRemoved` events; needs the `events` build, otherwise logged and ignored |
| `DRY_RUN`, `LOG_LEVEL`, `LOG_FILE_PATH` | | as in the Python evictor |
| `FILE_QUEUE_MAXSIZE`, `FILE_QUEUE_MIN_SIZE` | | accepted and ignored |

## `BlockRemoved` events

Events are behind the `events` Cargo feature and are off by default, so the
default build links no cryptographic code: libzmq compiles in its own SHA-1
for the WebSocket transport. A test scans the default binary for crypto and
zmq symbols.

```bash
cargo build --release --features=events
```

Against vLLM 0.31's built-in FS tier the events have no effect: vLLM
publishes storage-tier stores under the vLLM pod's identity with medium
`STORAGE`, while evictor removals go to the `SHARED_STORAGE` topic, which
only the old `llmd_fs_backend` (<= 0.23) used for its stores.

## Filesystems

See [docs/filesystems.md](docs/filesystems.md) for what kvreap assumes from
the filesystem, VAST and local NVMe behavior measured on CoreWeave, and
planned portability changes. Benchmark results are in
[benchmarks/](benchmarks/README.md).

## Development

```bash
cargo test
```

The container build runs the test suite on Linux, which exercises the `statx` path.
