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

- **Does nothing while usage is below `CLEANUP_THRESHOLD`** beyond `statvfs`
  (or, with `CAPACITY_BYTES`, sampling one bucket per second).
- **Samples instead of crawling.** Block files are hash-distributed, so a
  random `<rank>/<hhh>/` bucket is a uniform sample. Each round `statx`es one
  bucket into a pool of the oldest cold files and unlinks the oldest half as
  many as it sampled. Cost scales with bytes to free, and eviction is
  approximately LRU instead of crawl order.
- **Paces every metadata op** through one budget: at most
  `2 x DELETION_MAX_FILES_PER_SECOND` ops/s (0 = no cap), reduced further by
  AIMD when observed `readdir`/`statx`/`unlink` latency rises 3x above its
  rolling baseline. At `EMERGENCY_THRESHOLD` (default 97%, or
  `CLEANUP_THRESHOLD` if higher) pacing is dropped.
- **Paces prunes to need.** A prune targets the delete rate that brings usage
  from where it started down to `TARGET_THRESHOLD` in about 60 s (bytes over
  target / mean sampled file size, converted to ops with the measured ops per
  delete, at least 20 ops/s), within the cap and AIMD above. AIMD only grows
  while it is the binding limit.
- **Uses `statx(AT_STATX_DONT_SYNC)`** relative to directory fds, so stats can
  be answered from cached attributes instead of forcing cap recalls or flushes
  on a concurrent writer.
- **Checks atime at startup.** A probe file is read with a back-dated atime;
  if atime doesn't move, kvreap warns that "hot" means recently written.
- One process, one thread per shard (`NUM_CRAWLER_PROCESSES`), no `xargs`.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `PVC_MOUNT_PATH` | `/kv-cache` | |
| `CACHE_DIRECTORY` | `kv/model-cache/models` | relative to the mount |
| `CLEANUP_THRESHOLD` / `TARGET_THRESHOLD` | `85` / `70` | start / stop eviction (% used) |
| `EMERGENCY_THRESHOLD` | `max(97, CLEANUP_THRESHOLD)` | % used at which deletion stops being paced; must not be below `CLEANUP_THRESHOLD` |
| `NUM_CRAWLER_PROCESSES` | `8` | worker threads; 1, 2, 4, 8 or 16 |
| `LOGGER_INTERVAL_SECONDS` | `0.5` | `statvfs` poll interval |
| `DELETION_MAX_FILES_PER_SECOND` | `0` | 0 = no cap (AIMD still applies) |
| `FILE_ACCESS_TIME_THRESHOLD_MINUTES` | `60` | never delete files accessed more recently |
| `DELETION_BATCH_SIZE` | `100` | `BlockRemoved` events per message |
| `ENABLE_DIR_CLEANUP` / `DIR_CLEANUP_TTL_SECONDS` | `true` / `120` | rmdir leaf and bucket dirs that sampling finds empty and unchanged for the TTL; a leaf is never removed right after its last file |
| `HEX_BUCKET_LEN` | `3` | |
| `CAPACITY_BYTES` | unset | volume size in bytes (e.g. the PVC request). When set, used bytes are estimated from bucket samples (mean block bytes per bucket x bucket count) instead of `statvfs`; for volumes whose `statvfs` reports the wrong filesystem. Warns at startup if `statvfs` reports more than 10x this |
| `STORAGE_EVENTS_ENDPOINT` | unset | ZMQ PUB bind address for `BlockRemoved` events; needs the `events` build, otherwise logged and ignored |
| `HEALTH_DIR` | `/tmp/kvreap` | local writable dir for the `ready` and `alive` sentinel files; never the PVC |
| `HEALTH_MAX_AGE_SECONDS` | `30` | `healthcheck --live` fails when `alive` is older than this |
| `DRY_RUN`, `LOG_LEVEL`, `LOG_FILE_PATH` | | as in the Python evictor |
| `FILE_QUEUE_MAXSIZE`, `FILE_QUEUE_MIN_SIZE` | | accepted and ignored |

## Health probes

kvreap creates `HEALTH_DIR/ready` once the mount is found, the workers are
running and the first `statvfs` succeeded, and removes it on SIGTERM so the
pod goes unready while draining. The controller touches `HEALTH_DIR/alive`
after every successful `statvfs`, so a controller stuck on a hung hard NFS
mount lets it go stale. The probe command only stats those two files and
ignores the rest of the configuration:

```yaml
livenessProbe:
  exec:
    command: ["kvreap", "healthcheck", "--live"]   # optional: --max-age SECONDS
readinessProbe:
  exec:
    command: ["kvreap", "healthcheck", "--ready"]   # same as: ["cat", "/tmp/kvreap/ready"]
```

Readiness is plain file existence, so `cat HEALTH_DIR/ready` works too.
Liveness needs the age check: a controller stuck on a dead mount cannot
remove its own file, so `alive` is judged by its mtime.

It exits 0 when healthy, 1 with a one-line reason on stderr when not, and 2
on bad arguments. The image keeps `python3`, since the current pvc-evictor
chart's probes still run it; switching probes is a chart setting.

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
