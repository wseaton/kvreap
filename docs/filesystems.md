# Filesystem behavior and portability

What kvreap depends on from the filesystem, what we measured on CoreWeave
VAST and local NVMe, and what to change so it behaves well on any POSIX
filesystem.

Each item is marked **measured** (observed on coreweave-waldorf, 2026-10-09),
**inferred** (follows from a measurement or documented kernel behavior, not
directly tested), **proposed** (a change to kvreap) or **implemented** (a
proposed change that has landed).

## What kvreap assumes

| Assumption | Used for | Breaks when |
|---|---|---|
| `statvfs` on the mount reports the volume's own capacity and usage | cleanup/target thresholds | the volume is a directory on a shared filesystem (hostPath, emptyDir, NFS exports without quota reporting) |
| atime moves when vLLM reads a block | "hot" protection, LRU order | `noatime`, `relatime` after the first read, network filesystems that update atime lazily or not at all |
| block files are `<hhh>/<hh>_g<n>/<hex>.bin` under `_r<rank>` dirs | sampling | the writer changes its layout |
| `unlink` of a file being read fails the reader cleanly | correctness | never: vLLM checks existence and treats a failed read as a miss |

## CoreWeave VAST (`shared-vast`)

Mount options as set by the StorageClass and the CSI driver:
`vers=3, nconnect=32, lookupcache=pos, noacl, noatime, hard, timeo=600,
retrans=2, forcerdirplus, acregmin=3, acregmax=60, acdirmin=30, acdirmax=60`.

### Capacity and quota

- **`statvfs` reports the whole cluster until the PVC is first written** (measured).
  A fresh 20 GiB PVC showed `17P size, 9.9P used, 60%`. After the first
  writes it reported the 20 GiB quota. An evictor that starts before vLLM
  writes anything sees the cluster-wide percentage; if that is above
  `CLEANUP_THRESHOLD` it starts evicting an empty volume (harmless, but it
  sends the wrong signal in logs and metrics).
- **Usage lags writes by about 5 seconds** (measured): `statvfs` used bytes
  kept reporting 100% for one sample after 23 GiB was deleted, then 0%.
  Thresholds react late, so leave headroom between the cleanup threshold and
  100%.
- **The quota is soft** (measured): 23 GiB was written into a 20 GiB PVC before
  writes failed. Overshoot past 100% of the requested size is possible.
- **At the limit writes fail with `EDQUOT`** ("Disk quota exceeded"), not
  `ENOSPC` (measured). vLLM logs and drops the store; alerting should match
  both errors.
- **`reclaimPolicy: Retain`** on `shared-vast`: deleting a PVC leaves the PV
  and its data on VAST. Old KV caches accumulate unless PVs are cleaned up.

### Metadata and caching

- **All VAST mounts on a node share one NFS superblock** (measured). A new
  pod's `/proc/self/mountstats` showed counters 44 hours old and millions of
  ops from other pods. Two consequences:
  - per-pod NFS counters need the `nosharecache` mount option (kvbench uses a
    StorageClass clone with it);
  - an evictor scheduled on the same node as vLLM shares vLLM's NFS client,
    its 32 connections and its RPC slots (inferred). Keep the evictor off vLLM
    nodes (pod anti-affinity) or give its mount `nosharecache`.
- **`lookupcache=pos` caches positive lookups for up to `acdirmax` (60 s)** and
  never caches negative ones. vLLM checks block existence with `stat`, so a
  block the evictor just unlinked can still look present to vLLM until the
  cached entry expires, and the read then fails with `ENOENT` (inferred). We
  measured 174 / 139 / 64 such failures in 20 minutes for variants a / b / c,
  every one an `ENOENT` in vLLM's block I/O thread. Evicting oldest-first
  makes it rarer because old blocks are less likely to be read.
- **`forcerdirplus`**: every `readdir` is a READDIRPLUS that also returns
  attributes, which fill the attribute cache. With `AT_STATX_DONT_SYNC`,
  kvreap's follow-up `statx` calls can be answered from that cache.
- **`O_DIRECT` works** (measured): vLLM 0.31 did not fall back to buffered I/O.

### atime

- The client mounts `noatime`, so the client never sends atime updates. On
  NFSv3 the server owns atime; whether VAST updates it on `READ` depends on its
  view policy (not measured). vLLM 0.31 does not refresh atime on cache hits
  either. In practice, assume "hot" means "recently written" on VAST.

### Directory churn

- **vLLM issues about one `MKDIR` per stored block** (measured: 127–142
  MKDIR/s against 128–144 CREATE/s for all three variants). The layout has
  up to 1M leaf directories per rank, so at ~57k blocks nearly every store
  lands in a new leaf. Evictors that `rmdir` emptied leaves keep it that way:
  variant a issued 191 RMDIR/s, b 205/s, c 96/s.

### Hard mounts

- `hard` with `timeo=600` means filesystem calls block indefinitely if VAST
  is unreachable (inferred from the options). Worker threads sit in
  uninterruptible sleep, SIGTERM cannot interrupt them, and the chart's
  liveness probe (`python3 -c "os.path.exists('/kv-cache')"`) hangs too, so
  the kubelet restarts a container that cannot exit.

## Local NVMe (hostPath / emptyDir)

On waldorf GPU nodes the local NVMe is one XFS RAID (`/dev/md127`, 28 TiB)
shared by `/var/lib/kubelet`, `/mnt/local`, `/var/log/pods` and others,
mounted with `prjquota`.

- **`statvfs` reports the whole node disk** (measured): an `emptyDir` with
  `sizeLimit: 20Gi` reported 28 TiB, 8% used, so the kubelet does not apply
  a project quota for `sizeLimit` here (it enforces it by eviction; inferred).
  No evictor can use percentage
  thresholds on hostPath or emptyDir volumes: usage includes every other pod
  on the node, and 85% of 28 TiB is never reached.
- **`/var/tmp` on these nodes is a 15 GiB RAM disk, already full** (measured).
  Don't use paths outside `/mnt/local` for hostPath volumes.
- XFS project quotas can bound a directory: with an enforced project quota on
  a `PROJINHERIT` directory, `statvfs` on that directory reports the quota
  (XFS behavior). Setting one needs `CAP_SYS_ADMIN` on the node.
- Local XFS keeps atime under `relatime`: the first read after a write
  updates atime, later reads within 24 hours don't. Hot protection only sees
  the first read.

## Other filesystems (not tested)

| Filesystem | Capacity via `statvfs` | atime | Notes |
|---|---|---|---|
| CephFS (ceph-csi subvolumes) | quota, if the client is configured to report quota in df; check per deployment | lazy; reads may not update it | `AT_STATX_DONT_SYNC` avoids cap recalls from writers |
| Generic NFS export | whole export unless the server maps quotas to `FSSTAT` | server-side policy | same `lookupcache` / `ENOENT` race as VAST |
| Lustre | whole filesystem; project quotas need `lfs quota -p` | server-side, coarse | `statx` size may need an OST glimpse |
| Local ext4/XFS with a dedicated LV (topolvm, LVM CSI) | exact | `relatime` | the easy case |
| Filesystems without `d_type` (XFS `ftype=0`, some NFS) | — | — | kvreap stats each entry to learn its type: one extra op per entry |

## Changes to kvreap

In rough priority order.

1. **Pluggable capacity source** (implemented). `statvfs` stays the default;
   `CAPACITY_BYTES` sets the volume size (e.g. the PVC request) when the
   filesystem can't report it. Used bytes then come from a sampled estimate:
   mean block bytes over the last 64 sampled buckets x bucket count. Workers
   record every bucket they sample while evicting; a sampler thread lists the
   buckets every 60 s and samples one bucket per second (back to back until
   16 are in), so the estimate keeps up with vLLM while idle. Eviction waits
   for the first estimate. At startup kvreap warns when `statvfs` total is
   more than 10x `CAPACITY_BYTES` (VAST before the first write, hostPath,
   emptyDir). The estimate counts only block files, not temp files or other
   data on the volume, so leave headroom below 100%.
2. **Detect missing atime and say so** (implemented). At startup kvreap
   creates a probe file in the cache directory (the mount if the cache
   directory doesn't exist yet), back-dates its atime by two days, reads it
   with `O_DIRECT` (as vLLM does, so an NFS client cannot serve the read from
   its page cache), and `statx`es it again with `AT_STATX_FORCE_SYNC`. If
   atime didn't move, or couldn't be back-dated, it logs a warning that hot
   protection is effectively "recently written" and eviction is
   oldest-written-first. The probe file is removed afterwards. There is no
   metrics endpoint, so the result is only logged.
3. **Stop removing leaf directories right after the last unlink**
   (implemented). It cost one RMDIR per deleted block, raced vLLM's
   `makedirs` + create (an `rmdir` between them fails the store; not
   observed in our runs, where vLLM logged no store failures), and bought
   little: the next store in that leaf recreates it. kvreap now only reaps
   leaves and buckets that sampling finds empty and unchanged for
   `DIR_CLEANUP_TTL_SECONDS`. Still to do: measure vLLM MKDIR and evictor
   RMDIR against the numbers above.
4. **Pace to need instead of bursting** (implemented). On VAST the AIMD
   back-off never fired and the op budget climbed to ~7k ops/s during prunes.
   The controller now plans each prune: bytes over target when it started
   (or now, if larger) ÷ 60 s, converted to files with the mean sampled file
   size and to ops with the ops per delete measured during the prune (3 until
   20 deletes). The budget runs at the lowest of that (floored at 20 ops/s),
   `2 x DELETION_MAX_FILES_PER_SECOND` and AIMD, and AIMD no longer grows
   while it isn't the binding limit. The emergency band stays unpaced.
5. **Configurable emergency band** (implemented). The unpaced band starts at
   97% by default, which is too late on VAST with a 5 s usage lag and a soft
   quota. `EMERGENCY_THRESHOLD` moves it; the default stays
   `max(97, CLEANUP_THRESHOLD)`, and values below `CLEANUP_THRESHOLD` are
   rejected.
6. **Health check that never touches the mount** (proposed). Add
   `kvreap healthcheck`, which checks the controller's last successful
   `statvfs` timestamp from a heartbeat file in `/tmp`, so a hung NFS mount
   shows up as unhealthy instead of a probe that never returns. This needs a
   chart change to use it, so it is opt-in.
7. **Document scheduling** (proposed). Recommend pod anti-affinity against
   vLLM pods on NFS-backed volumes, since co-located pods share one NFS client.

## Changes outside kvreap

- **vLLM: refresh atime on cache hits**, rate-limited (only if older than a
  few minutes), so evictors can tell read-hot blocks from merely old ones on
  every filesystem.
- **Indexer: expire or re-validate storage-tier entries.** The evictor cannot
  address removals to the vLLM pod that stored a block on shared storage;
  see the README.
