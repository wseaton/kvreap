#!/usr/bin/env python3
"""Emit one JSON line per interval with the NFS per-op counters of a mount,
its statvfs usage and, optionally, selected Prometheus metrics.

Runs as a sidecar sharing the pod's volume mount. Stdlib only, so it works in
python:3.12-slim and in the vLLM image.

Per-op fields (from /proc/self/mountstats, statvers 1.1):
    ops, transmissions, major_timeouts, bytes_sent, bytes_recv,
    queue_ms, rtt_ms, execute_ms, errors
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request

OP_FIELDS = (
    "ops",
    "trans",
    "timeouts",
    "bytes_sent",
    "bytes_recv",
    "queue_ms",
    "rtt_ms",
    "execute_ms",
    "errors",
)


def read_mountstats(mount: str, path: str = "/proc/self/mountstats") -> dict[str, list[int]]:
    ops: dict[str, list[int]] = {}
    in_device = False
    in_ops = False
    with open(path) as f:
        for line in f:
            if line.startswith("device "):
                in_device = f" mounted on {mount} with " in line
                in_ops = False
                continue
            if not in_device:
                continue
            stripped = line.strip()
            if stripped.startswith("per-op statistics"):
                in_ops = True
            elif in_ops and ":" in stripped:
                name, rest = stripped.split(":", 1)
                values = [int(v) for v in rest.split()]
                if any(values):
                    ops[name] = values
    return ops


def read_usage(mount: str) -> dict[str, int]:
    st = os.statvfs(mount)
    total = st.f_blocks * st.f_frsize
    return {"total": total, "used": total - st.f_bfree * st.f_frsize}


def du(path: str) -> int:
    used, stack = 0, [path]
    while stack:
        try:
            with os.scandir(stack.pop()) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(e.path)
                        else:
                            used += e.stat(follow_symlinks=False).st_blocks * 512
                    except FileNotFoundError:
                        pass
        except FileNotFoundError:
            pass
    return used


METRIC_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+(\S+)")


def scrape(url: str, keep: re.Pattern[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    with urllib.request.urlopen(url, timeout=2) as resp:
        for raw in resp.read().decode().splitlines():
            m = METRIC_LINE.match(raw)
            if m and keep.search(m.group(1)):
                try:
                    out[m.group(1)] = float(m.group(2))
                except ValueError:
                    pass
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mount", required=True)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--scrape-url")
    p.add_argument("--scrape-every", type=int, default=5, help="scrape every N intervals")
    p.add_argument("--scrape-keep", default=r"^vllm:(kv_offload|external_prefix_cache|prefix_cache)")
    p.add_argument("--output", help="append JSON lines to this file instead of stdout")
    p.add_argument(
        "--capacity-bytes",
        type=int,
        help="report usage as bytes under --mount against this size instead of statvfs",
    )
    p.add_argument("--du-every", type=int, default=10, help="walk --mount every N intervals")
    args = p.parse_args()

    keep = re.compile(args.scrape_keep)
    sink = open(args.output, "a", buffering=1) if args.output else sys.stdout  # noqa: SIM115
    tick = 0
    next_at = time.monotonic()
    while True:
        rec: dict[str, object] = {"t": time.time()}
        try:
            rec["ops"] = read_mountstats(args.mount)
        except OSError as e:
            rec["ops_error"] = str(e)
        if args.capacity_bytes:
            if tick % args.du_every == 0:
                rec["usage"] = {"total": args.capacity_bytes, "used": du(args.mount)}
        else:
            try:
                rec["usage"] = read_usage(args.mount)
            except OSError as e:
                rec["usage_error"] = str(e)
        if args.scrape_url and tick % args.scrape_every == 0:
            try:
                rec["metrics"] = scrape(args.scrape_url, keep)
            except OSError as e:
                rec["scrape_error"] = str(e)
        sink.write(json.dumps(rec, separators=(",", ":")) + "\n")
        sink.flush()
        tick += 1
        next_at += args.interval
        time.sleep(max(0.0, next_at - time.monotonic()))


if __name__ == "__main__":
    sys.exit(main())
