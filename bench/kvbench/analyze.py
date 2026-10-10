"""Summarize one or more run directories produced by `kvbench run`."""

from __future__ import annotations

import itertools
import json
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

GIB = 1024**3
DATA_OPS = {"READ", "WRITE", "COMMIT"}
VLLM_RTT_OPS = ("LOOKUP", "GETATTR", "CREATE", "RENAME", "WRITE", "READ")
ERROR_PATTERNS = {
    "quota_exceeded": re.compile(r"Disk quota exceeded|EDQUOT", re.IGNORECASE),
    "store_failed": re.compile(r"(failed|error).{0,40}(store|write)", re.IGNORECASE),
    "load_failed": re.compile(r"(failed|error).{0,40}(load|read)", re.IGNORECASE),
}
CHAIN_COUNTERS = (
    ("deleted_heads", "chain deletions: heads (root + orphan)"),
    ("deleted_root", "chain deletions: root"),
    ("deleted_orphan", "chain deletions: orphan (parent gone)"),
    ("deleted_internal", "chain deletions: internal"),
    ("deleted_leaf", "chain deletions: leaf"),
    ("deleted_untracked", "chain deletions: untracked"),
    ("deferrals", "chain deferrals"),
    ("cascaded", "chain deletions: cascaded below a deleted root"),
    ("undigested", "blocks announced without a digest"),
    ("young_edges", "leaf edges skipped as too young"),
    ("young_fallbacks", "young leaf edges deleted under pressure"),
    ("event_batches", "KV event batches received"),
    ("decode_errors", "KV event decode errors"),
)
AGENT_KEYS = (
    "agent_requests",
    "agent_failed",
    "agent_first_ttft_p50_ms",
    "agent_first_ttft_p90_ms",
    "agent_later_ttft_p50_ms",
    "agent_later_ttft_p90_ms",
    "agent_later_ttft_p99_ms",
    "agent_prompt_tok_per_s",
)
KV_PAIR = re.compile(r"(\w+)=(\d+)\b")
TS_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s")


@dataclass
class Sample:
    t: float
    ops: dict[str, list[int]]
    usage: dict[str, int] | None
    metrics: dict[str, float] | None


def load_samples(path: Path) -> list[Sample]:
    out: list[Sample] = []
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        out.append(Sample(rec["t"], rec.get("ops", {}), rec.get("usage"), rec.get("metrics")))
    return out


def window(samples: list[Sample], start: float, end: float) -> list[Sample]:
    return [s for s in samples if start <= s.t <= end]


def op_field(s: Sample, op: str, idx: int) -> int:
    v = s.ops.get(op)
    return v[idx] if v and len(v) > idx else 0


def all_ops(samples: list[Sample]) -> set[str]:
    return {op for s in samples for op in s.ops}


@dataclass
class OpStats:
    count: int
    per_sec: float
    p95_10s_per_sec: float
    mean_rtt_ms: float | None
    mean_queue_ms: float | None = None


def op_stats(samples: list[Sample], op: str) -> OpStats:
    if len(samples) < 2:
        return OpStats(0, 0.0, 0.0, None, None)
    first, last = samples[0], samples[-1]
    count = op_field(last, op, 0) - op_field(first, op, 0)
    rtt = op_field(last, op, 6) - op_field(first, op, 6)
    queue = op_field(last, op, 5) - op_field(first, op, 5)
    span = last.t - first.t
    rates = []
    for a, b in zip(samples[::10], samples[10::10]):
        dt = b.t - a.t
        if dt > 0:
            rates.append((op_field(b, op, 0) - op_field(a, op, 0)) / dt)
    p95 = sorted(rates)[int(0.95 * (len(rates) - 1))] if rates else 0.0
    return OpStats(
        count,
        count / span if span > 0 else 0.0,
        p95,
        rtt / count if count else None,
        queue / count if count else None,
    )


def total_rate(samples: list[Sample], include_data: bool) -> tuple[int, float]:
    ops = [o for o in all_ops(samples) if include_data or o not in DATA_OPS]
    total = sum(op_stats(samples, o).count for o in ops)
    span = samples[-1].t - samples[0].t if len(samples) > 1 else 0.0
    return total, total / span if span > 0 else 0.0


def parse_ts(line: str) -> float | None:
    m = TS_PREFIX.match(line)
    if not m:
        return None
    raw = m.group(1).rstrip("Z")
    if "." in raw:
        head, frac = raw.split(".")
        raw = f"{head}.{frac[:6]}"
    return datetime.fromisoformat(raw + "+00:00").timestamp()


def deletion_windows(evictor_log: Path) -> list[tuple[float, float | None]]:
    windows: list[tuple[float, float | None]] = []
    if not evictor_log.exists():
        return windows
    for line in evictor_log.read_text().splitlines():
        ts = parse_ts(line)
        if ts is None:
            continue
        if "DELETION_START" in line:
            windows.append((ts, None))
        elif "DELETION_END" in line and windows and windows[-1][1] is None:
            windows[-1] = (windows[-1][0], ts)
    return windows


def usage_at(samples: list[Sample], t: float) -> float | None:
    best = min((s for s in samples if s.usage), key=lambda s: abs(s.t - t), default=None)
    if best is None or best.usage is None or best.usage["total"] == 0:
        return None
    return best.usage["used"] / best.usage["total"] * 100


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))]


def chain_counters(evictor_log: Path) -> dict[str, int]:
    """Counters from kvreap's last `chains` status line, empty without one."""
    if not evictor_log.exists():
        return {}
    last = next((ln for ln in reversed(evictor_log.read_text().splitlines()) if " chains " in ln), None)
    if last is None:
        return {}
    return {k: int(v) for k, v in KV_PAIR.findall(last)}


def load_agent(dir_: Path) -> dict[str, Any]:
    """nyann-bench `requests_*.jsonl`: TTFT for first turns (cold) and later turns (returning)."""
    first: list[float] = []
    later: list[float] = []
    failed = 0
    prompt_tokens = 0
    t0, t1 = float("inf"), 0.0
    for f in sorted(dir_.glob("requests_*.jsonl")):
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("status") != "ok":
                failed += 1
                continue
            (first if r.get("turn", 0) == 0 else later).append(float(r["ttft_ms"]))
            prompt_tokens += int(r.get("prompt_tokens", 0))
            t0, t1 = min(t0, float(r["t0"])), max(t1, float(r["tend"]))
    span = t1 - t0 if t1 > t0 else 0.0
    return {
        "agent_requests": len(first) + len(later),
        "agent_failed": failed,
        "agent_first_ttft_p50_ms": percentile(first, 0.5),
        "agent_first_ttft_p90_ms": percentile(first, 0.9),
        "agent_later_ttft_p50_ms": percentile(later, 0.5),
        "agent_later_ttft_p90_ms": percentile(later, 0.9),
        "agent_later_ttft_p99_ms": percentile(later, 0.99),
        "agent_prompt_tok_per_s": prompt_tokens / span if span and prompt_tokens else None,
    }


def load_bench(dir_: Path) -> dict[str, Any]:
    if (dir_ / "agent").is_dir():
        return load_agent(dir_ / "agent")
    hot_ttfts: list[float] = []
    hot_failed = 0
    churn_input_tokens = 0
    churn_duration = 0.0
    for f in sorted((dir_ / "hot").glob("hot-*.json")):
        r = json.loads(f.read_text())
        hot_ttfts += [t * 1000 for t in r.get("ttfts", []) if t]
        hot_failed += int(r.get("failed", 0))
    for f in sorted((dir_ / "churn").glob("churn-*.json")):
        r = json.loads(f.read_text())
        churn_input_tokens += int(r.get("total_input_tokens", 0))
        churn_duration += float(r.get("duration", 0.0))
    return {
        "hot_requests": len(hot_ttfts),
        "hot_failed": hot_failed,
        "hot_ttft_p50_ms": percentile(hot_ttfts, 0.5),
        "hot_ttft_p90_ms": percentile(hot_ttfts, 0.9),
        "hot_ttft_p99_ms": percentile(hot_ttfts, 0.99),
        "churn_input_tok_per_s": churn_input_tokens / churn_duration if churn_duration else None,
    }


def metric_deltas(samples: list[Sample], pattern: str = r"offload|kv_cache|prefix_cache") -> dict[str, float]:
    keep = re.compile(pattern)
    scraped = [s.metrics for s in samples if s.metrics]
    if len(scraped) < 2:
        return {}
    first, last = scraped[0], scraped[-1]
    return {
        k: last[k] - first.get(k, 0.0)
        for k in sorted(last)
        if keep.search(k) and (k.endswith("_total") or "_total{" in k) and last[k] != first.get(k, 0.0)
    }


@dataclass
class RunSummary:
    variant: str
    duration_s: float
    evictor_ops: dict[str, OpStats] = field(default_factory=dict)
    evictor_total: tuple[int, float] = (0, 0.0)
    vllm_ops: dict[str, OpStats] = field(default_factory=dict)
    vllm_rtt_deleting: dict[str, float | None] = field(default_factory=dict)
    vllm_rtt_idle: dict[str, float | None] = field(default_factory=dict)
    prune_cycles: list[dict[str, float | None]] = field(default_factory=list)
    max_usage_pct: float | None = None
    pct_time_above_cleanup: float | None = None
    errors: dict[str, int] = field(default_factory=dict)
    bench: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    chains: dict[str, int] = field(default_factory=dict)


def rtt_in_windows(samples: list[Sample], op: str, windows: list[tuple[float, float]], inside: bool) -> float | None:
    ops = rtt = 0
    for a, b in itertools.pairwise(samples):
        mid = (a.t + b.t) / 2
        hit = any(s <= mid <= e for s, e in windows)
        if hit == inside:
            ops += op_field(b, op, 0) - op_field(a, op, 0)
            rtt += op_field(b, op, 6) - op_field(a, op, 6)
    return rtt / ops if ops else None


def summarize(dir_: Path, cleanup_pct: float = 85.0) -> RunSummary:
    meta = json.loads((dir_ / "meta.json").read_text())
    ev = meta["events"]
    start, end = ev["load_start"], ev["load_end"]
    vllm = window(load_samples(dir_ / "vllm-sampler.jsonl"), start, end)
    evictor = window(load_samples(dir_ / "evictor-sampler.jsonl"), start, end)
    s = RunSummary(variant=meta["variant"], duration_s=end - start)

    s.evictor_ops = {op: op_stats(evictor, op) for op in sorted(all_ops(evictor))}
    s.evictor_total = total_rate(evictor, include_data=False) if len(evictor) > 1 else (0, 0.0)
    s.vllm_ops = {op: op_stats(vllm, op) for op in sorted(all_ops(vllm))}

    raw_windows = deletion_windows(dir_ / "evictor.log")
    closed = [(a, b if b is not None else end) for a, b in raw_windows]
    for op in VLLM_RTT_OPS:
        s.vllm_rtt_deleting[op] = rtt_in_windows(vllm, op, closed, inside=True)
        s.vllm_rtt_idle[op] = rtt_in_windows(vllm, op, closed, inside=False)
    for a, b in raw_windows:
        u0, u1 = usage_at(vllm, a), usage_at(vllm, b) if b else None
        s.prune_cycles.append(
            {
                "start": a,
                "seconds": (b - a) if b else None,
                "usage_start": u0,
                "usage_end": u1,
            }
        )

    usages = [x.usage["used"] / x.usage["total"] * 100 for x in vllm if x.usage and x.usage["total"]]
    if usages:
        s.max_usage_pct = max(usages)
        s.pct_time_above_cleanup = 100 * sum(u >= cleanup_pct for u in usages) / len(usages)

    log = (dir_ / "vllm.log").read_text() if (dir_ / "vllm.log").exists() else ""
    s.errors = {k: len(p.findall(log)) for k, p in ERROR_PATTERNS.items()}
    s.bench = load_bench(dir_ / "loadgen" if (dir_ / "loadgen").exists() else dir_)
    s.metrics = metric_deltas(vllm)
    s.chains = chain_counters(dir_ / "evictor.log")
    return s


def fmt(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "-"
    if isinstance(v, int):
        return f"{v:,}"
    return f"{v:,.{digits}f}"


def report(summaries: list[RunSummary]) -> str:
    cols = [s.variant for s in summaries]
    lines = ["| metric | " + " | ".join(cols) + " |", "|---|" + "---|" * len(cols)]

    def row(name: str, values: list[str]) -> None:
        lines.append(f"| {name} | " + " | ".join(values) + " |")

    row("load duration (s)", [fmt(s.duration_s, 0) for s in summaries])
    row("**evictor metadata ops/s (mean)**", [fmt(s.evictor_total[1]) for s in summaries])
    row("evictor metadata ops (total)", [fmt(s.evictor_total[0]) for s in summaries])
    ev_ops = sorted({op for s in summaries for op in s.evictor_ops})
    for op in ev_ops:
        row(
            f"evictor {op}/s (mean / p95 10s)",
            [
                f"{fmt(s.evictor_ops[op].per_sec)} / {fmt(s.evictor_ops[op].p95_10s_per_sec)}"
                if op in s.evictor_ops
                else "-"
                for s in summaries
            ],
        )
    for op in VLLM_RTT_OPS:
        row(
            f"vLLM {op} RTT ms (all)",
            [fmt(s.vllm_ops[op].mean_rtt_ms, 2) if op in s.vllm_ops else "-" for s in summaries],
        )
        row(
            f"vLLM {op} RTT ms (deleting / idle)",
            [f"{fmt(s.vllm_rtt_deleting.get(op), 2)} / {fmt(s.vllm_rtt_idle.get(op), 2)}" for s in summaries],
        )
    for op in VLLM_RTT_OPS:
        row(
            f"vLLM {op} client queue ms",
            [fmt(s.vllm_ops[op].mean_queue_ms, 3) if op in s.vllm_ops else "-" for s in summaries],
        )
    for tier_metric, label in (("read", "vLLM FS read s/GiB"), ("write", "vLLM FS write s/GiB")):
        row(label, [fmt(fs_seconds_per_gib(s, tier_metric), 2) for s in summaries])
    row("vLLM metadata ops/s", [fmt(total_rate_from(s)) for s in summaries])
    row("prune cycles", [fmt(len(s.prune_cycles)) for s in summaries])
    row(
        "prune seconds (median)",
        [
            fmt(statistics.median([c["seconds"] for c in s.prune_cycles if c["seconds"]]))
            if any(c["seconds"] for c in s.prune_cycles)
            else "-"
            for s in summaries
        ],
    )
    row("max usage %", [fmt(s.max_usage_pct) for s in summaries])
    row("% time >= cleanup threshold", [fmt(s.pct_time_above_cleanup) for s in summaries])
    for k in ("hot_ttft_p50_ms", "hot_ttft_p90_ms", "hot_ttft_p99_ms"):
        row(k.replace("_", " "), [fmt(s.bench.get(k)) for s in summaries])
    for k in AGENT_KEYS:
        if any(k in s.bench for s in summaries):
            row(k.replace("_", " "), [fmt(s.bench.get(k)) for s in summaries])
    row(
        "hot requests (failed)",
        [f"{fmt(s.bench.get('hot_requests'))} ({fmt(s.bench.get('hot_failed'))})" for s in summaries],
    )
    row("churn input tok/s", [fmt(s.bench.get("churn_input_tok_per_s"), 0) for s in summaries])
    for k in ERROR_PATTERNS:
        row(f"vLLM log: {k}", [fmt(s.errors.get(k, 0)) for s in summaries])
    if any(s.chains for s in summaries):
        for k, label in CHAIN_COUNTERS:
            row(label, [fmt(s.chains.get(k)) for s in summaries])
    metric_names = sorted({k for s in summaries for k in s.metrics})
    for k in metric_names:
        row(f"`{k}` Δ", [fmt(s.metrics.get(k), 0) for s in summaries])
    return "\n".join(lines)


def fs_seconds_per_gib(s: RunSummary, kind: str) -> float | None:
    secs = next((v for k, v in s.metrics.items() if f"tiering_{kind}_time_total" in k), None)
    nbytes = next((v for k, v in s.metrics.items() if f"tiering_{kind}_bytes_total" in k), None)
    if not secs or not nbytes:
        return None
    return secs / (nbytes / GIB)


def total_rate_from(s: RunSummary) -> float:
    return sum(o.per_sec for op, o in s.vllm_ops.items() if op not in DATA_OPS)


def main(dirs: list[Path]) -> int:
    summaries = [summarize(d) for d in dirs]
    print(report(summaries))
    for d, s in zip(dirs, summaries):
        (d / "summary.json").write_text(json.dumps(s, default=lambda o: o.__dict__, indent=2))
    return 0
