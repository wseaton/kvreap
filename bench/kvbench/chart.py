"""Render run directories into a Markdown report with static SVG charts.

Each chart is written twice, `<name>.svg` (light) and `<name>-dark.svg`, and
the report picks one with `<picture>` so it follows the GitHub color scheme.
"""

from __future__ import annotations

import itertools
import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

from kvbench.analyze import (
    DATA_OPS,
    RunSummary,
    Sample,
    deletion_windows,
    load_samples,
    op_field,
    report,
    summarize,
    window,
)

Point = tuple[float, float | None]

USAGE_STRIDE = 5
RATE_STRIDE = 5
RTT_STRIDE = 10
RTT_OPS = ("LOOKUP", "CREATE", "WRITE", "READ")
MAX_ECDF_POINTS = 300


@dataclass(frozen=True)
class Theme:
    name: str
    surface: str
    text: str
    text_secondary: str
    muted: str
    grid: str
    axis: str
    band: str
    series: tuple[str, str, str, str]


LIGHT = Theme(
    "light",
    "#fcfcfb",
    "#0b0b0b",
    "#52514e",
    "#898781",
    "#e1e0d9",
    "#c3c2b7",
    "#89878120",
    ("#2a78d6", "#eb6834", "#1baf7a", "#eda100"),
)
DARK = Theme(
    "dark",
    "#1a1a19",
    "#ffffff",
    "#c3c2b7",
    "#898781",
    "#2c2c2a",
    "#383835",
    "#c3c2b71a",
    ("#3987e5", "#d95926", "#199e70", "#c98500"),
)


@dataclass(frozen=True)
class Series:
    name: str
    idx: int
    points: list[Point]


@dataclass(frozen=True)
class Rule:
    y: float
    label: str


def fmt(v: float | None) -> str:
    if v is None:
        return "-"
    a = abs(v)
    if a >= 100 or float(v).is_integer():
        return f"{v:,.0f}"
    if a >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}".rstrip("0").rstrip(".") if v != 0 else "0"


def nice_ticks(lo: float, hi: float, n: int = 5) -> list[float]:
    span = hi - lo or 1.0
    mag = 10 ** math.floor(math.log10(span / n))
    step = next((m * mag for m in (1, 2, 2.5, 5, 10) if span / (m * mag) <= n), 10 * mag)
    start = math.ceil(lo / step) * step
    out = []
    v = start
    while v <= hi + 1e-9:
        out.append(round(v, 10))
        v += step
    return out


W, H = 640, 260
M_L, M_R, M_T, M_B = 48, 72, 30, 28


def svg_open(theme: Theme, title: str) -> list[str]:
    return [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
            f'font-family="system-ui, -apple-system, Segoe UI, sans-serif" role="img" aria-label="{escape(title)}">'
        ),
        f'<rect width="{W}" height="{H}" rx="8" fill="{theme.surface}"/>',
        f'<text x="{M_L}" y="18" font-size="13" font-weight="600" fill="{theme.text}">{escape(title)}</text>',
    ]


def legend(theme: Theme, items: list[tuple[str, int]]) -> list[str]:
    if len(items) < 2:
        return []
    out, x = [], W - 12
    for name, idx in reversed(items):
        x -= 22 + 7 * len(name)
        out.append(f'<line x1="{x}" x2="{x + 14}" y1="14" y2="14" stroke="{theme.series[idx % 4]}" stroke-width="2"/>')
        out.append(f'<text x="{x + 18}" y="18" font-size="11" fill="{theme.text_secondary}">{escape(name)}</text>')
    return out


def line_chart(
    theme: Theme,
    title: str,
    series: list[Series],
    *,
    y_unit: str = "",
    x_unit: str = "s",
    log_x: bool = False,
    y_max: float | None = None,
    rules: list[Rule] | None = None,
    bands: list[tuple[float, float]] | None = None,
) -> str:
    rules = rules or []
    out = svg_open(theme, title) + legend(theme, [(s.name, s.idx) for s in series])
    pts = [p for s in series for p in s.points if p[1] is not None]
    if not pts:
        out.append(
            f'<text x="{W / 2}" y="{H / 2}" text-anchor="middle" font-size="12" fill="{theme.muted}">no data</text></svg>'
        )
        return "\n".join(out)
    x0, x1 = min(p[0] for p in pts), max(p[0] for p in pts)
    if log_x:
        x0 = max(x0, 0.1)
    y0 = 0.0
    y1 = y_max if y_max is not None else max([p[1] for p in pts if p[1] is not None] + [r.y for r in rules]) * 1.05
    y1 = y1 if y1 > y0 else y0 + 1

    def lx(v: float) -> float:
        return math.log10(max(v, x0))

    def sx(v: float) -> float:
        frac = (lx(v) - lx(x0)) / ((lx(x1) - lx(x0)) or 1) if log_x else (v - x0) / ((x1 - x0) or 1)
        return M_L + frac * (W - M_L - M_R)

    def sy(v: float) -> float:
        return H - M_B - (v - y0) / (y1 - y0) * (H - M_T - M_B)

    for a, b in bands or []:
        a, b = max(a, x0), min(b, x1)
        if b <= a:
            continue
        out.append(
            f'<rect x="{sx(a):.1f}" y="{M_T}" width="{max(1.0, sx(b) - sx(a)):.1f}" height="{H - M_T - M_B}" fill="{theme.band}"/>'
        )
    for v in nice_ticks(y0, y1, 4):
        stroke = theme.axis if v == y0 else theme.grid
        out.append(
            f'<line x1="{M_L}" x2="{W - M_R}" y1="{sy(v):.1f}" y2="{sy(v):.1f}" stroke="{stroke}" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{M_L - 6}" y="{sy(v) + 4:.1f}" text-anchor="end" font-size="11" fill="{theme.muted}">{fmt(v)}{escape(y_unit)}</text>'
        )
    x_ticks = [t for t in (1, 10, 100, 1_000, 10_000, 100_000) if x0 <= t <= x1] if log_x else nice_ticks(x0, x1, 6)
    for v in x_ticks:
        out.append(
            f'<text x="{sx(v):.1f}" y="{H - 8}" text-anchor="middle" font-size="11" fill="{theme.muted}">{fmt(v)}{escape(x_unit)}</text>'
        )
    rule_labels: list[str] = []
    for r in rules:
        out.append(
            f'<line x1="{M_L}" x2="{W - M_R}" y1="{sy(r.y):.1f}" y2="{sy(r.y):.1f}" stroke="{theme.muted}" stroke-width="1" stroke-dasharray="4 3"/>'
        )
        rule_labels.append(
            f'<text x="{M_L + 6}" y="{sy(r.y) - 4:.1f}" font-size="11" fill="{theme.muted}" '
            f'stroke="{theme.surface}" stroke-width="3" paint-order="stroke">{escape(r.label)}</text>'
        )
    label_ys: list[float] = []
    for s in series:
        d, pen = [], False
        for x, y in s.points:
            if y is None:
                pen = False
                continue
            d.append(f"{'L' if pen else 'M'}{sx(max(x, x0)):.1f},{sy(y):.1f}")
            pen = True
        color = theme.series[s.idx % 4]
        out.append(
            f'<path d="{"".join(d)}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>'
        )
        last = next((p for p in reversed(s.points) if p[1] is not None), None)
        if last is not None and last[1] is not None and len(series) <= 4:
            y = sy(last[1]) + 4
            while any(abs(y - other) < 12 for other in label_ys):
                y += 12
            label_ys.append(y)
            out.append(
                f'<text x="{W - M_R + 4}" y="{y:.1f}" font-size="11" fill="{theme.text_secondary}">{escape(s.name)}</text>'
            )
    out += rule_labels
    out.append("</svg>")
    return "\n".join(out)


def grouped_bars(theme: Theme, title: str, rows: list[tuple[str, int, dict[str, float]]], unit: str = "/s") -> str:
    ops = sorted({op for _, _, vals in rows for op in vals}, key=lambda op: -max(v.get(op, 0.0) for _, _, v in rows))
    bh, gap = 10, 2
    group = len(rows) * (bh + gap) + 12
    top = 30
    height = max(120, top + len(ops) * group + 6)
    out = svg_open(theme, title)
    out[0] = out[0].replace(
        f'viewBox="0 0 {W} {H}" width="{W}" height="{H}"', f'viewBox="0 0 {W} {height}" width="{W}" height="{height}"'
    )
    out[1] = f'<rect width="{W}" height="{height}" rx="8" fill="{theme.surface}"/>'
    out += legend(theme, [(name, idx) for name, idx, _ in rows])
    if not ops:
        out.append(
            f'<text x="{W / 2}" y="{height / 2}" text-anchor="middle" font-size="12" fill="{theme.muted}">no evictor ops</text></svg>'
        )
        return "\n".join(out)
    left = 110
    vmax = max(max(vals.values(), default=0.0) for _, _, vals in rows) or 1.0

    def sx(v: float) -> float:
        return left + v / vmax * (W - left - M_R)

    for gi, op in enumerate(ops):
        gy = top + gi * group
        out.append(
            f'<text x="{left - 8}" y="{gy + (group - 12) / 2 + 4:.1f}" text-anchor="end" font-size="11" fill="{theme.muted}">{escape(op)}</text>'
        )
        for ri, (name, idx, vals) in enumerate(rows):
            v = vals.get(op, 0.0)
            y = gy + ri * (bh + gap)
            out.append(
                f'<rect x="{left}" y="{y}" width="{max(1.0, sx(v) - left):.1f}" height="{bh}" rx="2" fill="{theme.series[idx % 4]}"/>'
            )
            out.append(
                f'<text x="{sx(v) + 4:.1f}" y="{y + bh - 1}" font-size="10" fill="{theme.text_secondary}">{escape(name)} {fmt(v)}{unit}</text>'
            )
    out.append("</svg>")
    return "\n".join(out)


def usage_series(samples: list[Sample], t0: float) -> list[Point]:
    return [
        (round(s.t - t0, 1), round(s.usage["used"] / s.usage["total"] * 100, 2))
        for s in samples[::USAGE_STRIDE]
        if s.usage and s.usage["total"]
    ]


def metadata_rate_series(samples: list[Sample], t0: float) -> list[Point]:
    pts: list[Point] = []
    strided = samples[::RATE_STRIDE]
    for a, b in itertools.pairwise(strided):
        dt = b.t - a.t
        if dt <= 0:
            continue
        ops = {*a.ops, *b.ops} - DATA_OPS
        delta = sum(op_field(b, op, 0) - op_field(a, op, 0) for op in ops)
        pts.append((round(b.t - t0, 1), round(delta / dt, 2)))
    return pts


def rtt_series(samples: list[Sample], op: str, t0: float) -> list[Point]:
    pts: list[Point] = []
    strided = samples[::RTT_STRIDE]
    for a, b in itertools.pairwise(strided):
        n = op_field(b, op, 0) - op_field(a, op, 0)
        rtt = op_field(b, op, 6) - op_field(a, op, 6)
        pts.append((round(b.t - t0, 1), round(rtt / n, 3) if n > 0 else None))
    return pts


def ttft_ecdf(run_dir: Path) -> list[Point]:
    ttfts: list[float] = []
    for f in sorted((run_dir / "loadgen" / "hot").glob("hot-*.json")):
        ttfts += [t * 1000 for t in json.loads(f.read_text()).get("ttfts", []) if t]
    ttfts.sort()
    n = len(ttfts)
    if n == 0:
        return []
    step = max(1, n // MAX_ECDF_POINTS)
    idx = sorted({*range(0, n, step), n - 1})
    return [(round(ttfts[i], 2), round((i + 1) / n * 100, 2)) for i in idx]


@dataclass(frozen=True)
class RunSeries:
    name: str
    image: str
    usage: list[Point]
    evictor_ops: list[Point]
    rtt: dict[str, list[Point]]
    ttft: list[Point]
    deleting: list[tuple[float, float]]
    op_types: dict[str, float]
    cleanup: float
    target: float


def run_series(run_dir: Path, summary: RunSummary) -> RunSeries:
    meta = json.loads((run_dir / "meta.json").read_text())
    t0, t1 = meta["events"]["load_start"], meta["events"]["load_end"]
    vllm = window(load_samples(run_dir / "vllm-sampler.jsonl"), t0, t1)
    evictor = window(load_samples(run_dir / "evictor-sampler.jsonl"), t0, t1)
    cfg = meta.get("evictor_values", {}).get("config", {})
    return RunSeries(
        name=summary.variant,
        image=meta.get("evictor_image") or "no evictor",
        usage=usage_series(vllm, t0),
        evictor_ops=metadata_rate_series(evictor, t0),
        rtt={op: rtt_series(vllm, op, t0) for op in RTT_OPS},
        ttft=ttft_ecdf(run_dir),
        deleting=[(a - t0, (b or t1) - t0) for a, b in deletion_windows(run_dir / "evictor.log")],
        op_types={op: st.per_sec for op, st in summary.evictor_ops.items()},
        cleanup=float(cfg.get("cleanupThreshold", 85)),
        target=float(cfg.get("targetThreshold", 70)),
    )


def charts(runs: list[RunSeries], theme: Theme) -> dict[str, str]:
    def ser(get: Callable[[RunSeries], list[Point]]) -> list[Series]:
        return [Series(r.name, i, get(r)) for i, r in enumerate(runs)]

    first = runs[0]
    out = {
        "usage": line_chart(
            theme,
            "PVC usage (% of quota)" + (" — shaded: deleting" if len(runs) == 1 and first.deleting else ""),
            ser(lambda r: r.usage),
            y_unit="%",
            y_max=100,
            rules=[
                Rule(first.cleanup, f"cleanup {fmt(first.cleanup)}%"),
                Rule(first.target, f"target {fmt(first.target)}%"),
            ],
            bands=first.deleting if len(runs) == 1 else None,
        ),
        "evictor-ops": line_chart(theme, "Evictor NFS metadata ops/s (5 s windows)", ser(lambda r: r.evictor_ops)),
        "ttft": line_chart(
            theme,
            "Hot-prefix TTFT, cumulative % of requests",
            ser(lambda r: r.ttft),
            y_unit="%",
            x_unit=" ms",
            log_x=True,
            y_max=100,
        ),
        "evictor-op-types": grouped_bars(
            theme, "Evictor NFS ops by type (mean ops/s)", [(r.name, i, r.op_types) for i, r in enumerate(runs)]
        ),
    }
    for op in RTT_OPS:
        out[f"rtt-{op.lower()}"] = line_chart(
            theme, f"vLLM {op} RTT (mean ms per op, 10 s windows)", ser(lambda r, op=op: r.rtt[op]), y_unit=" ms"
        )
    return out


CHART_ORDER = (
    ("usage", "PVC usage"),
    ("evictor-ops", "Evictor metadata load"),
    ("evictor-op-types", "Evictor ops by type"),
    ("rtt-lookup", "vLLM LOOKUP latency"),
    ("rtt-create", "vLLM CREATE latency"),
    ("rtt-write", "vLLM WRITE latency"),
    ("rtt-read", "vLLM READ latency"),
    ("ttft", "Hot-prefix TTFT"),
)


def picture(name: str, alt: str) -> str:
    return (
        "<picture>"
        f'<source media="(prefers-color-scheme: dark)" srcset="charts/{name}-dark.svg">'
        f'<img alt="{escape(alt)}" src="charts/{name}.svg">'
        "</picture>"
    )


def render(dirs: list[Path], out_dir: Path, title: str) -> Path:
    summaries = [summarize(d) for d in dirs]
    runs = [run_series(d, s) for d, s in zip(dirs, summaries, strict=True)]
    chart_dir = out_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    for theme, suffix in ((LIGHT, ""), (DARK, "-dark")):
        for name, svg in charts(runs, theme).items():
            (chart_dir / f"{name}{suffix}.svg").write_text(svg)

    lines = [f"# {title}", "", "| run | evictor image |", "|---|---|"]
    lines += [f"| {r.name} | `{r.image}` |" for r in runs]
    lines += ["", "Time axes are seconds since the load generator started.", ""]
    for name, heading in CHART_ORDER:
        lines += [f"## {heading}", "", picture(name, heading), ""]
    lines += ["## All metrics", "", report(summaries), ""]
    md = out_dir / "REPORT.md"
    md.write_text("\n".join(lines))
    for s in summaries:
        (out_dir / f"summary-{s.variant}.json").write_text(json.dumps(s, default=lambda o: o.__dict__, indent=2))
    return md


def main(dirs: list[Path], out_dir: Path, title: str) -> int:
    md = render(dirs, out_dir, title)
    print(f"wrote {md}")
    return 0
