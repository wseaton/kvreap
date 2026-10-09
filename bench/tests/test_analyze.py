import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kvbench.analyze import deletion_windows, report, summarize

T0 = 1_800_000_000.0


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sample(t: float, ops: dict[str, list[int]], used: int, total: int = 100) -> str:
    return json.dumps({"t": t, "ops": ops, "usage": {"used": used, "total": total}})


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    d = tmp_path / "c"
    (d / "loadgen" / "hot").mkdir(parents=True)
    (d / "loadgen" / "churn").mkdir(parents=True)
    meta = {"variant": "c", "events": {"load_start": T0, "load_end": T0 + 100}}
    (d / "meta.json").write_text(json.dumps(meta))

    # vLLM: 10 LOOKUP/s at 1 ms while the evictor is idle, 5 ms during deletion (t in [40, 60)).
    lines, lookups, rtt = [], 0, 0
    for i in range(101):
        if i:
            lookups += 10
            rtt += 50 if 40 < i <= 60 else 10
        used = 80 + (i if i <= 50 else 100 - i)
        lines.append(sample(T0 + i, {"LOOKUP": [lookups, lookups, 0, 0, 0, 0, rtt, rtt, 0]}, min(used, 90)))
    (d / "vllm-sampler.jsonl").write_text("\n".join(lines))

    # Evictor: 20 REMOVE/s and 40 GETATTR/s only while deleting.
    ev, rm, ga = [], 0, 0
    for i in range(101):
        if 40 < i <= 60:
            rm += 20
            ga += 40
        ev.append(
            sample(T0 + i, {"REMOVE": [rm, rm, 0, 0, 0, 0, rm, rm, 0], "GETATTR": [ga, ga, 0, 0, 0, 0, 0, 0, 0]}, 0)
        )
    (d / "evictor-sampler.jsonl").write_text("\n".join(ev))
    (d / "evictor.log").write_text(
        f"{iso(T0 + 40)} INFO DELETION_START: timestamp=1, usage=85.00%\n"
        f"{iso(T0 + 60)} INFO DELETION_END: timestamp=2, usage=70.00%\n"
    )
    (d / "vllm.log").write_text(f"{iso(T0)} ERROR store failed: Disk quota exceeded\n")
    (d / "loadgen" / "hot" / "hot-0.json").write_text(
        json.dumps({"ttfts": [0.010, 0.020, 0.030, 0.040, 1.0], "failed": 1})
    )
    (d / "loadgen" / "churn" / "churn-0.json").write_text(
        json.dumps({"total_input_tokens": 4096 * 10, "duration": 10.0})
    )
    return d


def test_deletion_windows_parsed_from_timestamps(run_dir: Path) -> None:
    w = deletion_windows(run_dir / "evictor.log")
    assert len(w) == 1
    assert w[0][0] == pytest.approx(T0 + 40, abs=1e-3)
    assert w[0][1] == pytest.approx(T0 + 60, abs=1e-3)


def test_summary_attributes_ops_and_rtt(run_dir: Path) -> None:
    s = summarize(run_dir)
    assert s.evictor_ops["REMOVE"].count == 400
    assert s.evictor_ops["REMOVE"].per_sec == pytest.approx(4.0)
    assert s.evictor_total == (1200, pytest.approx(12.0))
    assert s.vllm_ops["LOOKUP"].count == 1000
    assert s.vllm_rtt_deleting["LOOKUP"] == pytest.approx(5.0)
    assert s.vllm_rtt_idle["LOOKUP"] == pytest.approx(1.0)
    assert len(s.prune_cycles) == 1
    assert s.prune_cycles[0]["seconds"] == pytest.approx(20.0, abs=1e-3)
    assert s.max_usage_pct == pytest.approx(90.0)
    assert s.errors["quota_exceeded"] == 1
    assert s.bench["hot_requests"] == 5
    assert s.bench["hot_failed"] == 1
    assert s.bench["hot_ttft_p50_ms"] == pytest.approx(30.0)
    assert s.bench["churn_input_tok_per_s"] == pytest.approx(4096.0)


def test_report_has_a_column_per_run(run_dir: Path) -> None:
    out = report([summarize(run_dir), summarize(run_dir)])
    header = out.splitlines()[0]
    assert header == "| metric | c | c |"
    assert "evictor REMOVE/s (mean / p95 10s)" in out


def test_chain_counters_come_from_the_last_chains_line(run_dir: Path) -> None:
    log = run_dir / "evictor.log"
    log.write_text(
        log.read_text() + f"{iso(T0 + 30)} INFO chains policy=TailFirst index_blocks=5 event_batches=1 decode_errors=0 "
        "deleted_heads=9 deleted_root=9 deleted_orphan=0 deleted_internal=0 deleted_leaf=0 deferrals=0\n"
        + f"{iso(T0 + 90)} INFO chains policy=TailFirst index_blocks=5 event_batches=40 decode_errors=0 "
        "blocks_stored=900 deleted_heads=12 deleted_root=10 deleted_orphan=2 deleted_internal=3 "
        "deleted_leaf=70 deleted_untracked=1 deferrals=55\n" + f"{iso(T0 + 91)} INFO status files_deleted=86\n"
    )
    s = summarize(run_dir)
    assert s.chains["deleted_heads"] == 12
    assert s.chains["deleted_leaf"] == 70
    assert s.chains["deferrals"] == 55
    assert "policy" not in s.chains
    out = report([s])
    assert "| chain deletions: heads (root + orphan) | 12 |" in out
    assert "| chain deletions: leaf | 70 |" in out


def test_no_chain_rows_without_a_chains_line(run_dir: Path) -> None:
    s = summarize(run_dir)
    assert s.chains == {}
    assert "chain deletions" not in report([s])


def test_agent_records_split_first_and_returning_turns(tmp_path: Path) -> None:
    from kvbench.analyze import load_bench

    agent = tmp_path / "agent"
    agent.mkdir()
    rows = [
        {"turn": 0, "ttft_ms": 3000.0, "status": "ok", "prompt_tokens": 18000, "t0": 100.0, "tend": 104.0},
        {"turn": 1, "ttft_ms": 400.0, "status": "ok", "prompt_tokens": 18700, "t0": 104.0, "tend": 105.0},
        {"turn": 2, "ttft_ms": 600.0, "status": "ok", "prompt_tokens": 19400, "t0": 105.0, "tend": 110.0},
        {"turn": 3, "ttft_ms": 0.0, "status": "error", "prompt_tokens": 0, "t0": 110.0, "tend": 110.0},
    ]
    (agent / "requests_0.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    b = load_bench(tmp_path)
    assert b["agent_requests"] == 3
    assert b["agent_failed"] == 1
    assert b["agent_first_ttft_p50_ms"] == pytest.approx(3000.0)
    assert b["agent_later_ttft_p50_ms"] == pytest.approx(600.0), "nearest rank"
    assert b["agent_prompt_tok_per_s"] == pytest.approx((18000 + 18700 + 19400) / 10.0)
