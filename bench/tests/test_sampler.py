from pathlib import Path

from kvbench.sampler import read_mountstats

MOUNTSTATS = """\
device proc mounted on /proc with fstype proc
device vast:/k8s/pvc-other mounted on /models with fstype nfs statvers=1.1
\topts:\trw,vers=3
\tper-op statistics
\t        NULL: 0 0 0 0 0 0 0 0 0
\t     GETATTR: 99 99 0 1 2 3 4 5 0
device vast:/k8s/pvc-kv mounted on /kv-cache with fstype nfs statvers=1.1
\topts:\trw,vers=3,nosharecache
\tage:\t12
\tevents:\t1 2 3
\tper-op statistics
\t        NULL: 0 0 0 0 0 0 0 0 0
\t     GETATTR: 10 10 0 1200 1120 1 25 30 0
\t      LOOKUP: 7 7 0 900 800 0 14 16 2
\t      REMOVE: 3 3 0 300 360 0 9 10 0
device tmpfs mounted on /dev/shm with fstype tmpfs
"""


def test_reads_only_the_requested_mount_and_skips_zero_ops(tmp_path: Path) -> None:
    f = tmp_path / "mountstats"
    f.write_text(MOUNTSTATS)
    ops = read_mountstats("/kv-cache", str(f))
    assert ops == {
        "GETATTR": [10, 10, 0, 1200, 1120, 1, 25, 30, 0],
        "LOOKUP": [7, 7, 0, 900, 800, 0, 14, 16, 2],
        "REMOVE": [3, 3, 0, 300, 360, 0, 9, 10, 0],
    }


def test_mount_prefix_does_not_match_longer_paths(tmp_path: Path) -> None:
    f = tmp_path / "mountstats"
    f.write_text(MOUNTSTATS.replace("/kv-cache with", "/kv-cache-2 with"))
    assert read_mountstats("/kv-cache", str(f)) == {}


def test_main_appends_json_lines_to_output(tmp_path: Path, monkeypatch) -> None:
    import json
    import sys

    from kvbench import sampler

    out = tmp_path / "samples.jsonl"
    calls = {"n": 0}

    def stop_after_two(_: float) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(sys, "argv", ["sampler", "--mount", str(tmp_path), "--output", str(out), "--interval", "0"])
    monkeypatch.setattr(sampler.time, "sleep", stop_after_two)
    try:
        sampler.main()
    except KeyboardInterrupt:
        pass
    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert len(lines) == 2
    assert lines[0]["usage"]["total"] > 0
    # tmp_path is not an NFS mount; on hosts without /proc the read is reported, not fatal.
    assert lines[0].get("ops") == {} or "ops_error" in lines[0]


def test_capacity_mode_reports_bytes_under_the_mount(tmp_path: Path, monkeypatch) -> None:
    import json
    import sys

    from kvbench import sampler

    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "b" / "x.bin").write_bytes(b"\1" * 65536)
    (tmp_path / "y.bin").write_bytes(b"\1" * 65536)
    out = tmp_path.parent / f"{tmp_path.name}.jsonl"
    calls = {"n": 0}

    def stop_after_three(_: float) -> None:
        calls["n"] += 1
        if calls["n"] == 3:
            raise KeyboardInterrupt

    argv = ["sampler", "--mount", str(tmp_path), "--output", str(out), "--interval", "0"]
    monkeypatch.setattr(sys, "argv", [*argv, "--capacity-bytes", "1048576", "--du-every", "2"])
    monkeypatch.setattr(sampler.time, "sleep", stop_after_three)
    try:
        sampler.main()
    except KeyboardInterrupt:
        pass
    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert [("usage" in rec) for rec in lines] == [True, False, True]
    assert lines[0]["usage"]["total"] == 1048576
    assert lines[0]["usage"]["used"] >= 2 * 65536
