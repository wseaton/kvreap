import xml.etree.ElementTree as ET
from pathlib import Path

from kvbench.chart import CHART_ORDER, nice_ticks, render
from tests.test_analyze import run_dir  # noqa: F401  (pytest fixture)


def test_report_writes_markdown_and_parseable_svgs(run_dir: Path, tmp_path: Path) -> None:  # noqa: F811
    out = tmp_path / "report"
    md = render([run_dir], out, "Smoke")
    text = md.read_text()
    assert text.startswith("# Smoke")
    assert "## All metrics" in text
    for name, heading in CHART_ORDER:
        assert f"## {heading}" in text
        for suffix in ("", "-dark"):
            svg = out / "charts" / f"{name}{suffix}.svg"
            root = ET.parse(svg).getroot()
            assert root.tag.endswith("svg"), svg
        assert f'srcset="charts/{name}-dark.svg"' in text
    usage = (out / "charts" / "usage.svg").read_text()
    assert "cleanup 85%" in usage
    assert "<path" in usage
    assert (out / "summary-c.json").exists()


def test_light_and_dark_use_their_own_palettes(run_dir: Path, tmp_path: Path) -> None:  # noqa: F811
    render([run_dir], tmp_path, "t")
    light = (tmp_path / "charts" / "evictor-ops.svg").read_text()
    dark = (tmp_path / "charts" / "evictor-ops-dark.svg").read_text()
    assert "#2a78d6" in light and "#fcfcfb" in light
    assert "#3987e5" in dark and "#1a1a19" in dark


def test_nice_ticks() -> None:
    assert nice_ticks(0, 100, 4) == [0, 25, 50, 75, 100]
    assert nice_ticks(0, 9, 5) == [0, 2, 4, 6, 8]
