#!/usr/bin/env python3
"""Scenario evidence: the numbers each `solution.md` quotes, recomputed from the History Server REST API.

  uv run scripts/evidence.py 00-healthy-baseline [--url http://localhost:18080]

Every solution has an "Evidence" table whose rows start with an id (`E1`, `E2`, …)
and end with the value. This script computes each value from a running History
Server (the same numbers the UI shows) and prints the table with ✓/✗ against
the solution. `tests/test_scenario00_hs.py` does the same check in CI.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import hs_api  # noqa: E402

# ---------------------------------------------------------------------------- UI formatting (Spark's UIUtils)


def fmt_duration(ms: float) -> str:
    """`UIUtils.formatDuration`: what the Jobs/Stages tables show."""
    if ms < 100:
        return f"{int(ms)} ms"
    s = ms / 1000
    if s < 1:
        return f"{s:.1f} s"
    if s < 60:
        return f"{s:.0f} s"
    m = s / 60
    if m < 10:
        return f"{m:.1f} min"
    if m < 60:
        return f"{m:.0f} min"
    return f"{m / 60:.1f} h"


def fmt_bytes(n: float) -> str:
    """`Utils.bytesToString` (binary units; a unit is used from 2x its size)."""
    for unit, size in (("TiB", 1 << 40), ("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if n >= 2 * size:
            return f"{n / size:.1f} {unit}"
    return f"{int(n)} B"


def secs(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


# ---------------------------------------------------------------------------- scenario 00


class Ctx:
    def __init__(self, hs: hs_api.HistoryServer, app_id: str):
        self.hs, self.app_id = hs, app_id
        self._stages = None

    def get(self, sub: str, **params):
        return self.hs.app(self.app_id, sub, **params)

    @property
    def stages(self) -> list[dict]:
        if self._stages is None:
            self._stages = self.get("stages")
        return self._stages

    def stage(self, sid: int) -> dict:
        return next(s for s in self.stages if s["stageId"] == sid and s["status"] != "SKIPPED")

    def quantiles(self, sid: int, metric: str, qs="0.5,1.0") -> list[float]:
        return self.get(f"stages/{sid}/0/taskSummary", quantiles=qs)[metric]


def _locality(ctx: Ctx, sid: int) -> str:
    """The stage page's "Locality Level Summary", from the task list."""
    counts: dict[str, int] = {}
    for t in ctx.hs.all_tasks(ctx.app_id, sid):
        counts[t["taskLocality"]] = counts.get(t["taskLocality"], 0) + 1
    order = ["PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY"]
    return ", ".join(f"{k} {counts[k]:,}" for k in order if counts.get(k))


def _gc_share(ctx: Ctx) -> str:
    execs = [e for e in ctx.get("allexecutors") if e["id"] != "driver"]
    shares = [e["totalGCTime"] / e["totalDuration"] for e in execs if e["totalDuration"]]
    return f"{100 * min(shares):.1f} % – {100 * max(shares):.1f} %"


def _tasks_per_executor(ctx: Ctx) -> str:
    execs = [e for e in ctx.get("allexecutors") if e["id"] != "driver"]
    n = [e["completedTasks"] for e in execs]
    return f"{min(n)} – {max(n)}"


def _spill(ctx: Ctx) -> str:
    total = sum(s["memoryBytesSpilled"] + s["diskBytesSpilled"] for s in ctx.stages if s["status"] == "COMPLETE")
    return fmt_bytes(total)


def _exec_layout(ctx: Ctx) -> str:
    execs = [e for e in ctx.get("allexecutors") if e["id"] != "driver"]
    hosts = {e["hostPort"].rsplit(":", 1)[0] for e in execs}
    cores = {e["totalCores"] for e in execs}
    return f"{len(execs)} x {'/'.join(map(str, sorted(cores)))} cores on {len(hosts)} hosts"


# ---------------------------------------------------------------------------- SQL tab


def _sql(ctx: Ctx, exec_id: int) -> dict:
    return ctx.get(f"sql/{exec_id}", details="true", planDescription="false")


def _metric(node: dict, name: str) -> str:
    """A node's metric as the plan graph shows it; for totals `total (min, med, max …)` → the total."""
    v = next(m["value"] for m in node["metrics"] if m["name"] == name)
    return v.split("\n")[1].split(" (")[0] if "\n" in v else v


def _rows(node: dict) -> int:
    return int(_metric(node, "number of output rows").replace(",", ""))


def _nodes(ctx: Ctx, exec_id: int, name: str) -> list[dict]:
    return [n for n in _sql(ctx, exec_id)["nodes"] if n["nodeName"] == name.strip()]  # the REST API trims names


def _sql_durations(ctx: Ctx) -> str:
    q = sorted(ctx.get("sql", details="false"), key=lambda e: e["id"])
    return f"{len(q)} queries: " + ", ".join(fmt_duration(e["duration"]) for e in q)


def _bot_filter(ctx: Ctx) -> str:
    scan = max(_nodes(ctx, 0, "Scan parquet "), key=_rows)
    filt = max(_nodes(ctx, 0, "Filter"), key=_rows)
    return f"{_rows(scan):,} → {_rows(filt):,}"


def _broadcast(ctx: Ctx) -> str:
    (b,) = _nodes(ctx, 0, "BroadcastExchange")
    return f"{_metric(b, 'data size')}, {_rows(b):,} rows"


def _partial_aggregation(ctx: Ctx) -> str:
    (scan,) = _nodes(ctx, 2, "Scan parquet ")
    partial, final = sorted(_nodes(ctx, 2, "HashAggregate"), key=lambda n: -n["nodeId"])  # leaf side first
    return f"{_rows(scan):,} → {_rows(partial):,} → {_rows(final):,}"


def _dur_median_max(ctx: Ctx, sid: int) -> str:
    med, mx = ctx.quantiles(sid, "duration")
    return f"{secs(med)} / {secs(mx)} ({mx / med:.2f}x)"


S00 = {
    "E1": lambda c: fmt_duration(c.hs.get(f"applications/{c.app_id}")["attempts"][0]["duration"]),
    "E2": lambda c: f"{len(c.get('jobs'))} jobs, "
                    f"{sum(s['status'] == 'COMPLETE' for s in c.stages)} completed + "
                    f"{sum(s['status'] == 'SKIPPED' for s in c.stages)} skipped stages",
    "E3": lambda c: f"{sum(s['numCompleteTasks'] for s in c.stages if s['status'] == 'COMPLETE'):,}",
    "E4": _exec_layout,
    "E5": lambda c: f"{c.stage(2)['numTasks']:,} tasks, {fmt_duration(_stage_ms(c.stage(2)))}",
    "E6": lambda c: _dur_median_max(c, 2),
    "E7": lambda c: f"{fmt_bytes(c.stage(2)['inputBytes'])} in, {fmt_bytes(c.stage(2)['shuffleWriteBytes'])} "
                    "shuffle write",
    "E8": lambda c: _locality(c, 2),
    "E9": lambda c: f"{fmt_bytes(c.stage(5)['shuffleReadBytes'])} read, {fmt_bytes(c.stage(5)['outputBytes'])} "
                    f"written by {c.stage(5)['numTasks']:,} tasks",
    "E10": lambda c: _dur_median_max(c, 5),
    "E11": lambda c: f"{c.stage(8)['numTasks']} tasks",
    "E12": _gc_share,
    "E13": _spill,
    "E14": _tasks_per_executor,
    "E15": _sql_durations,
    "E16": _bot_filter,
    "E17": _broadcast,
    "E18": lambda c: f"{_rows(_nodes(c, 0, 'SortMergeJoin')[0]):,} rows",
    "E19": _partial_aggregation,
}


def _stage_ms(s: dict) -> float:
    from datetime import datetime

    def t(x):
        return datetime.strptime(x.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000

    # The Stages table measures from the first task launch (StageTableBase), not from submission.
    return t(s["completionTime"]) - t(s.get("firstTaskLaunchedTime") or s["submissionTime"])


SCENARIOS = {"00-healthy-baseline": ("application_1791902144377_48213", S00)}


# ---------------------------------------------------------------------------- solution.md


ROW = re.compile(r"^\|\s*(E\d+)\s*\|.*\|\s*`?([^|`]+?)`?\s*\|\s*$")


def solution_values(scenario: str) -> dict[str, str]:
    """Evidence rows of scenarios/<id>/solution.md: `| E1 | where | what | value |` (value in backticks)."""
    text = (ROOT / "scenarios" / scenario / "solution.md").read_text()
    out = {}
    for line in text.splitlines():
        m = ROW.match(line.strip())
        if m:
            out[m.group(1)] = m.group(2).strip()
    return out


def compute(hs: hs_api.HistoryServer, scenario: str) -> dict[str, str]:
    app_id, funcs = SCENARIOS[scenario]
    ctx = Ctx(hs, app_id)
    return {k: f(ctx) for k, f in funcs.items()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=sorted(SCENARIOS))
    ap.add_argument("--url", default=hs_api.DEFAULT_URL)
    a = ap.parse_args(argv)
    got = compute(hs_api.HistoryServer(a.url), a.scenario)
    want = solution_values(a.scenario)
    bad = 0
    for k, v in got.items():
        ok = want.get(k) == v
        bad += not ok
        print(f"{'✓' if ok else '✗'} {k:<4} {v}" + ("" if ok else f"    (solution.md: {want.get(k)!r})"))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
