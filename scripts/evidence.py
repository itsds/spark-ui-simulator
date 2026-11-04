#!/usr/bin/env python3
"""Scenario evidence: the numbers each `solution.md` quotes, recomputed from the History Server REST API.

  uv run scripts/evidence.py 01-join-skew [--url http://localhost:18080]

Every solution has an "Evidence" table whose rows start with an id (`E1`, `E2`, …)
and end with the value. This script computes each value from a running History
Server (the same numbers the UI shows; container logs through the executors' log
links, i.e. the NodeManager mock) and prints the table with ✓/✗ against the
solution. The app ids come from the scenario's `scenario.yaml`; an incident's
evidence can use both runs (`c.runs["last_good"]`). `tests/test_scenario*_hs.py`
do the same check in CI.
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
        self.runs: dict[str, Ctx] = {}  # every run of the scenario by role (baseline | last_good | incident)

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


# ---------------------------------------------------------------------------- scenario 01 (join skew)


def _app_duration(c: Ctx) -> str:
    return fmt_duration(c.hs.get(f"applications/{c.app_id}")["attempts"][0]["duration"])


def _fmt_rec(n: float) -> str:
    return f"{int(n)}"


def _summary(c: Ctx, sid: int, qs: str) -> dict:
    return c.get(f"stages/{sid}/0/taskSummary", quantiles=qs)


def _durations(c: Ctx, sid: int) -> str:
    """Median / 75th / max as the Summary Metrics table rounds them, then the REST API's ms."""
    med, p75, mx = _summary(c, sid, "0.5,0.75,1.0")["duration"]
    return (f"{fmt_duration(med)} / {fmt_duration(p75)} / {fmt_duration(mx)} "
            f"(REST: {secs(med)} / {secs(p75)} / {mx / 1000:,.0f} s, {mx / med:,.0f}x the median)")


def _shuffle_read_med_max(c: Ctx, sid: int) -> str:
    sr = _summary(c, sid, "0.5,1.0")["shuffleReadMetrics"]
    (b_med, b_max), (r_med, r_max) = sr["readBytes"], sr["readRecords"]
    return f"{fmt_bytes(b_med)} / {_fmt_rec(r_med)} → {fmt_bytes(b_max)} / {_fmt_rec(r_max)}"


def _straggler(c: Ctx, sid: int) -> dict:
    return max(c.hs.all_tasks(c.app_id, sid), key=lambda t: t["duration"])


def _straggler_row(c: Ctx) -> str:
    t = _straggler(c, 5)
    return f"index {t['index']}, TID {t['taskId']}, executor {t['executorId']} ({t['host']}), {t['taskLocality']}"


def _ts(x: str) -> float:
    from datetime import datetime

    return datetime.strptime(x.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000


def _tail_wait(c: Ctx) -> str:
    tasks = c.hs.all_tasks(c.app_id, 5)
    ends = sorted(_ts(t["launchTime"]) + t["duration"] for t in tasks)
    first = _ts(c.stage(5)["firstTaskLaunchedTime"])
    return (f"{len(ends) - 1:,} tasks done {fmt_duration(ends[-2] - first)} after the first launch, "
            f"then {fmt_duration(ends[-1] - ends[-2])} for the last one")


def _spill_stage(c: Ctx) -> str:
    st = c.stage(5)
    mx = _summary(c, 5, "1.0")["memoryBytesSpilled"][0]
    share = "all from one task" if mx == st["memoryBytesSpilled"] else f"max task {fmt_bytes(mx)}"
    return f"{fmt_bytes(st['memoryBytesSpilled'])} / {fmt_bytes(st['diskBytesSpilled'])}, {share}"


def _by_executor(c: Ctx) -> str:
    st = c.get("stages/5/0")
    st = st[0] if isinstance(st, list) else st
    top = sorted(st["executorSummary"].items(), key=lambda kv: -kv[1]["taskTime"])
    (e1, a), (e2, b) = top[0], top[1]
    return f"executor {e1}: {fmt_duration(a['taskTime'])}; next, executor {e2}: {fmt_duration(b['taskTime'])}"


def _sql_metric_text(node: dict, name: str) -> str:
    """The metric as the plan graph prints it, without the 'total (min, med, max …)' header line."""
    v = next(m["value"] for m in node["metrics"] if m["name"] == name)
    return v.split("\n")[1] if "\n" in v else v


def _join_input_sort(c: Ctx) -> dict:
    """The Sort under the SortMergeJoin on the events side (the one that reads the big Exchange)."""
    q = _sql(c, 0)
    by_id = {n["nodeId"]: n for n in q["nodes"]}
    smj = next(n for n in q["nodes"] if n["nodeName"] == "SortMergeJoin")
    kids = [by_id[e["fromId"]] for e in q["edges"] if e["toId"] == smj["nodeId"]]

    def below(n):  # follow WholeStageCodegen / InputAdapter wrappers down to the Sort
        while n["nodeName"] != "Sort":
            n = by_id[next(e["fromId"] for e in q["edges"] if e["toId"] == n["nodeId"])]
        return n

    sorts = [below(k) for k in kids]
    return max(sorts, key=lambda n: _metric_bytes(n, "spill size"))


def _metric_bytes(node: dict, name: str) -> float:
    txt = _metric(node, name)
    num, unit = txt.split()
    return float(num) * {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}[unit]


def _events_exchange(c: Ctx) -> dict:
    return max(_nodes(c, 0, "Exchange"), key=lambda n: _rows_metric(n, "records read"))


def _rows_metric(node: dict, name: str) -> int:
    return int(_metric(node, name).replace(",", ""))


def _input_growth(c: Ctx) -> str:
    good = max(_nodes(c.runs["last_good"], 0, "Scan parquet "), key=_rows)
    bad = max(_nodes(c, 0, "Scan parquet "), key=_rows)
    g, b = _rows(good), _rows(bad)
    return f"{g:,} → {b:,} rows (+{100 * (b - g) / g:.1f} %)"


def _last_good_join(c: Ctx) -> str:
    g = c.runs["last_good"]
    med, mx = g.quantiles(5, "duration")
    sr = _summary(g, 5, "1.0")["shuffleReadMetrics"]["readBytes"][0]
    return f"median {secs(med)}, max {secs(mx)}, max shuffle read {fmt_bytes(sr)}"


def _env(c: Ctx, key: str) -> str:
    vals = set()
    for run in c.runs.values():
        props = dict(run.get("environment")["sparkProperties"])
        vals.add(props.get(key, "(not set)"))
    return " / ".join(sorted(vals))


def _health(c: Ctx) -> str:
    execs = [e for e in c.get("allexecutors") if e["id"] != "driver"]
    failed = sum(e["failedTasks"] for e in execs)
    dead = sum(1 for e in execs if not e["isActive"] and e.get("removeReason") not in (None, ""))
    return f"GC {_gc_share(c)}, {failed} failed tasks, {dead} executors lost"


def _spill_log(c: Ctx) -> str:
    """UnsafeExternalSorter lines of the straggler in its executor's stderr (through the NodeManager log link)."""
    import requests

    t = _straggler(c, 5)
    ex = next(e for e in c.get("allexecutors") if e["id"] == t["executorId"])
    link = ex["executorLogs"]["stderr"].replace("start=-4096", "start=0")
    html = requests.get(link, timeout=60).text
    thread = None
    counts: list[int] = []
    for line in html.splitlines():
        if f"(TID {t['taskId']})" in line and "Running task" in line:
            thread = True
        if thread and "UnsafeExternalSorter" in line and "spilling sort data" in line:
            n = int(re.search(r"\((\d+) +times? so far\)", line).group(1))
            if n == 1:
                counts.append(0)
            counts[-1] += 1
        if f"Finished task {t['index']}.0 in stage 5.0 (TID {t['taskId']})" in line:
            break
    return f"{sum(counts)} lines ({' + '.join(map(str, counts))} spills, {len(counts)} sorters)"


def _same_host_tasks(c: Ctx) -> str:
    """The straggler's executor runs its other tasks at normal speed: not a slow node."""
    import statistics

    tasks = c.hs.all_tasks(c.app_id, 5)
    hot = max(tasks, key=lambda t: t["duration"])
    on = [t["duration"] for t in tasks if t["executorId"] == hot["executorId"] and t is not hot]
    off = [t["duration"] for t in tasks if t["executorId"] != hot["executorId"]]
    return (f"executor {hot['executorId']}'s other {len(on)} tasks: median {secs(statistics.median(on))} "
            f"(all other executors: {secs(statistics.median(off))})")


S01 = {
    "E1": lambda c: f"{_app_duration(c.runs['last_good'])} → {_app_duration(c)}",
    "E2": _sql_durations,
    "E3": lambda c: f"{c.stage(5)['numTasks']:,} tasks, {fmt_duration(_stage_ms(c.stage(5)))}",
    "E4": lambda c: _durations(c, 5),
    "E5": lambda c: _shuffle_read_med_max(c, 5),
    "E6": _straggler_row,
    "E7": _tail_wait,
    "E8": _spill_stage,
    "E9": _by_executor,
    "E10": lambda c: _sql_metric_text(_join_input_sort(c), "spill size"),
    "E11": lambda c: _sql_metric_text(_events_exchange(c), "remote bytes read"),
    "E12": _input_growth,
    "E13": _last_good_join,
    "E14": lambda c: _env(c, "spark.sql.adaptive.skewJoin.enabled"),
    "E15": _health,
    "E16": _spill_log,
    "E17": _same_host_tasks,
}

EVIDENCE = {"00-healthy-baseline": S00, "01-join-skew": S01}


# ---------------------------------------------------------------------------- solution.md


ROW = re.compile(r"^\|\s*(E\d+)\s*\|.*\|\s*`?([^|`]+?)`?\s*\|\s*$")


def solution_values(scenario: str) -> dict[str, str]:
    """Evidence rows of scenarios/<id>/solution.md: `| E1 | where | what | value |` (value in backticks)."""
    path = ROOT / "scenarios" / scenario / "solution.md"
    if not path.is_file():  # a scenario being written: print the values to put in it
        return {}
    text = path.read_text()
    out = {}
    for line in text.splitlines():
        m = ROW.match(line.strip())
        if m:
            out[m.group(1)] = m.group(2).strip()
    return out


def compute(hs: hs_api.HistoryServer, scenario: str) -> dict[str, str]:
    """Every evidence value of a scenario. The functions get the incident run's (or the baseline's) context;
    `ctx.runs` has all of the scenario's runs."""
    from sparksim.scenario import load_scenario

    sc = load_scenario(scenario)
    runs = {role: Ctx(hs, sc.app_id(role)) for role in sc.roles}
    ctx = runs.get("incident") or runs["baseline"]
    for r in runs.values():
        r.runs = runs
    return {k: f(ctx) for k, f in EVIDENCE[scenario].items()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=sorted(EVIDENCE))
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
