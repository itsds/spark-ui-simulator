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
import collections
import html
import math
import re
import sys
from pathlib import Path

import requests

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


def fmt_bytes_js(n: float) -> str:
    """`formatBytes` in Spark's utils.js: what the stage page's Summary Metrics and task tables show
    (always the largest unit, one decimal, a trailing ".0" dropped: `1.5 GiB`, `3 GiB`)."""
    if n <= 0:
        return "0.0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    i = min(len(units) - 1, int(math.floor(math.log(n) / math.log(1024))))
    v = round(n / 1024 ** i, 1)
    return f"{v:g} {units[i]}" if v == int(v) else f"{v:.1f} {units[i]}"


def secs(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


# ---------------------------------------------------------------------------- scenario 00


class Ctx:
    """One YARN attempt of a run (`attempt=None`: the latest, the History Server's default view)."""

    def __init__(self, hs: hs_api.HistoryServer, app_id: str, attempt: str | None = None):
        self.hs, self.app_id, self.attempt = hs, app_id, attempt
        self._stages = None
        self.runs: dict[str, Ctx] = {}  # every run of the scenario by role (baseline | last_good | incident)

    def get(self, sub: str, **params):
        return self.hs.app(self.app_id, sub, self.attempt, **params)

    def tasks(self, sid: int, stage_attempt: int = 0) -> list[dict]:
        return self.hs.all_tasks(self.app_id, sid, stage_attempt, app_attempt=self.attempt)

    @property
    def attempts(self) -> dict[str, Ctx]:
        """Every YARN attempt of this run by attempt id ("1", "2")."""
        return {a: Ctx(self.hs, self.app_id, a) for a in self.hs.attempts(self.app_id)}

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
    return f"{fmt_bytes_js(b_med)} / {_fmt_rec(r_med)} → {fmt_bytes_js(b_max)} / {_fmt_rec(r_max)}"


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
            if n == 0:  # a sorter's first spill: "(0  time so far)"
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

# ---------------------------------------------------------------------------- scenario 02 (too few shuffle partitions)


def _job_ms(c: Ctx, job_id: int) -> float:
    j = c.get(f"jobs/{job_id}")
    return _ts(j["completionTime"]) - _ts(j["submissionTime"])


def _stage_row(c: Ctx, sid: int) -> str:
    return f"{c.stage(sid)['numTasks']:,} tasks, {fmt_duration(_stage_ms(c.stage(sid)))}"


def _uniform_durations(c: Ctx, sid: int) -> str:
    """Min / median / max as the Summary Metrics table rounds them, then the REST API's seconds."""
    mn, med, mx = _summary(c, sid, "0.0,0.5,1.0")["duration"]
    return (f"{fmt_duration(mn)} / {fmt_duration(med)} / {fmt_duration(mx)} "
            f"(REST: {secs(mn)} / {secs(med)} / {secs(mx)}, max {mx / med:.2f}x the median)")


def _shuffle_read_median(c: Ctx, sid: int) -> str:
    sr = _summary(c, sid, "0.5")["shuffleReadMetrics"]
    return f"{fmt_bytes_js(sr['readBytes'][0])} / {_fmt_rec(sr['readRecords'][0])}"


def _spill_everywhere(c: Ctx) -> str:
    st = c.stage(5)
    mn, mx = _summary(c, 5, "0.0,1.0")["memoryBytesSpilled"]
    every = "every task spilled" if mn > 0 else "not every task spilled"
    return (f"{fmt_bytes(st['memoryBytesSpilled'])} / {fmt_bytes(st['diskBytesSpilled'])}; {every} "
            f"({fmt_bytes_js(mn)} to {fmt_bytes_js(mx)} in memory per task)")


def _peak_memory(c: Ctx) -> str:
    med, mx = _summary(c, 5, "0.5,1.0")["peakExecutionMemory"]
    good = _summary(c.runs["last_good"], 5, "1.0")["peakExecutionMemory"][0]
    return f"median {fmt_bytes_js(med)}, max {fmt_bytes_js(mx)} (last good: max {fmt_bytes_js(good)})"


def _waves(c: Ctx) -> str:
    """How the 200 tasks used the 160 cores: launch waves, and how long most cores sat idle at the end."""
    tasks = c.hs.all_tasks(c.app_id, 5)
    launch = sorted(_ts(t["launchTime"]) for t in tasks)
    first = [t for t in tasks if _ts(t["launchTime"]) - launch[0] < 1_000]
    rest = [t for t in tasks if _ts(t["launchTime"]) - launch[0] >= 1_000]
    cores = sum(e["totalCores"] for e in c.get("allexecutors") if e["id"] != "driver")
    first_done = max(_ts(t["launchTime"]) + t["duration"] for t in first)
    end = max(_ts(t["launchTime"]) + t["duration"] for t in tasks)
    starts = sorted(_ts(t["launchTime"]) for t in rest)
    return (f"{len(first)} tasks at once, the other {len(rest)} from {fmt_duration(starts[0] - launch[0])} later; "
            f"at most {len(rest)} of {cores} cores busy for the last {fmt_duration(end - first_done)}")


def _same_input(c: Ctx) -> str:
    def row(run: Ctx) -> str:
        s = run.stage(2)
        return f"{fmt_bytes(s['inputBytes'])} in, {fmt_bytes(s['shuffleWriteBytes'])} shuffle write"
    return f"{row(c.runs['last_good'])} → {row(c)}"


def _exchange_partitions(c: Ctx) -> str:
    def n(run: Ctx) -> str:
        return " / ".join(sorted({_metric(x, "number of partitions") for x in _nodes(run, 0, "Exchange")}))
    return f"{n(c.runs['last_good'])} → {n(c)}"


PER_RUN_KEYS = {"spark.app.id", "spark.app.startTime", "spark.app.submitTime", "spark.driver.host",
                "spark.driver.port", "spark.yarn.app.id", "spark.yarn.app.container.log.dir"}


def _conf_diff(c: Ctx) -> str:
    """Spark Properties that differ between the runs, without the ones every run has its own value of."""
    good = dict(c.runs["last_good"].get("environment")["sparkProperties"])
    bad = dict(c.get("environment")["sparkProperties"])
    keys = sorted(k for k in set(good) | set(bad)
                  if good.get(k) != bad.get(k) and k not in PER_RUN_KEYS and "PROXY_URI_BASES" not in k)
    return "; ".join(f"{k}: {good.get(k, 'not set')} → {bad.get(k, 'not set')}" for k in keys)


def _task_time(c: Ctx) -> str:
    def total(run: Ctx) -> float:
        return sum(e["totalDuration"] for e in run.get("allexecutors") if e["id"] != "driver")
    return f"{fmt_duration(total(c.runs['last_good']))} → {fmt_duration(total(c))}"


def _executor_spill_log(c: Ctx) -> str:
    """The UnsafeExternalSorter lines in the stderr of the executor that ran stage 5's task 0 (through the
    NodeManager log link). Only stage 5 spills, so every such line in the log belongs to one of its tasks."""
    import collections

    import requests

    t = next(t for t in c.hs.all_tasks(c.app_id, 5) if t["index"] == 0)
    ex = next(e for e in c.get("allexecutors") if e["id"] == t["executorId"])
    html = requests.get(ex["executorLogs"]["stderr"].replace("start=-4096", "start=0"), timeout=60).text
    tasks = len(re.findall(r"Running task \S+ in stage 5\.0 ", html))
    sizes = collections.Counter(re.findall(r"spilling sort data of (.+?) to disk", html))
    lines = sum(sizes.values())
    by_size = ", ".join(f"{n} x {size}" for size, n in sorted(sizes.items(), key=lambda kv: -kv[1]))
    return f"executor {t['executorId']}: {tasks} tasks of stage 5, {lines} lines ({by_size})"


def _written_files(c: Ctx) -> str:
    def w(run: Ctx) -> str:
        (cmd,) = _nodes(run, 0, "Execute InsertIntoHadoopFsRelationCommand")
        return f"{_metric(cmd, 'number of written files')} files, {_metric(cmd, 'written output')}"
    return f"{w(c.runs['last_good'])} → {w(c)}"


S02 = {
    "E1": lambda c: f"{_app_duration(c.runs['last_good'])} → {_app_duration(c)}",
    "E2": lambda c: f"{fmt_duration(_job_ms(c.runs['last_good'], 3))} → {fmt_duration(_job_ms(c, 3))}",
    "E3": lambda c: f"{_stage_row(c.runs['last_good'], 5)} → {_stage_row(c, 5)}",
    "E4": lambda c: _uniform_durations(c, 5),
    "E5": lambda c: f"{_shuffle_read_median(c.runs['last_good'], 5)} → {_shuffle_read_median(c, 5)}",
    "E6": _spill_everywhere,
    "E7": _peak_memory,
    "E8": _waves,
    "E9": _same_input,
    "E10": _exchange_partitions,
    "E11": lambda c: _sql_metric_text(_join_input_sort(c), "spill size"),
    "E12": _conf_diff,
    "E13": _task_time,
    "E14": _executor_spill_log,
    "E15": _written_files,
}

# ---------------------------------------------------------------------------- scenario 09 (bad-node straggler)

BIG_STAGES = (2, 5, 6, 9)  # events scan, join + write, the two aggregations' map stages
TASK_STAGES = (2, 5, 6, 8, 9, 11)  # every stage the slow executor ran tasks in


def _executors(c: Ctx) -> list[dict]:
    return [e for e in c.get("allexecutors") if e["id"] != "driver"]


def _host(e: dict) -> str:
    return e["hostPort"].rsplit(":", 1)[0]


def _slow_executor(c: Ctx) -> dict:
    """The executor whose stage-2 tasks are the slowest (the learner's suspect)."""
    eid = _straggler(c, 2)["executorId"]
    return next(e for e in _executors(c) if e["id"] == eid)


def _stage_durations(c: Ctx) -> str:
    g = c.runs["last_good"]
    return ", ".join(f"stage {sid}: {fmt_duration(_stage_ms(g.stage(sid)))} → {fmt_duration(_stage_ms(c.stage(sid)))}"
                     for sid in BIG_STAGES)


def _straggler_durations(c: Ctx) -> str:
    med, mx = _summary(c, 2, "0.5,1.0")["duration"]
    return (f"median {fmt_duration(med)}, max {fmt_duration(mx)} (REST: {secs(med)} / {secs(mx)}, "
            f"{mx / med:.0f}x the median)")


def _normal_input(c: Ctx) -> str:
    med, mx = _summary(c, 2, "0.5,1.0")["inputMetrics"]["bytesRead"]
    t = _straggler(c, 2)
    mine = t["taskMetrics"]["inputMetrics"]["bytesRead"]
    return (f"median {fmt_bytes_js(med)}, max {fmt_bytes_js(mx)}; the longest task (index {t['index']}) read "
            f"{fmt_bytes_js(mine)}")


def _longest_tasks(c: Ctx) -> str:
    tasks = sorted(c.hs.all_tasks(c.app_id, 2), key=lambda t: -t["duration"])
    ex = _slow_executor(c)
    n = next(i for i, t in enumerate(tasks) if t["executorId"] != ex["id"])
    durs = [t["duration"] for t in tasks[:n]]
    nxt = tasks[n]
    return (f"the {n} longest tasks all ran on executor {ex['id']} ({_host(ex)}), {secs(min(durs))} to "
            f"{secs(max(durs))}; the next one: {secs(nxt['duration'])} (executor {nxt['executorId']})")


def _stage2_by_executor(c: Ctx) -> str:
    st = c.get("stages/2/0")
    st = st[0] if isinstance(st, list) else st
    ex = _slow_executor(c)
    summ = st["executorSummary"]
    mine = summ[ex["id"]]
    others = [v for k, v in summ.items() if k != ex["id"]]
    n = sorted(v["succeededTasks"] for v in others)
    t = sorted(v["taskTime"] for v in others)
    return (f"executor {ex['id']}: {mine['succeededTasks']} tasks, {fmt_duration(mine['taskTime'])}; the other "
            f"{len(others)}: {n[0]}–{n[-1]} tasks, {fmt_duration(t[0])}–{fmt_duration(t[-1])}")


def _every_stage(c: Ctx) -> str:
    """In each stage it ran tasks in, the slow executor's tasks are the longest ones (medians compared)."""
    import statistics

    ex = _slow_executor(c)
    parts = []
    for sid in TASK_STAGES:
        tasks = sorted(c.hs.all_tasks(c.app_id, sid), key=lambda t: -t["duration"])
        mine = [t["duration"] for t in tasks if t["executorId"] == ex["id"]]
        rest = [t["duration"] for t in tasks if t["executorId"] != ex["id"]]
        top = all(t["executorId"] == ex["id"] for t in tasks[:len(mine)])
        parts.append(f"stage {sid}: {len(mine)}{'' if top else ' (not all longest)'}, median "
                     f"{secs(statistics.median(mine))} vs {secs(statistics.median(rest))}")
    return "; ".join(parts)


def _executors_tab(c: Ctx) -> str:
    ex = _slow_executor(c)
    others = [e for e in _executors(c) if e["id"] != ex["id"]]
    n = sorted(e["completedTasks"] for e in others)
    return (f"executor {ex['id']}: {ex['completedTasks']} tasks, {fmt_duration(ex['totalDuration'])} task time, "
            f"{fmt_bytes(ex['totalInputBytes'])} input; the other {len(others)}: {n[0]}–{n[-1]} tasks, "
            f"{fmt_duration(min(e['totalDuration'] for e in others))}–"
            f"{fmt_duration(max(e['totalDuration'] for e in others))}")


def _late_registration(c: Ctx) -> str:
    from datetime import UTC, datetime

    ex = _slow_executor(c)
    adds = sorted(_ts(e["addTime"]) for e in _executors(c) if e["id"] != ex["id"])

    def hms(ms: float) -> str:
        return datetime.fromtimestamp(ms / 1000, UTC).strftime("%H:%M:%S")
    mine = _ts(ex["addTime"])
    return (f"executor {ex['id']} at {hms(mine)} UTC, the other 39 between {hms(adds[0])} and {hms(adds[-1])} "
            f"({(mine - adds[-1]) / 1000:.0f} s later)")


def _gc_slow(c: Ctx) -> str:
    ex = _slow_executor(c)
    others = [e["totalGCTime"] / e["totalDuration"] for e in _executors(c) if e["id"] != ex["id"]]
    return (f"executor {ex['id']}: {100 * ex['totalGCTime'] / ex['totalDuration']:.1f} %; the others "
            f"{100 * min(others):.1f} % – {100 * max(others):.1f} %")


def _rack_neighbours(c: Ctx) -> str:
    """The ticket's switch work was on rack04 = wn061–wn080: the slow executor's neighbours there are fine."""
    ex = _slow_executor(c)
    rack = sorted((e for e in _executors(c) if e["id"] != ex["id"] and 61 <= int(_host(e)[2:5]) <= 80),
                  key=lambda e: int(e["id"]))
    n = sorted(e["completedTasks"] for e in rack)
    return f"{len(rack)} executors on wn061–wn080 besides executor {ex['id']}: {n[0]}–{n[-1]} tasks each"


def _last_good_hosts(c: Ctx) -> str:
    ex = _slow_executor(c)
    host = _host(ex)
    g = _executors(c.runs["last_good"])
    on = [e["id"] for e in g if _host(e) == host]
    return (f"{len(g)} executors on {len({_host(e) for e in g})} hosts, "
            f"{'executor ' + ', '.join(on) if on else 'none'} on {host.split('.')[0]}")


def _same_conf(c: Ctx) -> str:
    diff = _conf_diff(c)
    props = dict(c.get("environment")["sparkProperties"])
    spec = props.get("spark.speculation", "not set")
    return f"{'no property differs' if not diff else diff}; spark.speculation: {spec}"


def _slow_startup_log(c: Ctx) -> str:
    """The slow executor's stderr against a healthy one's (executor 1): JVM start to registration, and the
    CodeGenerator times of its first tasks."""
    import requests

    def stats(e: dict) -> tuple[float, float]:
        html = requests.get(e["executorLogs"]["stderr"].replace("start=-4096", "start=0"), timeout=60).text
        start = re.search(r"(\d\d:\d\d:\d\d) INFO CoarseGrainedExecutorBackend: Started daemon", html).group(1)
        reg = re.search(r"(\d\d:\d\d:\d\d) INFO YarnCoarseGrainedExecutorBackend: Successfully registered", html)
        h1, m1, s1 = map(int, start.split(":"))
        h2, m2, s2 = map(int, reg.group(1).split(":"))
        gen = [float(x) for x in re.findall(r"Code generated in ([0-9.]+) ms", html)]
        return (h2 * 3600 + m2 * 60 + s2) - (h1 * 3600 + m1 * 60 + s1), max(gen)
    ex = _slow_executor(c)
    one = next(e for e in _executors(c) if e["id"] == "1")
    (a, ga), (b, gb) = stats(ex), stats(one)
    return (f"executor {ex['id']}: {a:.0f} s from 'Started daemon' to 'Successfully registered with driver', "
            f"slowest 'Code generated in' {ga:,.0f} ms; executor 1: {b:.0f} s, {gb:,.0f} ms")


S09 = {
    "E1": lambda c: f"{_app_duration(c.runs['last_good'])} → {_app_duration(c)}",
    "E2": _stage_durations,
    "E3": _straggler_durations,
    "E4": _normal_input,
    "E5": _longest_tasks,
    "E6": _stage2_by_executor,
    "E7": _every_stage,
    "E8": _executors_tab,
    "E9": _late_registration,
    "E10": _gc_slow,
    "E11": _rack_neighbours,
    "E12": _last_good_hosts,
    "E13": _same_conf,
    "E14": _slow_startup_log,
}

# ---------------------------------------------------------------------------- scenario 05 (missed broadcast join)


def _below(q: dict, node: dict, name: str) -> dict:
    """Follow a node's first input down the plan graph (through codegen wrappers, sorts, reads) to `name`."""
    by_id = {n["nodeId"]: n for n in q["nodes"]}
    while node["nodeName"] != name:
        node = by_id[next(e["fromId"] for e in q["edges"] if e["toId"] == node["nodeId"])]
    return node


def _join_inputs(q: dict, join: dict) -> list[dict]:
    """The nodes feeding a join (its two sides), in the order of the graph's edges."""
    by_id = {n["nodeId"]: n for n in q["nodes"]}
    return [by_id[e["fromId"]] for e in q["edges"] if e["toId"] == join["nodeId"]]


def _page_scan(c: Ctx) -> dict:
    return min(_nodes(c, 0, "Scan parquet "), key=_rows)


def _joins(c: Ctx) -> str:
    def names(run: Ctx) -> str:
        q = _sql(run, 0)
        joins = sorted((n for n in q["nodes"] if n["nodeName"].endswith("Join")), key=lambda n: -n["nodeId"])
        return " + ".join(n["nodeName"] for n in joins)  # leaf side first: the page catalog join
    return f"{names(c.runs['last_good'])} → {names(c)}"


def _page_catalog_scan(c: Ctx) -> str:
    def row(run: Ctx) -> str:
        s = _page_scan(run)
        n = int(_metric(s, "number of files read"))
        return f"{n} file{'s' if n != 1 else ''}, {_metric(s, 'size of files read')}, {_rows(s):,} rows"
    return f"{row(c.runs['last_good'])} → {row(c)}"


def _page_catalog_exchange(c: Ctx) -> str:
    (b,) = _nodes(c.runs["last_good"], 0, "BroadcastExchange")
    pages = _page_scan(c)
    ex = next(x for x in _nodes(c, 0, "Exchange") if _rows_metric(x, "records read") == _rows(pages))
    return (f"BroadcastExchange: {_metric(b, 'data size')}, {_rows(b):,} rows → Exchange: {_metric(ex, 'data size')} "
            f"in {_metric(ex, 'number of partitions')} partitions")


def _page_join_events_exchange(c: Ctx) -> str:
    """The Exchange on the events side of the page catalog's SortMergeJoin (the incident's first join)."""
    q = _sql(c, 0)
    smj = max((n for n in q["nodes"] if n["nodeName"] == "SortMergeJoin"), key=lambda n: n["nodeId"])  # leaf side
    pages = _page_scan(c)
    ex = next(x for x in (_below(q, k, "Exchange") for k in _join_inputs(q, smj))
              if _rows_metric(x, "records read") != _rows(pages))
    return (f"{_rows_metric(ex, 'records read'):,} records, data size {_metric(ex, 'data size')}, "
            f"shuffle bytes written {_metric(ex, 'shuffle bytes written')}")


def _event_stage_writes(c: Ctx) -> str:
    g = c.runs["last_good"]
    big = 50 << 30

    def row(run: Ctx) -> str:
        st = [s for s in run.stages if s["status"] == "COMPLETE" and s["shuffleWriteBytes"] > big]
        return " + ".join(f"stage {s['stageId']} {fmt_bytes(s['shuffleWriteBytes'])}"
                          for s in sorted(st, key=lambda s: s["stageId"]))
    return f"{row(g)} → {row(c)}"


def _longest_page_join_tasks(c: Ctx) -> str:
    tasks = sorted(c.hs.all_tasks(c.app_id, 5), key=lambda t: -t["duration"])[:5]

    def read(t: dict) -> int:
        m = t["taskMetrics"]["shuffleReadMetrics"]
        return m["remoteBytesRead"] + m["localBytesRead"]
    execs = {t["executorId"] for t in tasks}
    return (", ".join(f"{fmt_duration(t['duration'])} ({fmt_bytes_js(read(t))})" for t in tasks)
            + f", on {len(execs)} different executors")


def _page_join_spill(c: Ctx) -> str:
    st = c.stage(5)
    n = sum(1 for t in c.hs.all_tasks(c.app_id, 5) if t["taskMetrics"]["diskBytesSpilled"] > 0)
    return (f"{fmt_bytes(st['memoryBytesSpilled'])} / {fmt_bytes(st['diskBytesSpilled'])}, "
            f"from {n} of {st['numTasks']:,} tasks")


def _same_events(c: Ctx) -> str:
    good = max(_nodes(c.runs["last_good"], 0, "Scan parquet "), key=_rows)
    bad = max(_nodes(c, 0, "Scan parquet "), key=_rows)
    g, b = _rows(good), _rows(bad)
    return f"{g:,} → {b:,} rows ({100 * (b - g) / g:+.1f} %)"


def _straggler_spills(c: Ctx) -> str:
    """The longest task's spill lines in its executor's stderr (through the NodeManager log link). The
    ShuffleExternalSorter lines name the task and its thread; the UnsafeExternalSorter lines of that thread
    between the task's start and end are its join sort's."""
    import requests

    t = _straggler(c, 5)
    ex = next(e for e in c.get("allexecutors") if e["id"] == t["executorId"])
    html = requests.get(ex["executorLogs"]["stderr"].replace("start=-4096", "start=0"), timeout=60).text
    lines = html.splitlines()
    start = next(i for i, ln in enumerate(lines) if f"(TID {t['taskId']})" in ln and "Running task" in ln)
    end = next(i for i, ln in enumerate(lines) if f"(TID {t['taskId']})" in ln and "Finished task" in ln)
    shuffle = [re.search(r"on Thread (\d+) spilling sort data of (.+?) to disk", ln)
               for ln in lines[start:end] if f"ShuffleExternalSorter: Task {t['taskId']} on Thread" in ln]
    thread = shuffle[0].group(1)
    sort = [re.search(r"spilling sort data of (.+?) to disk", ln) for ln in lines[start:end]
            if f"UnsafeExternalSorter: Thread {thread} spilling" in ln]
    return (f"TID {t['taskId']} (Thread {thread}): {len(sort)} x UnsafeExternalSorter ({sort[0].group(1)}), "
            f"then {len(shuffle)} x ShuffleExternalSorter ({shuffle[0].group(2)})")


def _page_join_rows(c: Ctx) -> str:
    """Rows into the page catalog's SortMergeJoin from the events side, and rows out of it."""
    q = _sql(c, 0)
    smj = max((n for n in q["nodes"] if n["nodeName"] == "SortMergeJoin"), key=lambda n: n["nodeId"])
    pages = _page_scan(c)
    ex = next(x for x in (_below(q, k, "Exchange") for k in _join_inputs(q, smj))
              if _rows_metric(x, "records read") != _rows(pages))
    return f"{_rows_metric(ex, 'records read'):,} events in → {_rows(smj):,} rows out"


def _customer_join(c: Ctx) -> str:
    """The customers join + write: stage 5 of the last good run, stage 10 of the incident."""
    return f"{_stage_row(c.runs['last_good'], 5)} → {_stage_row(c, 10)}"


S05 = {
    "E1": lambda c: f"{_app_duration(c.runs['last_good'])} → {_app_duration(c)}",
    "E2": lambda c: f"{_sql_durations(c.runs['last_good'])} → {_sql_durations(c)}",
    "E3": lambda c: _stage_row(c, 5),
    "E4": lambda c: _durations(c, 5),
    "E5": lambda c: _shuffle_read_med_max(c, 5),
    "E6": _longest_page_join_tasks,
    "E7": _page_join_spill,
    "E8": _joins,
    "E9": _page_catalog_scan,
    "E10": _page_catalog_exchange,
    "E11": _event_stage_writes,
    "E12": _page_join_events_exchange,
    "E13": _conf_diff,
    "E14": _customer_join,
    "E15": _same_events,
    "E16": _health,
    "E17": _straggler_spills,
    "E18": _page_join_rows,
    "E19": _tail_wait,
}

# ---------------------------------------------------------------------------- scenario 03

PMEM = re.compile(r"Container killed by YARN for exceeding physical memory limits\. ([0-9.]+) GB of ([0-9.]+) GB")


def _attempt_durations(c: Ctx) -> str:
    att = sorted(c.hs.get(f"applications/{c.app_id}")["attempts"], key=lambda a: int(a["attemptId"]))
    return ", ".join(f"attempt {a['attemptId']}: {fmt_duration(a['duration'])}" for a in att)


def _failed_job(a: Ctx) -> dict:
    return next(j for j in a.get("jobs") if j["status"] == "FAILED")


def _failed_jobs(c: Ctx) -> str:
    out = []
    for att, a in c.attempts.items():
        jobs = a.get("jobs")
        j = _failed_job(a)
        out.append(f"attempt {att}: {len(jobs)} jobs, job {j['jobId']} FAILED ({j['numCompletedTasks']:,}/"
                   f"{j['numTasks']:,} tasks succeeded, {j['numFailedTasks']} failed, {j['numKilledTasks']} killed)")
    return "; ".join(out)


def _abort_reason(c: Ctx) -> str:
    """The failed stage's Failure Reason, first line, as the Stages tab shows it."""
    a = c.attempts["1"]
    st = next(s for s in a.stages if s["status"] == "FAILED")
    return st["failureReason"].split("\n")[0]


def _failed_task_reasons(c: Ctx) -> str:
    out = []
    for att, a in c.attempts.items():
        st = next(s for s in a.stages if s["status"] == "FAILED")
        tasks = a.tasks(st["stageId"])
        failed = [t for t in tasks if t["status"] == "FAILED"]
        pmem = sum(1 for t in failed if PMEM.search(t.get("errorMessage", "")))
        killed = [t for t in tasks if t["status"] == "KILLED"]
        stage_cancelled = sum(1 for t in killed if "Stage cancelled" in t.get("errorMessage", ""))
        out.append(f"attempt {att}: {len(failed)} FAILED, {pmem} of them pmem kills; {len(killed)} KILLED by "
                   f"'Stage cancelled' ({stage_cancelled})")
    return "; ".join(out)


def _retries(c: Ctx) -> str:
    a = c.attempts["1"]
    st = next(s for s in a.stages if s["status"] == "FAILED")
    per: dict[int, int] = {}
    for t in a.tasks(st["stageId"]):
        if t["status"] == "FAILED":
            per[t["index"]] = per.get(t["index"], 0) + 1
    counts = collections.Counter(per.values())
    return ", ".join(f"{counts[k]} {'index' if counts[k] == 1 else 'indices'} failed {k}x" for k in sorted(counts))


def _executor_churn(c: Ctx) -> str:
    out = []
    for att, a in c.attempts.items():
        ex = [e for e in a.get("allexecutors") if e["id"] != "driver"]
        top = max(int(e["id"]) for e in ex)
        dead = [e for e in ex if not e["isActive"]]
        out.append(f"attempt {att}: executors 1–{top}, {len(ex) - len(dead)} active at the end, "
                   f"{len(dead)} dead listed")
    return "; ".join(out)


def _dead_reasons(c: Ctx) -> str:
    used = []
    for a in c.attempts.values():
        for e in a.get("allexecutors"):
            m = PMEM.search(e.get("removeReason") or "")
            if m:
                used.append(float(m.group(1)))
                limit = m.group(2)
    return f"every dead executor: pmem kill, {min(used):.1f}–{max(used):.1f} GB of {limit} GB"


def _heap_peaks(c: Ctx) -> str:
    def peak(a: Ctx) -> float:
        return max(e["peakMemoryMetrics"]["JVMHeapMemory"] for e in a.get("allexecutors")
                   if e["id"] != "driver" and e.get("peakMemoryMetrics"))
    return (f"last good {fmt_bytes(peak(c.runs['last_good']))}, incident {fmt_bytes(peak(c.attempts['1']))} "
            f"/ {fmt_bytes(peak(c.attempts['2']))} (heap 16 GiB)")


def _dist_diff(c: Ctx) -> str:
    good = dict(c.runs["last_good"].get("environment")["sparkProperties"])
    bad = dict(c.attempts["1"].get("environment")["sparkProperties"])
    diff = sorted(k for k in set(good) | set(bad) if good.get(k) != bad.get(k)
                  and not k.startswith(("spark.app.", "spark.yarn.app.", "spark.driver.port", "spark.driver.host"))
                  and "PROXY_URI_BASES" not in k and "log.dir" not in k)
    over = bad.get("spark.executor.memoryOverhead", "not set")
    return "; ".join(f"{k}: {bad.get(k, 'not set').rsplit('/', 1)[-1]}" for k in diff) + \
        f"; spark.executor.memoryOverhead: {over}"


def _udf_in_plan(c: Ctx) -> str:
    """The SQL tab, query 0: the events-side Project's description in the plan graph (node details), and the
    events scan's ReadSchema in the plan text (Details)."""
    def page(a: Ctx) -> str:
        base = a.hs.base.removesuffix("/api/v1")
        return html.unescape(requests.get(f"{base}/history/{a.app_id}/{a.attempt or '1'}/SQL/execution/?id=0",
                                          timeout=60).text)

    def read_schema(a: Ctx) -> str:
        text = a.get("sql/0", planDescription="true")["planDescription"]
        return re.search(r"\(1\) Scan parquet.*?ReadSchema: (struct<[^>]*>)", text, re.S).group(1)

    udf = re.findall(r"geoip_city\(ip_address#\d+\) AS geo_city#\d+", page(c.attempts["1"]))
    none = not re.search(r"geoip_city\(", page(c.runs["last_good"]))
    ip = ("ip_address" in read_schema(c.attempts["1"]), "ip_address" in read_schema(c.runs["last_good"]))
    return (f"incident: {udf[0]}; last good: {'none' if none else 'there too'}; ip_address in the events "
            f"scan's ReadSchema: incident {'yes' if ip[0] else 'no'}, last good {'yes' if ip[1] else 'no'}")


def _stderr(e: dict) -> str:
    return requests.get(e["executorLogs"]["stderr"].replace("start=-4096", "start=0"), timeout=60).text


def _killed_executor_log(c: Ctx) -> str:
    """The executor that ran the last straw (the abort's most recent failure)."""
    a = c.attempts["1"]
    eid = re.search(r"executor (\d+)\): ExecutorLostFailure", _abort_reason(c)).group(1)
    e = next(x for x in a.get("allexecutors") if x["id"] == eid)
    log = _stderr(e)
    tasks = len(re.findall(r"INFO Executor: Running task ", log))
    opened = len(re.findall(r"GeoIpCity: Opened IP database \./ip-intel-2026q4\.mmdb \(2\.0 GiB\) in MEMORY_MAPPED",
                            log))
    last = log.strip().splitlines()
    term = next((i for i, x in enumerate(last) if "RECEIVED SIGNAL TERM" in x), None)
    return (f"executor {eid}: {tasks} tasks, {opened} 'Opened IP database ... (2.0 GiB) in MEMORY_MAPPED mode', "
            f"{'ends with RECEIVED SIGNAL TERM' if term is not None else 'no SIGTERM'}")


def _am_logs(c: Ctx) -> str:
    out = []
    for att, a in c.attempts.items():
        d = next(e for e in a.get("allexecutors") if e["id"] == "driver")
        err = _stderr(d)
        final = re.search(r"Final app status: (\w+), exitCode: (\d+)", err)
        unreg = "Unregistering ApplicationMaster with FAILED" in err
        out.append(f"attempt {att}: Final app status: {final.group(1)}, exitCode: {final.group(2)}"
                   + (", Unregistering ApplicationMaster with FAILED" if unreg else ", no unregister"))
    return "; ".join(out)


def _am_stdout(c: Ctx) -> str:
    d = next(e for e in c.attempts["1"].get("allexecutors") if e["id"] == "driver")
    out = html.unescape(requests.get(d["executorLogs"]["stdout"].replace("start=-4096", "start=0"), timeout=60).text)
    line = re.search(r'File ".*/(daily_enrichment\.py)", line (\d+), in <module>', out)
    err = re.search(r"py4j\.protocol\.Py4JJavaError: (An error occurred while calling o\d+\.parquet\.)", out)
    return f"{line.group(1)} line {line.group(2)}: Py4JJavaError: {err.group(1)}"


S03 = {
    "E1": lambda c: f"{_app_duration(c.runs['last_good'])} → {_attempt_durations(c)}",
    "E2": _failed_jobs,
    "E3": _abort_reason,
    "E4": _failed_task_reasons,
    "E5": _retries,
    "E6": _executor_churn,
    "E7": _dead_reasons,
    "E8": _heap_peaks,
    "E9": _dist_diff,
    "E10": _udf_in_plan,
    "E11": _killed_executor_log,
    "E12": _am_logs,
    "E13": _am_stdout,
}


EVIDENCE = {"00-healthy-baseline": S00, "01-join-skew": S01, "02-shuffle-partitions-spill": S02,
            "03-memory-overhead-kills": S03, "05-missed-broadcast-join": S05, "09-bad-node-straggler": S09}


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
