"""Scenario 05 (missed broadcast join) without the History Server: the only cause is one inherited Spark
default (`spark.sql.autoBroadcastJoinThreshold=-1`), the join strategy follows from it, the skew follows from
page popularity that was always in the data, and both fixes the solution recommends bring the job back.
The REST-checked evidence is in `test_scenario05_hs.py`."""

from __future__ import annotations

import copy
import dataclasses
import statistics

import pytest

from sparksim.apps.daily_enrichment import PAGE
from sparksim.engine.emit import build_events
from sparksim.engine.runtime import AppRun, quantile
from sparksim.eventlog import event_to_line
from sparksim.model.shuffle import row_shares
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict

SCENARIO = "05-missed-broadcast-join"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return AppRun(sc.spec("incident")).run()


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def variant(role: str, conf: dict | None = None, params: dict | None = None):
    """A run of the scenario with `conf` submitted on top (after the gateway's defaults) or other params."""
    raw = copy.deepcopy(load_raw(SCENARIO))
    run = raw["runs"][role]
    if conf:
        run.setdefault("faults", []).append({"type": "config", "set": conf})
    run["params"].update(params or {})
    return AppRun(scenario_from_dict(raw).spec(role)).run()


def app_minutes(run) -> float:
    return (run.app_end - run.t0) / 60_000


def final_plan(run, execution: int = 0) -> str:
    """The operator tree of the query's final plan, as the SQL tab's explain prints it."""
    text = [p[1] for _, kind, p in run.log if kind == "sql_update" and p[0].execution_id == execution][-1]
    return text.split("== Final Plan ==")[1].split("== Initial Plan ==")[0]


def _smj_stages(run):
    """Stages that sort-merge join two shuffles, in stage order (the incident: page catalog, then customers)."""
    return sorted((st for st in run.stages.values() if not st.skipped and len(st.spec.shuffle_inputs) == 2),
                  key=lambda st: st.stage_id)


def test_the_cause_is_one_inherited_default(sc):
    """Same job, cluster, executors and data; the script never hinted the page catalog; the incident run
    inherited the platform's new defaults: automatic broadcast off, and a harmless result-size cap."""
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.sizing == bad.sizing and good.cluster == bad.cluster
    assert [dataclasses.replace(i, path="") for i in good.inputs] == [dataclasses.replace(i, path="")
                                                                      for i in bad.inputs]
    g, b = dict(good.conf), dict(bad.conf)
    assert {k: v for k, v in b.items() if g.get(k) != v} == {
        "spark.sql.autoBroadcastJoinThreshold": "-1", "spark.driver.maxResultSize": "512m"}
    assert set(g) <= set(b)
    assert g["spark.sql.adaptive.skewJoin.enabled"] == "false"  # as in every run of this job (Scenario 00)
    for role in ("last_good", "incident"):
        assert sc.model.runs[role].params["broadcast_hint"] is False
    assert bad.native_memory_per_task == 0 and not bad.executor_kills and not bad.cluster.host_faults
    assert all(not ks.hot for i in bad.inputs for ks in i.keys.values())
    # page popularity is a property of the data in both runs, not a fault
    pages = [i.keys["page_id"] for i in (good.inputs[0], bad.inputs[0])]
    assert pages == [PAGE, PAGE] and PAGE.zipf > 0


def test_auto_broadcast_is_the_same_plan_as_the_hint(sc, last_good):
    """Without the hint, the 3 MB catalog is under the default 10 MB threshold: Spark plans exactly the
    broadcast hash join the hint would. The last good run is byte-identical with F.broadcast() added."""
    plan = final_plan(last_good)
    assert "BroadcastHashJoin" in plan and plan.count("SortMergeJoin") == 1
    hinted = variant("last_good", params={"broadcast_hint": True})
    assert [event_to_line(e) for e in build_events(hinted)] == [event_to_line(e) for e in build_events(last_good)]


def test_threshold_minus_one_turns_the_page_join_into_a_sort_merge_join(incident, last_good):
    plan = final_plan(incident)
    assert "BroadcastHashJoin" not in plan and "BroadcastExchange" not in plan
    assert plan.count("SortMergeJoin") == 2  # the page catalog join and the customers join
    explain = [p[1] for _, kind, p in incident.log if kind == "sql_update" and p[0].execution_id == 0][-1]
    assert "hashpartitioning(page_id#" in explain
    # one more stage: the events are shuffled on page_id, joined, and shuffled again on customer_id
    good = [st for st in last_good.stages.values() if not st.skipped and st.query == 0]
    bad = [st for st in incident.stages.values() if not st.skipped and st.query == 0]
    assert len(bad) == len(good) + 1
    assert not any(st.spec.is_broadcast for st in bad) and sum(st.spec.is_broadcast for st in good) == 1

    def shuffled(run):
        return sum(t.result.metrics.shuffle_write.bytes_written for st in run.stages.values() if not st.skipped
                   for t in st.tasks if t.ok)
    assert 1.7 < shuffled(incident) / shuffled(last_good) < 1.9  # the fact table is shuffled twice


def test_page_popularity_skews_only_the_page_join(incident, last_good):
    """The hottest pages land in a few of the 2,400 partitions; a broadcast join never partitioned on them."""
    smj = _smj_stages(incident)
    page_join, customer_join = smj[0], smj[1]
    durs = [t.duration for t in page_join.tasks]
    med = quantile(durs, 0.5)
    longest = sorted(page_join.tasks, key=lambda t: -t.duration)[:5]
    assert longest[0].duration > 150 * med
    assert all(t.duration > 30 * med for t in longest)
    assert len({t.executor.id for t in longest}) == 5  # different executors: the data, not a node
    reads = sorted((t.result.metrics.shuffle_read.records_read for t in page_join.tasks), reverse=True)
    total = sum(reads)
    top_share = row_shares(PAGE, 2400, incident.spec.seed).max()
    assert reads[0] / total == pytest.approx(top_share, rel=0.05)
    assert 0.03 < reads[0] / total < 0.045  # the home page's partition
    spilled = [t for t in page_join.tasks if t.result.metrics.disk_bytes_spilled]
    assert 0 < len(spilled) < 0.05 * len(page_join.tasks)
    # the customers join after it is as even as the night before
    (good_join,) = _smj_stages(last_good)
    for st in (customer_join, good_join):
        d = [t.duration for t in st.tasks]
        assert len(st.tasks) == 2400 and max(d) < 1.6 * quantile(d, 0.5)
        assert not any(t.result.metrics.disk_bytes_spilled for t in st.tasks)
    assert (customer_join.completed - customer_join.submitted) == pytest.approx(
        good_join.completed - good_join.submitted, rel=0.1)
    assert not any(t.failure for t in incident.task_records.values())
    assert not any(e.removed_at for e in incident.executors)


def test_the_job_is_four_times_slower(incident, last_good):
    assert 5.0 < app_minutes(last_good) < 6.0
    assert 20.0 < app_minutes(incident) < 24.0


def test_the_shuffle_writer_buffers_and_spills_like_spark(incident):
    """UnsafeShuffleWriter (2,400 reduce partitions): every map task reports its ShuffleExternalSorter's peak
    in Peak Execution Memory; the home page's task writes ~26 GiB of rows and spills them in the writer too,
    after its join sort has spilled, with the lines a real 3.5.9 executor prints."""
    import re

    from sparksim.yarnlogs.executor import executor_stderr

    scan = next(st for st in incident.stages.values() if st.stage_id == 0)
    for t in scan.tasks[:50]:
        res = t.result
        assert res.shuffle_sorter is not None and not res.shuffle_sorter.spill
        assert res.metrics.peak_execution_memory == round(res.shuffle_sorter.held) > 64 * 2**20
    page_join = _smj_stages(incident)[0]
    hot = max(page_join.tasks, key=lambda t: t.duration)
    sorter = hot.result.shuffle_sorter
    sort_spill = sum(op.spill for op in hot.result.trace)
    assert sorter.spill > 0.8 * sort_spill > 0
    assert hot.result.metrics.memory_bytes_spilled == round(sort_spill) + round(sorter.spill)
    log = executor_stderr(incident, hot.executor).splitlines()
    mine = [ln for ln in log if f"ShuffleExternalSorter: Task {hot.task_id} on Thread" in ln]
    counts = [re.search(r"\((\d+) +(times?) so far\)", ln).groups() for ln in mine]
    assert [int(n) for n, _ in counts] == list(range(len(mine))) and len(mine) > 5
    assert [w for _, w in counts][:3] == ["times", "time", "times"]  # "0  times", "1  time", "2  times"
    thread = re.search(r"on Thread (\d+)", mine[0]).group(1)
    sorts = [i for i, ln in enumerate(log) if f"UnsafeExternalSorter: Thread {thread} spilling" in ln]
    assert sorts and max(sorts) < log.index(mine[0])  # the join's sort spills first, then the writer


def test_shuffle_sorter_model():
    from sparksim.cost.model import _shuffle_sorter

    mib = 2**20
    small = _shuffle_sorter(800_000, 152, 64 * mib, 2.35 * 2**30, 1.0)
    assert small.spill == 0 and small.held == 128 * mib + 800_000 * 16  # 2 pages (119 MiB of records) + pointers
    big = _shuffle_sorter(150_000_000, 152, 64 * mib, 2.35 * 2**30, 1.0)
    assert big.held == 2.35 * 2**30 and 0.9 * 150_000_000 * 156 < big.spill < 150_000_000 * 156


@pytest.mark.parametrize("fix", [
    {"conf": {"spark.sql.autoBroadcastJoinThreshold": "10m"}},  # tonight: the job's own --conf wins
    {"params": {"broadcast_hint": True}},  # durable: F.broadcast(pages), honoured whatever the threshold
])
def test_the_fix_brings_the_broadcast_back(fix, last_good):
    run = variant("incident", **fix)
    plan = final_plan(run)
    assert "BroadcastHashJoin" in plan and plan.count("SortMergeJoin") == 1
    assert app_minutes(run) < app_minutes(last_good) * 1.05
    (join,) = _smj_stages(run)
    assert statistics.median(t.duration for t in join.tasks) < 5_000
