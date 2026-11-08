"""Scenario 09 (bad-node straggler) without the History Server: the only cause is one CPU-throttled
NodeManager, the symptoms (the executor YARN put on it is the slowest in every stage, with normal inputs,
normal GC, no spill, no failures) follow from it, the night before was unaffected only because no container
landed on that host, and both fixes the solution recommends bring the job back. The REST-checked evidence is
in `test_scenario09_hs.py`."""

from __future__ import annotations

import copy
import dataclasses
import statistics

import pytest

from sparksim.engine.emit import build_events
from sparksim.engine.runtime import AppRun
from sparksim.eventlog import event_to_line
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict

SCENARIO = "09-bad-node-straggler"
HOST = "wn063.hadoop.prod.internal"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return AppRun(sc.spec("incident")).run()


@pytest.fixture(scope="module")
def last_good(sc):
    return AppRun(sc.spec("last_good")).run()


def with_fix(conf: dict):
    """The incident run, submitted with `conf` added."""
    raw = copy.deepcopy(load_raw(SCENARIO))
    raw["runs"]["incident"]["faults"].append({"type": "config", "set": conf})
    return AppRun(scenario_from_dict(raw).spec("incident")).run()


def app_minutes(run) -> float:
    return (run.app_end - run.t0) / 60_000


def test_the_cause_is_one_degraded_host(sc):
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.sizing == bad.sizing and good.conf == bad.conf
    assert dict(bad.conf).get("spark.speculation") is None  # Spark's default: off
    (f,) = bad.cluster.host_faults
    assert (f.host, f.cpu, f.disk, f.network) == (HOST, 0.05, 1.0, 1.0)
    assert dataclasses.replace(bad.cluster, host_faults=()) == good.cluster
    assert bad.native_memory_per_task == 0 and not bad.executor_kills
    assert all(not ks.hot for i in bad.inputs for ks in i.keys.values())


def test_the_night_before_never_touched_the_node(sc, last_good):
    """wn063 was already degraded on 2026-10-27; with the fault the last good run is byte for byte the same."""
    assert all(e.host.name != HOST for e in last_good.executors) and last_good.am_host.name != HOST
    spec = sc.spec("last_good")
    (fault,) = sc.spec("incident").cluster.host_faults
    faulted = dataclasses.replace(spec, cluster=dataclasses.replace(spec.cluster, host_faults=(fault,)))
    a = [event_to_line(e) for e in build_events(AppRun(faulted).run())]
    b = [event_to_line(e) for e in build_events(AppRun(spec).run())]
    assert a == b


def test_one_executor_is_slow_in_every_stage(incident, last_good):
    (slow,) = [e for e in incident.executors if e.host.name == HOST]
    assert slow.id == "19" and incident.am_host.name != HOST
    others = [e for e in incident.executors if e is not slow]
    # its JVM starts ~20x slower: it registers long after the others
    assert slow.registered_at - max(e.registered_at for e in others) > 60_000
    for sid, st in incident.stages.items():
        if st.skipped or not st.tasks:
            continue
        mine = [t for t in st.tasks if t.executor is slow]
        if not mine:
            continue
        rest = [t.duration for t in st.tasks if t.executor is not slow]
        longest = sorted(st.tasks, key=lambda t: -t.duration)[:len(mine)]
        assert set(map(id, longest)) == set(map(id, mine)), sid  # its tasks are the stage's longest
        assert statistics.median(t.duration for t in mine) > 10 * statistics.median(rest), sid
    # not the data: the longest scan task read an ordinary split
    scan = incident.stages[2]
    longest = max(scan.tasks, key=lambda t: t.duration)
    inputs = sorted(t.result.metrics.input.bytes_read for t in scan.tasks)
    assert longest.executor is slow and longest.result.metrics.input.bytes_read < inputs[int(0.9 * len(inputs))]
    # nothing else is wrong
    recs = list(incident.task_records.values())
    assert all(t.ok for t in recs) and not any(t.result.metrics.memory_bytes_spilled for t in recs)
    assert not any(e.removed_at for e in incident.executors)
    gc = {e.id: 0 for e in incident.executors}
    tt = {e.id: 0 for e in incident.executors}
    for t in recs:
        gc[t.executor.id] += t.result.metrics.jvm_gc_time
        tt[t.executor.id] += t.duration
    assert gc["19"] / tt["19"] < 0.04  # GC share like everyone's: everything on the host is slow, GC included
    n = {e.id: sum(1 for t in recs if t.executor is e) for e in incident.executors}
    assert n["19"] * 5 < min(v for k, v in n.items() if k != "19")
    assert app_minutes(incident) > 1.5 * app_minutes(last_good)


def test_excluding_the_node_brings_the_job_back(last_good):
    run = with_fix({"spark.yarn.exclude.nodes": HOST})
    assert len(run.executors) == 40 and all(e.host.name != HOST for e in run.executors)
    assert app_minutes(run) < 1.03 * app_minutes(last_good)


def test_speculation_routes_around_the_node(last_good, incident):
    run = with_fix({"spark.speculation": "true"})
    recs = list(run.task_records.values())
    spec = [t for t in recs if t.speculative]
    won = [t for t in spec if t.ok]
    assert 15 <= len(spec) <= 30 and len(won) >= 0.75 * len(spec)
    assert all(t.executor.host.name != HOST for t in spec)
    killed = [t for t in recs if t.killed]
    assert {t.killed for t in killed} <= {"another attempt succeeded", "Stage finished",
                                          "Finish but did not commit due to another attempt succeeded"}
    assert app_minutes(run) < 1.05 * app_minutes(last_good) < app_minutes(incident)
    # the price: what the killed attempts ran, a few minutes against the job's ~13 h of task time
    wasted = sum(t.duration for t in killed)
    total = sum(t.duration for t in recs)
    assert wasted < 0.02 * total and 12 * 3_600_000 < total < 14 * 3_600_000
