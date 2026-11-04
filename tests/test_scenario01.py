"""Scenario 01 (join-key skew) without the History Server: the cause is in the data only, and the
symptoms follow from it. The REST-checked evidence is in `test_scenario01_hs.py`."""

from __future__ import annotations

import dataclasses

import pytest

from sparksim.engine.runtime import quantile
from sparksim.model.murmur3 import partition_id
from sparksim.scenario import load_scenario

SCENARIO = "01-join-skew"


@pytest.fixture(scope="module")
def sc():
    return load_scenario(SCENARIO)


@pytest.fixture(scope="module")
def incident(sc):
    return sc.build("incident")


def _fields(spec):
    d = {f.name: getattr(spec, f.name) for f in dataclasses.fields(spec) if f.name not in ("queries", "inputs")}
    d["queries"] = [q.description for q in spec.queries]
    d["inputs"] = [dataclasses.replace(i, keys={}) for i in spec.inputs]
    d["keys"] = [i.keys for i in spec.inputs]
    return d


def test_last_good_run_is_scenario_00(sc):
    """ADR-006: the last good run of the same job is the healthy baseline the learner already knows."""
    assert _fields(sc.spec("last_good")) == _fields(load_scenario("00-healthy-baseline").spec("baseline"))
    assert sc.app_id("last_good") == load_scenario("00-healthy-baseline").app_id("baseline")


def test_the_cause_is_only_in_the_data(sc):
    """Same job, cluster, executors and conf; the incident differs by date, input volume and one hot key."""
    good, bad = sc.spec("last_good"), sc.spec("incident")
    assert good.conf == bad.conf and good.sizing == bad.sizing and good.cluster == bad.cluster
    assert dict(bad.conf)["spark.sql.adaptive.skewJoin.enabled"] == "false"
    g_ev, b_ev = good.inputs[0], bad.inputs[0]
    assert b_ev.files == round(g_ev.files * 1.15)
    gk, bk = g_ev.keys["customer_id"], b_ev.keys["customer_id"]
    assert dataclasses.replace(bk, hot=()) == gk
    assert [(h.value, h.share) for h in bk.hot] == [((-1,), 0.13)]
    assert bad.native_memory_per_task == 0 and not bad.executor_kills and not bad.cluster.host_faults


def test_one_task_carries_the_hot_key(incident):
    run, _ = incident
    st = next(s for s in run.stages.values() if not s.skipped and len(s.spec.shuffle_inputs) == 2)
    assert len(st.tasks) == 2400  # no AQE coalescing: ~130 MiB partitions are above the 64 MiB target
    hot = max(st.tasks, key=lambda t: t.duration)
    assert hot.index == partition_id([-1], ["bigint"], 2400) == 1993
    events = max((run.exchanges[x] for x in st.spec.shuffle_inputs), key=lambda e: e.records.sum())
    share = events.records[:, 1993].sum() / events.records.sum()
    assert share == pytest.approx(0.13 + 0.87 / 2400, abs=0.002)  # the hot key + its share of the other keys
    durs = [t.duration for t in st.tasks]
    assert hot.duration > 1000 * quantile(durs, 0.5)
    others = sorted(durs)[:-1]
    assert max(others) < 3 * quantile(durs, 0.5)
    assert [t for t in st.tasks if t.result.metrics.memory_bytes_spilled] == [hot]
    assert not any(t.failure for t in run.task_records.values())


def test_the_rest_of_the_run_is_healthy(incident):
    """Only the skewed stage is affected: the scans and the aggregations look like the baseline (+15 %)."""
    run, _ = incident
    for st in run.stages.values():
        if st.skipped or len(st.spec.shuffle_inputs) == 2 or len(st.tasks) < 50:
            continue
        durs = [t.duration for t in st.tasks]
        assert max(durs) < 3 * quantile(durs, 0.5), st.stage_id
        assert not any(t.result.metrics.memory_bytes_spilled for t in st.tasks)
    assert not any(e.removed_at for e in run.executors)
