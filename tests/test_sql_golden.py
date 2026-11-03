"""The SQL layer against real Spark 3.5.9 output.

* `ref01-daily-etl-aqe` execution 0, re-planned with sparksim's DataFrame API (`sql_ref01_model.py`):
  every plan event (SQLExecutionStart + 4 AQE updates) must match the real log, explain text and
  SparkPlanInfo, accumulator ids included. Only `[plan_id=N]` is normalized (a JVM-wide counter of
  every SparkPlan Catalyst instantiates).
* RDD scopes and RDD ids of the same execution's stages.
* Every operator's SQL metric definitions (names, types, allocation order, JSON order) against every
  node of the same kind in all reference logs.
"""

from __future__ import annotations

import json
import random
import re
from pathlib import Path

import pytest
import sql_ref01_model as ref01

from sparksim.sql import nodes as n
from sparksim.sql.expr import Attr

REF = Path(__file__).resolve().parents[1] / "reference" / "event-logs"


def _norm(s: str) -> str:
    return re.sub(r"plan_id=\d+", "plan_id=N", s)


def _plan_events(log: str, execution: int | None = None):
    for line in (REF / log).read_text().splitlines():
        d = json.loads(line)
        if d["Event"].endswith(("SQLExecutionStart", "SQLAdaptiveExecutionUpdate")):
            if execution is None or d["executionId"] == execution:
                yield d


@pytest.fixture(scope="module")
def ours():
    return ref01.plan_events()


def test_ref01_plan_events_count(ours):
    assert len(ours) == len(list(_plan_events("ref01-daily-etl-aqe", 0))) == 5


@pytest.mark.parametrize("i", range(5))
def test_ref01_physical_plan_description(ours, i):
    real = list(_plan_events("ref01-daily-etl-aqe", 0))[i]
    assert _norm(ours[i][0]) == _norm(real["physicalPlanDescription"])


@pytest.mark.parametrize("i", range(5))
def test_ref01_spark_plan_info_and_metric_ids(ours, i):
    real = list(_plan_events("ref01-daily-etl-aqe", 0))[i]
    got = json.dumps(ours[i][1].to_json(), separators=(",", ":"))
    assert _norm(got) == _norm(json.dumps(real["sparkPlanInfo"], separators=(",", ":")))


def _real_stage_rdds() -> dict[int, list[tuple]]:
    out = {}
    for line in (REF / "ref01-daily-etl-aqe").read_text().splitlines():
        d = json.loads(line)
        if d["Event"] == "SparkListenerStageSubmitted":
            si = d["Stage Info"]
            out[si["Stage ID"]] = sorted((r["RDD ID"], r["Name"], json.loads(r["Scope"])["id"],
                                          json.loads(r["Scope"])["name"], tuple(r["Parent IDs"]))
                                         for r in si["RDD Info"])
    return out


def _ours(lineage) -> list[tuple]:
    infos = lineage.rdd_infos("cs", 6, random.Random(0))
    return sorted((i.rdd_id, i.name, json.loads(i.scope)["id"], json.loads(i.scope)["name"], tuple(i.parent_ids))
                  for i in infos)


def test_ref01_rdd_scopes_single_thread_rounds():
    """Stages materialized alone (orders exchange, join exchange, write) match exactly."""
    _, rounds = ref01.lineage_rounds()
    real = _real_stage_rdds()
    (orders,) = rounds[1].values()
    (join,) = rounds[2].values()
    assert _ours(orders) == real[5]
    assert _ours(join) == real[8]
    assert _ours(rounds[3]["final"]) == real[12]
    assert join.top_scope == (44, "Exchange")


def test_ref01_rdd_scopes_concurrent_round():
    """The broadcast and the customers exchange run on two threads: their ids interleave (seeded), but each
    stage has the real shape and together they use exactly the real ids."""
    real = _real_stage_rdds()
    for seed in range(5):
        aqe, rounds = ref01.lineage_rounds(seed)
        bcast, cust = rounds[0][id(aqe.stages[0])], rounds[0][id(aqe.stages[1])]
        shape = lambda rs: [(name, scope_name) for _, name, _, scope_name, _ in rs]  # noqa: E731
        assert shape(_ours(bcast)) == shape(real[4]) and shape(_ours(cust)) == shape(real[3])
        ids = {r[0] for r in _ours(bcast) + _ours(cust)}
        assert ids == {r[0] for r in real[3] + real[4]}
        assert bcast.top_scope == (15, "BroadcastExchange")


# --------------------------------------------------------------------------- metric definitions vs every reference


def _node(name: str, metric_names: list[str]) -> n.SparkPlan | None:
    a = Attr("x", "bigint", 0)
    if name == "Scan parquet ":
        rel = n.Relation(["p"], [a], [a] if "number of partitions read" in metric_names else [])
        return n.FileSourceScan(rel, [a], [], [], [])
    if name.startswith("WholeStageCodegen"):
        return n.WholeStageCodegen(n.InputAdapter([]), 1)
    if name == "AQEShuffleRead":
        if "number of skewed partitions" in metric_names or "partition data size" not in metric_names:
            return None  # skew splits and local reads are not modelled yet
        stage = n.QueryStage(0, None, False)
        ranges = [(0, 2)] if "number of coalesced partitions" in metric_names else [(0, 1)]
        return n.AQEShuffleRead(stage, ranges)
    simple = {
        "ColumnarToRow": lambda: n.ColumnarToRow([]),
        "Filter": lambda: n.Filter(a, None),
        "Project": lambda: n.Project([], None),
        "HashAggregate": lambda: n.HashAggregate([], [], [], None),
        "Sort": lambda: n.Sort([], None),
        "SortMergeJoin": lambda: n.SortMergeJoin([], [], "Inner", None, None),
        "BroadcastHashJoin": lambda: n.BroadcastHashJoin([], [], "Inner", None, None),
        "Exchange": lambda: n.ShuffleExchange([], 1, None, 0),
        "BroadcastExchange": lambda: n.BroadcastExchange([], None, 0),
        "Execute InsertIntoHadoopFsRelationCommand": lambda: n.DataWritingCommand(
            n.WriteCommand("p", "p", [], []), None),
        "WriteFiles": lambda: n.WriteFiles([]),
        "AdaptiveSparkPlan": lambda: n.AdaptiveSparkPlan(None, []),
    }
    return simple[name]() if name in simple else None


def test_metric_definitions_match_every_reference_node():
    checked = 0
    for log in sorted(p.name for p in REF.iterdir()):
        for ev in _plan_events(log):
            stack = [ev["sparkPlanInfo"]]
            while stack:
                p = stack.pop()
                stack.extend(p["children"])
                if p["nodeName"] == "Exchange" and "RoundRobin" in p["simpleString"]:
                    pass  # same metrics as hash partitioning
                node = _node(p["nodeName"], [m["name"] for m in p["metrics"]])
                if node is None:
                    continue
                defs = node.metric_defs()
                json_order = [(d.name, d.type) for d in n.metric_json_order(defs)]
                assert json_order == [(m["name"], m["metricType"]) for m in p["metrics"]], (log, p["nodeName"])
                by_id = [m["name"] for m in sorted(p["metrics"], key=lambda m: m["accumulatorId"])]
                if p["nodeName"] != "Execute InsertIntoHadoopFsRelationCommand" or by_id:
                    assert by_id == [d.name for d in defs], (log, p["nodeName"], "allocation order")
                checked += 1
    assert checked > 300


def test_spark_number_formats():
    assert n.bytes_to_string(1088576) == "1063.1 KiB"
    assert n.bytes_to_string(96_048_048) == "91.6 MiB"
    assert n.bytes_to_string(512) == "512.0 B"
    assert [n.big_decimal_3(x) for x in (576, 96, 5000, 1_000_000, 1_714_018, 99_950, 0)] == \
        ["576", "96", "5.00E+3", "1.00E+6", "1.71E+6", "1.00E+5", "0"]
    assert n.abbreviate("x" * 101) == "x" * 97 + "..."
