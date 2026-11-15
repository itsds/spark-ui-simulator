"""JVM collection iteration orders reproduce every order Spark wrote in the reference logs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sparksim.eventlog.ordering import (
    java_properties_order,
    java_string_hash,
    scala_int_hashset_order,
    scala_long_hashmap_order,
    scala_map_order,
)

REF = Path(__file__).resolve().parents[1] / "reference" / "event-logs"
LOGS = sorted(p for p in REF.iterdir() if p.is_file())


def events(path):
    with open(path) as fh:
        for line in fh:
            yield json.loads(line)


def test_java_string_hash():
    assert java_string_hash("") == 0
    assert java_string_hash("a") == 97
    assert java_string_hash("hello") == 99162322  # "hello".hashCode()
    assert java_string_hash("polygenelubricants") == 0x80000000  # Integer.MIN_VALUE, as unsigned


def test_stage_ids_order():
    seen = 0
    for p in LOGS:
        for e in events(p):
            if e["Event"] == "SparkListenerJobStart":
                ids = e["Stage IDs"]
                # DAGScheduler inserts the final stage first, then parents depth first; with no
                # collisions the insertion order doesn't matter, so feed them sorted.
                assert scala_int_hashset_order(sorted(ids)) == ids, (p.name, e["Job ID"])
                assert [s["Stage ID"] for s in e["Stage Infos"]] == ids
                seen += len(ids) > 1
    assert seen == 43  # ref00–ref12 (ref10/ref11: one log per YARN attempt)


def test_hashset_grows_like_flat_hash_table():
    order = scala_int_hashset_order(range(40))
    assert sorted(order) == list(range(40)) and order != list(range(40))


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_properties_order(path):
    for e in events(path):
        props = e.get("Properties")
        if isinstance(props, dict) and props:
            keys = list(props)
            assert java_properties_order(keys) == keys


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_environment_map_order(path):
    for e in events(path):
        if e["Event"] == "SparkListenerEnvironmentUpdate":
            for section in ("Spark Properties", "Hadoop Properties", "System Properties", "Classpath Entries"):
                keys = list(e[section])
                assert len(keys) > 4
                assert scala_map_order(sorted(keys)) == keys, section


def test_executor_loss_task_end_order():
    """`TaskSetManager.executorLost` walks `taskInfos` (a Scala mutable.HashMap of the task set's task ids in
    launch order): the Resubmitted and ExecutorLostFailure TaskEnds of one executor loss come in its order."""
    checked = 0
    for p in LOGS:
        launched: dict[tuple[int, int], list[int]] = {}
        group: tuple | None = None
        groups = []
        for line in p.read_text().splitlines():
            e = json.loads(line)
            ev = e["Event"]
            if ev == "SparkListenerTaskStart":
                launched.setdefault((e["Stage ID"], e["Stage Attempt ID"]), []).append(e["Task Info"]["Task ID"])
            elif ev == "SparkListenerTaskEnd" and (why := e["Task End Reason"]["Reason"]) in (
                    "Resubmitted", "ExecutorLostFailure"):
                key = (e["Stage ID"], e["Stage Attempt ID"], e["Task Info"]["Executor ID"], why)
                if group is not None and group[0] == key:
                    group[1].append(e["Task Info"]["Task ID"])
                else:
                    group = (key, [e["Task Info"]["Task ID"]], list(launched[key[:2]]))
                    groups.append(group)
            elif ev not in ("SparkListenerExecutorRemoved", "SparkListenerBlockManagerRemoved"):
                group = None
        for _key, tids, keys in groups:
            if len(tids) > 1:
                assert [t for t in scala_long_hashmap_order(keys) if t in tids] == tids, (p.name, _key)
                checked += 1
    assert checked == 23  # ref05, ref10, ref11, ref12
