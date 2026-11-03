"""JVM collection iteration orders reproduce every order Spark wrote in the reference logs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sparksim.eventlog.ordering import (
    java_properties_order,
    java_string_hash,
    scala_int_hashset_order,
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
    assert seen == 34


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
