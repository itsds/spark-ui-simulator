"""`scenario.yaml`: schema, validation against the workload, units, and every scenario in the repo."""

from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from sparksim.cli import main as cli
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict, scenario_ids
from sparksim.scenario.schema import duration_ms, size_mb


def incident(*faults, **run_changes):
    raw = copy.deepcopy(load_raw("00-healthy-baseline"))
    base = raw["runs"].pop("baseline")
    raw["runs"] = {"last_good": base,
                   "incident": {**base, "app_seq": base["app_seq"] + 1, "started_at": "2026-11-04T02:10:00Z",
                                "faults": list(faults), **run_changes}}
    return raw


HOT = {"type": "key_skew", "dataset": "events", "column": "customer_id", "hot_keys": [{"value": -1, "share": 0.1}]}


def test_every_scenario_in_the_repo_validates():
    ids = scenario_ids()
    assert "00-healthy-baseline" in ids and "01-join-skew" in ids
    for sid in ids:
        sc = load_scenario(sid)
        assert sc.id == sid
        assert all((sc.dir / f).is_file() for f in ("ticket.md", "hints.md", "solution.md"))
    assert cli(["validate"]) == 0


def test_units():
    assert size_mb("16g") == 16384 and size_mb("384m") == 384 and size_mb(2048) == 2048 and size_mb("1.5g") == 1536
    assert duration_ms("6m30s") == 390_000 and duration_ms("+90s") == 90_000 and duration_ms("1h") == 3_600_000
    with pytest.raises(ValueError):
        duration_ms("6 minutes")
    with pytest.raises(ValueError):
        size_mb("lots")


def test_conf_values_as_spark_submit_passes_them():
    spec = load_scenario("00-healthy-baseline").spec("baseline")
    conf = dict(spec.conf)
    assert conf["spark.dynamicAllocation.enabled"] == "false"
    assert conf["spark.sql.shuffle.partitions"] == "2400"
    assert [k for k, _ in spec.conf][:2] == ["spark.yarn.queue", "spark.dynamicAllocation.enabled"]


@pytest.mark.parametrize("raw, match", [
    (incident(), "needs at least one fault"),
    (incident(HOT, started_at="2026-11-01T00:00:00Z"), "starts before"),
    (incident(HOT, app_seq=48213), "own app_seq"),
    (incident({**HOT, "hot_keys": [{"value": -1, "share": 0.6}, {"value": 0, "share": 0.5}]}), "less than 1"),
    (incident({"type": "key_skew", "dataset": "events", "column": "customer_id"}), "hot_keys or zipf"),
    (incident({"type": "slow_node", "host": "wn999.hadoop.prod.internal", "disk": 0.3}), "not in the cluster"),
    (incident({"type": "meteor_strike"}), "does not match any of the expected tags"),
    (incident({"type": "memory_pressure", "native_per_task": "much"}), "not a memory size"),
    (incident({"type": "executor_killed", "executor": "3", "at": "soon"}), "not a duration"),
    (incident(HOT, started_at="2026-11-04T02:10:00"), "time zone"),
])
def test_schema_rejects(raw, match):
    with pytest.raises(ValidationError, match=match):
        scenario_from_dict(raw)


def test_last_good_run_has_no_faults():
    raw = incident(HOT)
    raw["runs"]["last_good"]["faults"] = [HOT]
    with pytest.raises(ValidationError, match="last good run has no faults"):
        scenario_from_dict(raw)


def test_baseline_and_incident_dont_mix():
    raw = incident(HOT)
    raw["runs"]["baseline"] = raw["runs"]["last_good"]
    with pytest.raises(ValidationError, match="either"):
        scenario_from_dict(raw)


@pytest.mark.parametrize("fault, match", [
    ({**HOT, "dataset": "orders"}, "no dataset 'orders'"),
    ({**HOT, "column": "session_id"}, "not a key column"),
    ({**HOT, "hot_keys": [{"value": [-1, 3], "share": 0.1}]}, "has 1 column"),
])
def test_faults_are_checked_against_the_workload(fault, match):
    with pytest.raises(ValueError, match=match):
        scenario_from_dict(incident(fault))


def test_workload_params_and_data_are_checked():
    raw = incident(HOT)
    raw["runs"]["incident"]["params"] = {"date": "2026-11-03"}
    with pytest.raises(ValueError, match="unknown params"):
        scenario_from_dict(raw)
    raw = incident(HOT)
    raw["runs"]["incident"]["data"] = {"clicks": {"files": 10}}
    with pytest.raises(ValueError, match="unknown dataset"):
        scenario_from_dict(raw)
    raw = copy.deepcopy(load_raw("00-healthy-baseline"))
    raw["job"]["workload"] = "nightly_magic"
    with pytest.raises(ValueError, match="unknown workload"):
        scenario_from_dict(raw)


def test_data_overrides_and_skew_reach_the_spec():
    sc = scenario_from_dict(incident(HOT, data={"events": {"files": 4945, "mean_file_size": "280m"}}))
    good, bad = sc.spec("last_good"), sc.spec("incident")
    ev_good, ev_bad = good.inputs[0], bad.inputs[0]
    assert (ev_good.files, ev_bad.files) == (4300, 4945)
    assert ev_bad.mean_file_bytes == 280 * 1024 * 1024
    assert not ev_good.keys["customer_id"].hot
    assert ev_bad.keys["customer_id"].hot[0].value == (-1,) and ev_bad.keys["customer_id"].hot[0].share == 0.1
    assert "dt=2026-11-02" in ev_bad.path  # params unchanged unless the run sets them
