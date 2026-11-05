"""`sparksim prepare` (ADR-032): app fingerprints, shared runs, the manifest, and regenerating only what is
missing or out of date."""

from __future__ import annotations

import copy
import json
import shutil

import pytest

from sparksim.cli import main
from sparksim.prepare import (
    MANIFEST,
    Manifest,
    app_fingerprint,
    app_state,
    generate_app,
    plan_apps,
    prepare,
)
from sparksim.scenario import load_raw, load_scenario, scenario_from_dict


def small(**run_changes):
    """Scenario 00's job on 2 % of its data (a run takes about a second)."""
    raw = copy.deepcopy(load_raw("00-healthy-baseline"))
    raw["runs"]["baseline"].update({"data": {"events": {"files": 86}, "customers": {"files": 10}}, **run_changes})
    return scenario_from_dict(raw)


def test_a_shared_run_is_one_app():
    s00, s01 = load_scenario("00-healthy-baseline"), load_scenario("01-join-skew")
    plans = plan_apps([s00, s01])
    assert [p.app_id for p in plans] == [s00.app_id("baseline"), s01.app_id("incident")]
    assert plans[0].scenario_ids == ["00-healthy-baseline", "01-join-skew"]
    assert plans[1].scenario_ids == ["01-join-skew"]
    assert app_fingerprint(s00, "baseline") == app_fingerprint(s01, "last_good")
    assert app_fingerprint(s01, "last_good") != app_fingerprint(s01, "incident")


def test_fingerprint_follows_what_changes_the_bytes():
    base = small()
    assert app_fingerprint(small(label="another label"), "baseline") == app_fingerprint(base, "baseline")
    assert app_fingerprint(small(seed=1), "baseline") != app_fingerprint(base, "baseline")
    assert app_fingerprint(small(params={"dt": "2026-11-01"}), "baseline") != app_fingerprint(base, "baseline")


def test_two_runs_may_not_claim_one_app_id():
    with pytest.raises(ValueError, match="claimed by two different runs"):
        plan_apps([load_scenario("00-healthy-baseline"), small()])


def test_validate_checks_every_scenario_and_shared_app_ids(capsys):
    assert main(["validate"]) == 0
    assert "FAIL" not in capsys.readouterr().out


def test_prepare_generates_once_then_only_what_is_missing_or_stale(tmp_path):
    sc = small()
    app = sc.app_id("baseline")
    ev, yl = tmp_path / "event-logs", tmp_path / "yarn-logs"
    log: list[str] = []
    assert prepare([sc], ev, yl, log=log.append) == [(app, "generated")]
    path = ev / f"{app}_1"
    assert path.is_file() and (yl / app).is_dir()
    m = Manifest.load(ev)
    assert m.apps[app]["fingerprint"] == app_fingerprint(sc, "baseline")
    assert m.apps[app]["scenarios"] == ["00-healthy-baseline"]
    assert m.progress is None  # cleared when prepare is done
    assert json.loads((ev / MANIFEST).read_text())["version"] == 1
    assert MANIFEST.startswith(".")  # the History Server skips hidden files
    first = path.read_bytes()

    assert prepare([sc], ev, yl, log=log.append) == [(app, "up to date")]
    assert any(line.startswith("up to date") for line in log)

    m.apps[app]["fingerprint"] = "from-an-older-simulator"
    m.save()
    assert app_state(plan_apps([sc])[0], ev, yl, Manifest.load(ev)) == "stale"
    assert prepare([sc], ev, yl, log=log.append) == [(app, "generated")]
    assert path.read_bytes() == first  # deterministic

    shutil.rmtree(yl / app)
    assert prepare([sc], ev, yl, log=log.append) == [(app, "generated")]
    path.unlink()
    assert prepare([sc], ev, yl, log=log.append) == [(app, "generated")]
    assert prepare([sc], ev, yl, force=True, log=log.append) == [(app, "generated")]


def test_a_seed_override_is_not_the_scenarios_run(tmp_path):
    sc = small()
    app = sc.app_id("baseline")
    generate_app(sc, "baseline", tmp_path, None, seed=7)
    assert app not in Manifest.load(tmp_path).apps
    assert app_state(plan_apps([sc])[0], tmp_path, None, Manifest.load(tmp_path)) == "stale"


def test_progress_of_a_dead_prepare_is_ignored(tmp_path):
    m = Manifest.load(tmp_path)
    m.progress = {"pending": ["a"], "generating": "a", "updated": 0}
    m.save()
    assert Manifest.load(tmp_path).active_progress() is None
