"""Scenario 00 in the real Spark 3.5.9 History Server: replay, pages, and the solution's evidence numbers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

from sparksim.apps import daily_enrichment

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence  # noqa: E402
import hs_api  # noqa: E402

SCENARIO = "00-healthy-baseline"


@pytest.mark.integration
def test_scenario00_in_history_server(tmp_path):
    from conftest import run_history_server

    path = daily_enrichment.generate(tmp_path)
    app_id = evidence.SCENARIOS[SCENARIO][0]
    assert path.name == f"{app_id}_1"
    with run_history_server(tmp_path, 1, log_name="hs-scenario00-test.log") as url:
        hs = hs_api.HistoryServer(url)
        assert hs_api.validate(hs, tmp_path) == 0

        # Every number quoted in solution.md, recomputed from the REST API.
        want = evidence.solution_values(SCENARIO)
        got = evidence.compute(hs, SCENARIO)
        assert set(want) == set(got), "solution.md evidence ids and scripts/evidence.py disagree"
        mismatches = {k: (want[k], got[k]) for k in got if want[k] != got[k]}
        assert not mismatches, f"solution.md is stale (solution, server): {mismatches}"

        # The pages a learner opens render without errors.
        base = f"{url}/history/{app_id}/1"
        for page in ("jobs/", "jobs/job/?id=3", "stages/", "stages/stage/?id=2&attempt=0",
                     "stages/stage/?id=5&attempt=0", "executors/", "environment/", "storage/"):
            r = requests.get(f"{base}/{page}", timeout=120)
            assert r.status_code == 200, page
    log = (ROOT / "build" / "hs-scenario00-test.log").read_text()
    assert "Exception" not in log, "History Server logged an exception while replaying"
