"""Scenario 09 in the real Spark 3.5.9 History Server: both runs replay, the pages a learner opens render,
and every number `solution.md` quotes matches the REST API (and the slow executor's container log)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

from sparksim.scenario import load_scenario

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence  # noqa: E402
import hs_api  # noqa: E402

SCENARIO = "09-bad-node-straggler"
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    from conftest import run_history_server, run_nm_logs

    sc = load_scenario(SCENARIO)
    base = tmp_path_factory.mktemp("scenario09")
    ev_dir, logs_dir = base / "event-logs", base / "yarn-logs"
    ev_dir.mkdir()
    paths = sc.generate(ev_dir, logs_dir)
    assert [p.name for p in paths] == [f"{sc.app_id(r)}_1" for r in ("last_good", "incident")]
    with run_nm_logs(logs_dir) as nm_url:
        log_url = f"{nm_url}/node/containerlogs/{{{{CONTAINER_ID}}}}/{{{{USER}}}}/{{{{FILE_NAME}}}}?start=-4096"
        opts = f"-Dspark.history.custom.executor.log.url={log_url}"
        with run_history_server(ev_dir, 2, log_name="hs-scenario09-test.log", extra_opts=opts) as url:
            yield url, sc, ev_dir
    log = (ROOT / "build" / "hs-scenario09-test.log").read_text()
    assert "Exception" not in log, "History Server logged an exception while replaying"


def test_scenario09_in_history_server(stack):
    url, sc, ev_dir = stack
    hs = hs_api.HistoryServer(url)
    assert hs_api.validate(hs, ev_dir) == 0

    want = evidence.solution_values(SCENARIO)
    got = evidence.compute(hs, SCENARIO)
    assert set(want) == set(got), "solution.md evidence ids and scripts/evidence.py disagree"
    mismatches = {k: (want[k], got[k]) for k in got if want[k] != got[k]}
    assert not mismatches, f"solution.md is stale (solution, server): {mismatches}"

    for role in sc.roles:
        base = f"{url}/history/{sc.app_id(role)}/1"
        for page in ("jobs/", "jobs/job/?id=2", "stages/", "stages/stage/?id=2&attempt=0",
                     "stages/stage/?id=5&attempt=0", "executors/", "environment/", "SQL/", "SQL/execution/?id=0"):
            r = requests.get(f"{base}/{page}", timeout=120)
            assert r.status_code == 200, (role, page)
