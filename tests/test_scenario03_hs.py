"""Scenario 03 in the real Spark 3.5.9 History Server: the last good run and both YARN attempts of the failed
one replay, the pages a learner opens render (per attempt), and every number `solution.md` quotes matches the
REST API (and the executors' and the ApplicationMaster's container logs)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
import requests

from sparksim.scenario import load_scenario

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence  # noqa: E402
import hs_api  # noqa: E402

SCENARIO = "03-memory-overhead-kills"
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    from conftest import run_history_server, run_nm_logs

    sc = load_scenario(SCENARIO)
    base = tmp_path_factory.mktemp("scenario03")
    ev_dir, logs_dir = base / "event-logs", base / "yarn-logs"
    ev_dir.mkdir()
    paths = sc.generate(ev_dir, logs_dir)
    good, bad = sc.app_id("last_good"), sc.app_id("incident")
    assert [p.name for p in paths] == [f"{good}_1", f"{bad}_1", f"{bad}_2"]
    with run_nm_logs(logs_dir) as nm_url:
        log_url = f"{nm_url}/node/containerlogs/{{{{CONTAINER_ID}}}}/{{{{USER}}}}/{{{{FILE_NAME}}}}?start=-4096"
        opts = f"-Dspark.history.custom.executor.log.url={log_url}"
        with run_history_server(ev_dir, 2, log_name="hs-scenario03-test.log", extra_opts=opts) as url:
            hs = hs_api.HistoryServer(url)
            for _ in range(60):  # the app is listed once its first attempt is parsed: wait for the second
                if hs.attempts(bad) == ["1", "2"]:
                    break
                time.sleep(1)
            yield url, sc, ev_dir
    log = (ROOT / "build" / "hs-scenario03-test.log").read_text()
    assert "Exception" not in log, "History Server logged an exception while replaying"


def test_scenario03_in_history_server(stack):
    url, sc, ev_dir = stack
    hs = hs_api.HistoryServer(url)
    assert hs_api.validate(hs, ev_dir) == 0
    assert hs.attempts(sc.app_id("incident")) == ["1", "2"]

    want = evidence.solution_values(SCENARIO)
    got = evidence.compute(hs, SCENARIO)
    assert set(want) == set(got), "solution.md evidence ids and scripts/evidence.py disagree"
    mismatches = {k: (want[k], got[k]) for k in got if want[k] != got[k]}
    assert not mismatches, f"solution.md is stale (solution, server): {mismatches}"

    pages = ("jobs/", "jobs/job/?id=2", "stages/", "stages/stage/?id=2&attempt=0", "executors/", "environment/",
             "SQL/", "SQL/execution/?id=0")
    for app, attempt in ((sc.app_id("last_good"), "1"), (sc.app_id("incident"), "1"), (sc.app_id("incident"), "2")):
        for page in pages:
            r = requests.get(f"{url}/history/{app}/{attempt}/{page}", timeout=120)
            assert r.status_code == 200, (app, attempt, page)
