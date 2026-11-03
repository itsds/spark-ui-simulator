"""Scenario 00 in the real Spark 3.5.9 History Server: replay, pages, the SQL tab, the solution's evidence,
and the executor log links (mock NodeManager) that open logs matching what each executor did."""

from __future__ import annotations

import html
import re
import sys
from collections import defaultdict
from pathlib import Path

import pytest
import requests

from sparksim.apps import daily_enrichment

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evidence  # noqa: E402
import hs_api  # noqa: E402

SCENARIO = "00-healthy-baseline"
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    """Scenario 00's event log + container logs, the History Server and the mock NodeManager log server."""
    from conftest import run_history_server, run_nm_logs

    base = tmp_path_factory.mktemp("scenario00")
    ev_dir, logs_dir = base / "event-logs", base / "yarn-logs"
    ev_dir.mkdir()
    path = daily_enrichment.generate(ev_dir, logs_dir=logs_dir)
    app_id = evidence.SCENARIOS[SCENARIO][0]
    assert path.name == f"{app_id}_1"
    with run_nm_logs(logs_dir) as nm_url:
        log_url = f"{nm_url}/node/containerlogs/{{{{CONTAINER_ID}}}}/{{{{USER}}}}/{{{{FILE_NAME}}}}?start=-4096"
        opts = f"-Dspark.history.custom.executor.log.url={log_url}"
        with run_history_server(ev_dir, 1, log_name="hs-scenario00-test.log", extra_opts=opts) as url:
            yield url, nm_url, app_id, ev_dir
    log = (ROOT / "build" / "hs-scenario00-test.log").read_text()
    assert "Exception" not in log, "History Server logged an exception while replaying"


def test_scenario00_in_history_server(stack):
    url, _nm, app_id, ev_dir = stack
    hs = hs_api.HistoryServer(url)
    assert hs_api.validate(hs, ev_dir) == 0

    # Every number quoted in solution.md, recomputed from the REST API.
    want = evidence.solution_values(SCENARIO)
    got = evidence.compute(hs, SCENARIO)
    assert set(want) == set(got), "solution.md evidence ids and scripts/evidence.py disagree"
    mismatches = {k: (want[k], got[k]) for k in got if want[k] != got[k]}
    assert not mismatches, f"solution.md is stale (solution, server): {mismatches}"

    # The SQL tab: one execution per write, linked to its jobs, every plan node's metrics filled in.
    sql = hs.app(app_id, "sql", details="true", planDescription="false")
    assert [(q["id"], q["status"], q["successJobIds"]) for q in sql] == [
        (0, "COMPLETED", [0, 1, 2, 3]), (1, "COMPLETED", [4, 5]), (2, "COMPLETED", [6, 7])]
    for q in sql:
        for node in q["nodes"]:
            if node["nodeName"] in ("Scan parquet", "Filter", "Exchange", "Sort", "HashAggregate",
                                    "SortMergeJoin", "BroadcastHashJoin", "BroadcastExchange"):
                assert node["metrics"] and all(m["value"] for m in node["metrics"]), node["nodeName"]

    # The pages a learner opens render without errors.
    base = f"{url}/history/{app_id}/1"
    for page in ("jobs/", "jobs/job/?id=3", "stages/", "stages/stage/?id=2&attempt=0",
                 "stages/stage/?id=5&attempt=0", "executors/", "environment/", "storage/",
                 "SQL/", "SQL/execution/?id=0", "SQL/execution/?id=1", "SQL/execution/?id=2"):
        r = requests.get(f"{base}/{page}", timeout=120)
        assert r.status_code == 200, page


def _full_log(link: str) -> str:
    """Open a log link as the browser does, then its 'full log' page; return the log text."""
    r = requests.get(link, timeout=30)
    assert r.status_code == 200, link
    assert re.search(r"Showing \d+ bytes of \d+ total|Log Length: 0", r.text), link
    full = requests.get(link.replace("start=-4096", "start=0"), timeout=60)
    assert full.status_code == 200
    m = re.search(r"<pre>(.*)</pre>", full.text, re.S)
    return html.unescape(m.group(1)) if m else ""


def test_executor_log_links_open_matching_logs(stack):
    """Phase 5 'done when': every executor's stderr link opens a log that matches what that executor did."""
    url, nm, app_id, _ev = stack
    hs = hs_api.HistoryServer(url)
    execs = hs.app(app_id, "allexecutors")
    by_exec: dict[str, dict[str, set[int]]] = defaultdict(lambda: {"all": set(), "ok": set()})
    for stage in hs.app(app_id, "stages"):
        for t in hs.all_tasks(app_id, stage["stageId"], stage["attemptId"]):
            by_exec[t["executorId"]]["all"].add(t["taskId"])
            if t["status"] == "SUCCESS":
                by_exec[t["executorId"]]["ok"].add(t["taskId"])
    assert len(execs) == 41
    for e in execs:
        logs = e["executorLogs"]
        assert set(logs) == {"stdout", "stderr"}
        assert all(v.startswith(f"{nm}/node/containerlogs/") for v in logs.values()), "custom log URL not applied"
        container = e["attributes"]["CONTAINER_ID"]
        assert container in logs["stderr"]
        text = _full_log(logs["stderr"])
        assert text.startswith("Using Spark's default log4j profile")
        if e["id"] == "driver":
            assert f"Submitted application: {daily_enrichment.SPEC.app_name}" in text
            assert text.count("INFO TaskSetManager: Finished task ") == sum(len(v["ok"]) for v in by_exec.values())
            assert "Final app status: SUCCEEDED, exitCode: 0" in text
            continue
        host = e["hostPort"].split(":")[0]
        assert f"Starting executor ID {e['id']} on host {host}" in text
        running = {int(x) for x in re.findall(r"Executor: Running task \S+ in stage \S+ \(TID (\d+)\)", text)}
        finished = {int(x) for x in re.findall(r"Executor: Finished task \S+ in stage \S+ \(TID (\d+)\)", text)}
        assert running == by_exec[e["id"]]["all"], f"executor {e['id']}: tasks in its log != tasks it ran"
        assert finished == by_exec[e["id"]]["ok"]
        assert text.rstrip().endswith("ShutdownHookManager: Shutdown hook called")
        assert _full_log(logs["stdout"]) == ""
