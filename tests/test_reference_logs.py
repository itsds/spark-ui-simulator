"""Sanity checks on the real reference event logs (format truth for the generator)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import hs_api  # noqa: E402

LOGS = sorted(p for p in (ROOT / "reference" / "event-logs").iterdir() if p.is_file())


def events(path: Path):
    return [json.loads(line) for line in path.open() if line.strip()]


def test_reference_logs_exist():
    assert len(LOGS) >= 9, "run reference/capture.sh to (re)create the reference logs"


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_log_is_well_formed(path: Path):
    ev = events(path)
    assert ev[0] == {"Event": "SparkListenerLogStart", "Spark Version": "3.5.9"}
    names = [e["Event"] for e in ev]
    assert names.count("SparkListenerApplicationStart") == 1
    assert names[-1] in ("SparkListenerApplicationEnd", "SparkListenerExecutorRemoved")
    assert "SparkListenerApplicationEnd" in names

    starts = {e["Task Info"]["Task ID"] for e in ev if e["Event"] == "SparkListenerTaskStart"}
    ends = [e["Task Info"]["Task ID"] for e in ev if e["Event"] == "SparkListenerTaskEnd"]
    assert len(ends) == len(set(ends)), "task IDs must be unique"
    assert set(ends) <= starts, "every TaskEnd needs a TaskStart"


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_captured_on_yarn_cluster_mode(path: Path):
    ev = events(path)
    app = next(e for e in ev if e["Event"] == "SparkListenerApplicationStart")
    assert app["App ID"].startswith("application_"), "reference logs must come from YARN (reference/capture.sh)"
    assert app["App Attempt ID"] == "1"
    env = next(e for e in ev if e["Event"] == "SparkListenerEnvironmentUpdate")
    props = dict(env["Spark Properties"]) if isinstance(env["Spark Properties"], list) else env["Spark Properties"]
    assert props["spark.master"] == "yarn"
    assert props["spark.submit.deployMode"] == "cluster"


@pytest.mark.integration
def test_history_server_replays_every_reference_log(history_server):
    hs = hs_api.HistoryServer(history_server)
    assert hs_api.validate(hs, ROOT / "reference" / "event-logs") == 0
