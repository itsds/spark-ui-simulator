"""`sparksim up` (ADR-032): one command from nothing to a scenario ready to debug, then a clean stop.

Runs the real command on free ports and fresh directories, waits for its "Ready." line, then checks the stack
the way a learner uses it (`scripts/stack_check.py`: portal pages, History Server deep links, container logs
behind the executors' log links), and that Ctrl+C (SIGINT) stops every server it started."""

from __future__ import annotations

import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import requests
from conftest import _free_port

from sparksim.prepare import MANIFEST
from sparksim.scenario import load_scenario
from sparksim.up import port_in_use

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import stack_check  # noqa: E402

pytestmark = pytest.mark.integration
SCENARIO = "00-healthy-baseline"


def test_one_command_from_nothing_to_ready(tmp_path):
    ports = {"--port": _free_port(), "--hs-port": _free_port(), "--nm-port": _free_port()}
    ev, yl = tmp_path / "event-logs", tmp_path / "yarn-logs"
    cmd = [sys.executable, "-m", "sparksim.cli", "up", SCENARIO, "--event-logs", str(ev), "--yarn-logs", str(yl)]
    for k, v in ports.items():
        cmd += [k, str(v)]
    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
    proc = subprocess.Popen(cmd, cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines: queue.Queue[str] = queue.Queue()
    threading.Thread(target=lambda: [lines.put(ln) for ln in proc.stdout], daemon=True).start()
    out: list[str] = []
    try:
        deadline = time.monotonic() + 300
        while not any(ln.startswith("Ready.") for ln in out):
            assert proc.poll() is None, "sparksim up exited:\n" + "".join(out)
            assert time.monotonic() < deadline, "no 'Ready.' line within 300 s:\n" + "".join(out)
            try:
                out.append(lines.get(timeout=1))
            except queue.Empty:
                pass
        text = "".join(out)
        app = load_scenario(SCENARIO).app_id("baseline")
        assert f"generating  {app}" in text
        assert (ev / f"{app}_1").is_file() and (ev / MANIFEST).is_file() and (yl / app).is_dir()

        hs = f"http://localhost:{ports['--hs-port']}"
        assert [a["id"] for a in requests.get(f"{hs}/api/v1/applications", timeout=30).json()] == [app]
        portal = f"http://localhost:{ports['--port']}"
        assert stack_check.main(["--portal", portal, "--wait", "60", "--scenario", SCENARIO]) == 0
    finally:
        proc.send_signal(signal.SIGINT)  # Ctrl+C
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise
    assert proc.returncode == 0
    time.sleep(1)
    assert not any(port_in_use(p) for p in ports.values()), "a server outlived sparksim up"
    log = (ROOT / "build" / "logs" / "history-server.log").read_text()
    assert "Exception" not in log, "the History Server logged an exception (e.g. while reading the manifest)"
