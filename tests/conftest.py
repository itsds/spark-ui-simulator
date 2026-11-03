from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
REFERENCE_LOGS = ROOT / "reference" / "event-logs"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def history_server():
    """Start the real Spark 3.5.9 History Server on reference/event-logs; yield its base URL."""
    port = _free_port()
    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
    log = open(ROOT / "build" / "hs-test.log", "w") if (ROOT / "build").exists() else subprocess.DEVNULL
    proc = subprocess.Popen(
        [str(ROOT / "services" / "history-server" / "run.sh"), str(REFERENCE_LOGS), str(port)],
        env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    url = f"http://localhost:{port}"
    expected = len([p for p in REFERENCE_LOGS.iterdir() if p.is_file()])
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                if len(requests.get(f"{url}/api/v1/applications", timeout=5).json()) >= expected:
                    break
            except (requests.ConnectionError, ValueError):
                pass
            time.sleep(1)
        else:
            pytest.fail("History Server did not list all reference apps within 120s")
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
