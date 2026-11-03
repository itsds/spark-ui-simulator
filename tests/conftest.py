from __future__ import annotations

import contextlib
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


@contextlib.contextmanager
def run_history_server(log_dir: Path, expected_apps: int, log_name: str = "hs-test.log", timeout: float = 120,
                       extra_opts: str = ""):
    """Start the real Spark 3.5.9 History Server on `log_dir`; yield its base URL once it lists every app.

    `extra_opts`: more `-Dspark.history.*` settings (SPARK_HISTORY_OPTS)."""
    port = _free_port()
    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}
    if extra_opts:
        env["SPARK_HISTORY_OPTS"] = extra_opts
    (ROOT / "build").mkdir(exist_ok=True)
    with open(ROOT / "build" / log_name, "w") as log:
        proc = subprocess.Popen(
            [str(ROOT / "services" / "history-server" / "run.sh"), str(log_dir), str(port)],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        url = f"http://localhost:{port}"
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                if proc.poll() is not None:
                    pytest.fail(f"History Server exited early (see build/{log_name})")
                try:
                    if len(requests.get(f"{url}/api/v1/applications", timeout=5).json()) >= expected_apps:
                        break
                except (requests.ConnectionError, ValueError):
                    pass
                time.sleep(1)
            else:
                pytest.fail(f"History Server did not list {expected_apps} apps within {timeout}s")
            yield url
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()


@contextlib.contextmanager
def run_nm_logs(log_dir: Path):
    """Start the mock NodeManager log server (services/nm-logs) on `log_dir`; yield its base URL."""
    import sys

    port = _free_port()
    proc = subprocess.Popen([sys.executable, str(ROOT / "services" / "nm-logs" / "server.py"), "--dir", str(log_dir),
                             "--port", str(port), "--host", "127.0.0.1"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://localhost:{port}"
    try:
        for _ in range(50):
            try:
                requests.get(url, timeout=2)
                break
            except requests.ConnectionError:
                time.sleep(0.1)
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture(scope="session")
def history_server():
    """Real History Server on reference/event-logs."""
    expected = len([p for p in REFERENCE_LOGS.iterdir() if p.is_file()])
    with run_history_server(REFERENCE_LOGS, expected) as url:
        yield url
