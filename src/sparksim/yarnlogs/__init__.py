"""YARN container logs (stdout/stderr) that match the simulated run.

`write_container_logs(run, out_dir)` writes `<out_dir>/<appId>/<containerId>/{stdout,stderr}` for the
ApplicationMaster (the driver, in cluster mode) and every executor container, the layout the mock
NodeManager log server (`services/nm-logs`) serves. Executor logs come from `executor.py`, the driver's
from `driver.py`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from .driver import am_stderr, am_stdout
from .executor import executor_stderr

if TYPE_CHECKING:
    from ..engine.runtime import AppRun


def write_container_logs(run: AppRun, out_dir: str | Path) -> Path:
    """Write every container's logs; return the application's log directory."""
    app_dir = Path(out_dir) / run.yapp.app_id
    app_dir.mkdir(parents=True, exist_ok=True)

    uploaded = (run.app_end + 6_000) / 1000  # log aggregation uploads shortly after the app finished

    def put(container_id: str, stdout: str, stderr: str) -> None:
        d = app_dir / container_id
        d.mkdir(exist_ok=True)
        for name, text in (("stdout", stdout), ("stderr", stderr)):
            (d / name).write_text(text, encoding="utf-8")
            os.utime(d / name, (uploaded, uploaded))  # the mock NodeManager shows it as "Log Upload Time"

    echo: list = []
    for ex in run.executors:
        put(ex.container_id, "", executor_stderr(run, ex, echo))
    put(run.yapp.am_container_id, am_stdout(run), am_stderr(run, echo))
    return app_dir


__all__ = ["am_stderr", "am_stdout", "executor_stderr", "write_container_logs"]
