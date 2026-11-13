#!/usr/bin/env python3
"""Query a running Spark History Server's REST API (/api/v1) and validate replays.

Examples
  uv run scripts/hs_api.py apps
  uv run scripts/hs_api.py stages <app-id>
  uv run scripts/hs_api.py tasks <app-id> <stage-id> [--attempt 0]
  uv run scripts/hs_api.py summary <app-id> <stage-id>
  uv run scripts/hs_api.py get <app-id> executors          # any sub-path under the app
  uv run scripts/hs_api.py --app-attempt 1 jobs <app-id>     # a failed app's first YARN attempt
  uv run scripts/hs_api.py validate --log-dir reference/event-logs

`validate` checks that every event log in --log-dir is listed by the History
Server and that what it serves matches the raw log (job, stage, task, executor
and SQL execution counts). A mismatch means the server dropped or failed to
replay events.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

import requests

DEFAULT_URL = "http://localhost:18080"


RETAINED_DEAD_EXECUTORS = 100  # spark.ui.retainedDeadExecutors (the History Server's default)


class HistoryServer:
    def __init__(self, base_url: str = DEFAULT_URL):
        self.base = base_url.rstrip("/") + "/api/v1"

    def get(self, path: str, **params):
        r = requests.get(f"{self.base}/{path.lstrip('/')}", params=params or None, timeout=120)
        r.raise_for_status()
        return r.json()

    def apps(self):
        return self.get("applications")

    def app_path(self, app_id: str, app_attempt: str | None = None) -> str:
        """`applications/<id>` or `applications/<id>/<attemptId>` (YARN cluster apps have attempt IDs).

        `app_attempt`: the YARN attempt ("1", "2"); default the latest one (the History Server lists an
        app's attempts newest first).
        """
        if app_attempt is not None:
            return f"applications/{app_id}/{app_attempt}"
        app = self.get(f"applications/{app_id}")
        attempt = app["attempts"][0].get("attemptId")
        return f"applications/{app_id}/{attempt}" if attempt else f"applications/{app_id}"

    def attempts(self, app_id: str) -> list[str]:
        """The app's YARN attempt ids, oldest first."""
        app = self.get(f"applications/{app_id}")
        return sorted((a.get("attemptId") for a in app["attempts"] if a.get("attemptId")), key=int)

    def app(self, app_id: str, sub: str, app_attempt: str | None = None, **params):
        return self.get(f"{self.app_path(app_id, app_attempt)}/{sub.lstrip('/')}", **params)

    def all_tasks(self, app_id: str, stage_id: int, attempt: int = 0, page: int = 2000,
                  app_attempt: str | None = None):
        """Every task of stage attempt `attempt` (of YARN attempt `app_attempt`)."""
        out, offset = [], 0
        while True:
            batch = self.app(app_id, f"stages/{stage_id}/{attempt}/taskList", app_attempt, offset=offset,
                             length=page)
            out.extend(batch)
            if len(batch) < page:
                return out
            offset += page


# ---------------------------------------------------------------------------- raw event log


def read_events(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def log_counts(path: Path) -> dict:
    ev = Counter()
    app_id = attempt = None
    stage_attempts = set()
    task_ids = set()  # started tasks: a Resubmitted map task ends twice, a task killed as the app stops never
    executors = set()
    removed = set()
    for e in read_events(path):
        name = e["Event"].rsplit(".", 1)[-1]
        ev[name] += 1
        if name == "SparkListenerApplicationStart":
            app_id, attempt = e.get("App ID"), e.get("App Attempt ID")
        elif name == "SparkListenerTaskStart":  # the server lists every started task, ended or not
            task_ids.add(e["Task Info"]["Task ID"])
        elif name == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            stage_attempts.add((si["Stage ID"], si["Stage Attempt ID"]))
        elif name == "SparkListenerExecutorAdded":
            executors.add(e["Executor ID"])
        elif name == "SparkListenerExecutorRemoved":
            removed.add(e["Executor ID"])
    return {
        "app_id": app_id,
        "attempt": attempt,
        "jobs": ev["SparkListenerJobStart"],
        "stage_attempts": len(stage_attempts),
        "tasks": len(task_ids),
        "executors": executors,
        "removed": removed,
        "sql": ev["SparkListenerSQLExecutionStart"],
        "events": ev,
    }


def validate(hs: HistoryServer, log_dir: Path) -> int:
    listed = {a["id"] for a in hs.apps()}
    files = sorted(p for p in log_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    failures = 0
    for f in files:
        want = log_counts(f)
        app_id, att = want["app_id"], want["attempt"]
        problems = []
        if app_id not in listed:
            problems.append("not listed by History Server")
        else:
            jobs = hs.app(app_id, "jobs", att)
            stages = hs.app(app_id, "stages", att)
            execs = {e["id"] for e in hs.app(app_id, "allexecutors", att)}
            sql = hs.app(app_id, "sql", att, length=10_000)
            tasks = sum(
                len(hs.all_tasks(app_id, s["stageId"], s["attemptId"], app_attempt=att))
                for s in stages
                if s["status"] != "SKIPPED"
            )
            got = {
                "jobs": len(jobs),
                "stage_attempts": len([s for s in stages if s["status"] != "SKIPPED"]),
                "tasks": tasks,
                "sql": len(sql),
            }
            for k, v in got.items():
                if v != want[k]:
                    problems.append(f"{k}: log={want[k]} server={v}")
            # AppStatusListener keeps the latest spark.ui.retainedDeadExecutors (100) dead executors: past that it
            # deletes the oldest max(10, dead - 100) at a time, so 91-100 of them stay (ref: AppStatusListener)
            missing = want["executors"] - execs
            kept = len(want["removed"]) - len(missing)
            lo = RETAINED_DEAD_EXECUTORS - RETAINED_DEAD_EXECUTORS // 10
            if missing and (not missing <= want["removed"] or not lo < kept <= RETAINED_DEAD_EXECUTORS):
                problems.append(f"executors missing on server: {sorted(missing)}")
        status = "OK  " if not problems else "FAIL"
        failures += bool(problems)
        print(
            f"{status} {f.name:<45} jobs={want['jobs']:<3} stages={want['stage_attempts']:<3} "
            f"tasks={want['tasks']:<6} execs={len(want['executors']):<3} sql={want['sql']:<3}"
            + ("" if not problems else "  <- " + "; ".join(problems))
        )
    print(f"\n{len(files) - failures}/{len(files)} event logs replay consistently.")
    return 1 if failures else 0


# ---------------------------------------------------------------------------- CLI


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--app-attempt", help="YARN attempt of the app (default: the latest)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("apps")
    for name in ("jobs", "stages", "executors", "sql", "environment"):
        sub.add_parser(name).add_argument("app_id")
    t = sub.add_parser("tasks")
    t.add_argument("app_id")
    t.add_argument("stage_id", type=int)
    t.add_argument("--attempt", type=int, default=0)
    s = sub.add_parser("summary", help="task metric quantiles for a stage")
    s.add_argument("app_id")
    s.add_argument("stage_id", type=int)
    s.add_argument("--attempt", type=int, default=0)
    g = sub.add_parser("get", help="any sub-path under the application")
    g.add_argument("app_id")
    g.add_argument("path")
    v = sub.add_parser("validate")
    v.add_argument("--log-dir", type=Path, required=True)
    a = ap.parse_args(argv)

    hs = HistoryServer(a.url)
    if a.cmd == "validate":
        return validate(hs, a.log_dir)
    if a.cmd == "apps":
        out = hs.apps()
    elif a.cmd == "executors":
        out = hs.app(a.app_id, "allexecutors", a.app_attempt)
    elif a.cmd == "tasks":
        out = hs.all_tasks(a.app_id, a.stage_id, a.attempt, app_attempt=a.app_attempt)
    elif a.cmd == "summary":
        out = hs.app(a.app_id, f"stages/{a.stage_id}/{a.attempt}/taskSummary", a.app_attempt,
                     quantiles="0,0.25,0.5,0.75,1.0")
    elif a.cmd == "get":
        out = hs.app(a.app_id, a.path, a.app_attempt)
    else:
        out = hs.app(a.app_id, a.cmd, a.app_attempt)
    json.dump(out, sys.stdout, indent=2)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
