#!/usr/bin/env python3
"""Smoke-check a running stack (`uv run sparksim up` or `docker compose up`) the way a learner uses it.

  uv run scripts/stack_check.py [--portal http://localhost:8000] [--wait 600] [--scenario 01-join-skew ...]

Waits until the portal reports every run ready, then checks: the portal's pages (cards, ticket, hints,
solution), each run's History Server deep links (Jobs, Stages, SQL, Executors, Environment pages) and, through
the History Server's REST API, that the driver's and an executor's `stderr` links open their container logs
on the log server. Exit code 0 when everything passed. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request

PAGES = ("jobs/", "stages/", "SQL/", "executors/", "environment/")


def get(url: str, timeout: float = 120) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - local stack
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except OSError as e:
        return 0, str(e)


class Check:
    def __init__(self):
        self.failed = 0

    def __call__(self, ok: bool, what: str) -> bool:
        print(f"  {'✓' if ok else '✗'} {what}", flush=True)
        self.failed += not ok
        return ok


def wait_ready(portal: str, wait: float, scenarios: list[str] | None) -> dict | None:
    """The portal's status once every run (of `scenarios`, default all) is ready; None after `wait` s."""
    deadline = time.monotonic() + wait
    last = ""
    while True:
        code, body = get(f"{portal}/api/status", timeout=10)
        if code == 200:
            st = json.loads(body)
            states = {a: i["state"] for a, i in st["apps"].items()
                      if not scenarios or set(scenarios) & set(i["scenarios"])}
            line = ", ".join(f"{a[-5:]}: {s}" for a, s in states.items())
            if line != last:
                print(f"  status: {line}", flush=True)
                last = line
            if states and all(s == "ready" for s in states.values()):
                return st
        elif not last:
            print(f"  portal not answering yet ({body or code})", flush=True)
            last = "down"
        if time.monotonic() > deadline:
            return None
        time.sleep(3)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--portal", default="http://localhost:8000")
    ap.add_argument("--wait", default=600, type=float, help="seconds to wait for every run to be ready")
    ap.add_argument("--scenario", action="append", help="only these scenarios' pages (default: every card)")
    a = ap.parse_args(argv)
    portal = a.portal.rstrip("/")
    ok = Check()

    print(f"Portal {portal}: waiting for every run to be ready ...", flush=True)
    st = wait_ready(portal, a.wait, a.scenario)
    if not ok(st is not None, f"every run ready within {a.wait:.0f} s"):
        return 1
    hs = st["history_server"]["url"].rstrip("/")
    ok(st["nm_logs"]["up"], "container log server reachable from the portal")

    code, index = get(f"{portal}/")
    ids = re.findall(r'href="/s/([0-9]{2}-[a-z0-9-]+)"', index)
    ok(code == 200 and bool(ids), f"portal index lists {len(set(ids))} scenario(s)")
    for sid in a.scenario or sorted(set(ids)):
        print(f"Scenario {sid}", flush=True)
        code, page = get(f"{portal}/s/{sid}")
        if not ok(code == 200, f"portal page /s/{sid}"):
            continue
        hints = len(re.findall(r'<li class="hint"', page))
        ok('class="card ticket"' in page and "<table>" in page, "ticket rendered")
        ok(hints > 0, f"{hints} hints")
        ok('class="md solution-body"' in page, "solution rendered (hidden until revealed)")
        apps = sorted(set(re.findall(r'data-run="(application_\d+_\d+)"', page)))
        ok(bool(apps), f"runs: {', '.join(apps)}")
        for app, attempt in [(a, att) for a in apps for att in st["apps"][a]["attempts"]]:
            for p in PAGES:
                code, body = get(f"{hs}/history/{app}/{attempt}/{p}")
                ok(code == 200 and app in body, f"{app} {p}")
            code, body = get(f"{hs}/api/v1/applications/{app}/{attempt}/allexecutors")
            execs = json.loads(body) if code == 200 else []
            driver = next((e for e in execs if e["id"] == "driver"), None)
            worker = next((e for e in execs if e["id"] != "driver"), None)
            for e in (driver, worker):
                link = (e or {}).get("executorLogs", {}).get("stderr")
                if not ok(bool(link), f"{app} executor {(e or {}).get('id')} has a stderr link"):
                    continue
                code, log = get(link)
                container = link.split("/containerlogs/")[-1].split("/")[0]
                ok(code == 200 and "Log Type: stderr" in log and container in log,
                   f"  … opens {container}'s log ({link.split('/node/')[0]})")
    print(f"\n{'All checks passed.' if not ok.failed else f'{ok.failed} check(s) failed.'}")
    return 1 if ok.failed else 0


if __name__ == "__main__":
    sys.exit(main())
