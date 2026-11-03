#!/usr/bin/env python3
"""Screenshot key Spark History Server pages for an app (visual smoke test).

  uv run scripts/screenshot.py <app-id> [--url http://localhost:18080] [--out build/screenshots]
      [--stage 6] [--sql 0]

Saves jobs, job detail (DAG), stages, stage detail (DAG, Event Timeline, all
additional metrics, aggregated metrics by executor), executors, environment
and SQL pages (list, plan graph with Details expanded, the other executions) as PNGs. Uses the Playwright +
Chromium preinstalled in the dev container (`require('playwright')` via node).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]

JS = r"""
const { chromium } = require('playwright');
const pages = JSON.parse(process.argv[2]);
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 } });
  for (const p of pages) {
    await page.goto(p.url, { waitUntil: 'networkidle' });
    for (const sel of (p.click || [])) {
      const el = await page.$(sel);
      if (el) { await el.click(); await page.waitForTimeout(600); }
    }
    await page.waitForTimeout(800);
    await page.screenshot({ path: p.path, fullPage: !!p.full });
    console.log(p.path);
  }
  await browser.close();
})();
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("app_id")
    ap.add_argument("--url", default="http://localhost:18080")
    ap.add_argument("--out", type=Path, default=ROOT / "build" / "screenshots")
    ap.add_argument("--stage", type=int, help="stage id for the stage-detail shot (default: longest stage)")
    ap.add_argument("--sql", type=int, help="SQL execution id for the plan shot (default: first)")
    ap.add_argument("--job", type=int, default=0, help="job id for the job-detail shot (default: 0)")
    a = ap.parse_args(argv)

    api = f"{a.url}/api/v1/applications/{a.app_id}"
    attempt = requests.get(api, timeout=60).json()["attempts"][0].get("attemptId")
    base = f"{a.url}/history/{a.app_id}" + (f"/{attempt}" if attempt else "")
    api = api + (f"/{attempt}" if attempt else "")
    stages = requests.get(f"{api}/stages", timeout=120).json()
    if a.stage is None:
        done = [s for s in stages if s.get("completionTime") and s.get("submissionTime")]
        longest = max(done, key=lambda s: s["executorRunTime"])
        a.stage, attempt_id = longest["stageId"], longest["attemptId"]
    else:
        attempt_id = 0
    sql = requests.get(f"{api}/sql", timeout=60).json()
    if a.sql is None and sql:
        a.sql = sql[0]["id"]

    out = a.out / a.app_id
    out.mkdir(parents=True, exist_ok=True)
    shots = [
        {"url": f"{base}/jobs/", "path": str(out / "1-jobs.png"),
         "click": ["span.expand-application-timeline"]},
        {"url": f"{base}/stages/", "path": str(out / "2-stages.png")},
        {"url": f"{base}/jobs/job/?id={a.job}", "path": str(out / "1b-job-detail.png"),
         "click": ["#job-dag-viz", "span.expand-task-assignment-timeline"], "full": True},
        # stagepage.js: #additionalMetrics toggles the metric checkboxes, #box-0 is "Select All".
        {"url": f"{base}/stages/stage/?id={a.stage}&attempt={attempt_id}", "path": str(out / "3-stage-detail.png"),
         "click": ["#stage-dag-viz", "span.expand-task-assignment-timeline", "#additionalMetrics", "#box-0",
                   "#aggregatedMetrics"], "full": True},
        {"url": f"{base}/executors/", "path": str(out / "4-executors.png")},
        {"url": f"{base}/environment/", "path": str(out / "5-environment.png")},
    ]
    if a.sql is not None:
        shots.append({"url": f"{base}/SQL/", "path": str(out / "6-sql-list.png")})
        # ExecutionPage.scala: the "Details" link under the graph calls clickPhysicalPlanDetails()
        shots.append({"url": f"{base}/SQL/execution/?id={a.sql}", "path": str(out / "6b-sql-plan.png"),
                      "click": ["span[onclick='clickPhysicalPlanDetails();']"], "full": True})
        for other in [x["id"] for x in sql if x["id"] != a.sql][:2]:
            shots.append({"url": f"{base}/SQL/execution/?id={other}", "path": str(out / f"6c-sql-plan-{other}.png"),
                          "full": True})

    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
        fh.write(JS)
    node_path = subprocess.run(["npm", "root", "-g"], capture_output=True, text=True).stdout.strip()
    env = {**os.environ, "NODE_PATH": node_path}  # keeps PLAYWRIGHT_BROWSERS_PATH etc.
    r = subprocess.run(["node", fh.name, json.dumps(shots)], capture_output=True, text=True, env=env)
    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr[-2000:])
    return r.returncode


if __name__ == "__main__":
    raise SystemExit(main())
