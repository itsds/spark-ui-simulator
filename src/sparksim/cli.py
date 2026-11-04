"""`sparksim` command line.

  sparksim generate <scenario> [--out event-logs] [--logs-out yarn-logs] [--run ROLE] [--seed N]
                     every run of scenarios/<scenario>/scenario.yaml (incidents: the last good run and the
                     incident run), each event log + its YARN container logs
  sparksim generate tiny [--out event-logs] [--seed N]                 the Phase 2 smoke app (8 tasks)
  sparksim scenarios                                                   list the scenarios and their runs
  sparksim validate [<scenario>...]                                    check scenario.yaml files (default: all)
  sparksim roundtrip <event-log>...                                    parse + re-serialize, report byte diffs
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

from .eventlog import event_to_line, line_to_event


def _generate(a) -> int:
    if a.app == "tiny":
        from .apps import tiny

        path = tiny.generate(a.out, tiny.TinySpec() if a.seed is None else tiny.TinySpec(seed=a.seed))
        print(f"wrote {path}")
        return 0
    from .eventlog import write_event_log
    from .scenario import load_scenario
    from .yarnlogs import write_container_logs

    sc = load_scenario(a.app)
    logs = Path(a.logs_out) if a.logs_out else None
    for role in ([a.run] if a.run else sc.roles):
        spec = sc.spec(role)
        if a.seed is not None:
            spec = replace(spec, seed=a.seed)
        from .engine.emit import build_events
        from .engine.runtime import AppRun

        run = AppRun(spec).run()
        if logs is not None:
            write_container_logs(run, logs)
        path = write_event_log(Path(a.out) / run.yapp.event_log_name, build_events(run))
        label = sc.model.runs[role].label
        print(f"{role:9s} {run.yapp.app_id}  {label}\n          wrote {path}"
              + (f" (+ container logs under {logs})" if logs else ""))
    return 0


def _scenarios(a) -> int:
    from .scenario import load_scenario, scenario_ids

    for sid in scenario_ids():
        sc = load_scenario(sid)
        m = sc.model
        print(f"{sid}  [{m.difficulty}]  {m.title}")
        for role in sc.roles:
            print(f"    {role:9s} {sc.app_id(role)}  {m.runs[role].label}")
    return 0


def _validate(a) -> int:
    from .scenario import load_scenario, scenario_ids

    bad = 0
    for sid in a.scenarios or scenario_ids():
        try:
            sc = load_scenario(sid)
            faults = sum(len(r.faults) for r in sc.model.runs.values())
            print(f"OK   {sid}: {len(sc.roles)} run(s), {faults} fault(s)")
        except Exception as e:  # noqa: BLE001 - report every scenario's problem
            bad += 1
            print(f"FAIL {sid}: {e}")
    return 1 if bad else 0


def _roundtrip(a) -> int:
    bad = 0
    for p in a.logs:
        lines = Path(p).read_text(encoding="utf-8").splitlines()
        diffs = [i for i, ln in enumerate(lines, 1) if event_to_line(line_to_event(ln)) != ln]
        bad += bool(diffs)
        where = f", first diff at line {diffs[0]}" if diffs else ""
        print(f"{'OK  ' if not diffs else 'DIFF'} {p}: {len(lines)} lines{where}")
    return 1 if bad else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sparksim", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write a scenario's event logs (every run) or the tiny app's")
    g.add_argument("app", help="a scenario id (see `sparksim scenarios`) or 'tiny'")
    g.add_argument("--out", default="event-logs", type=Path)
    g.add_argument("--run", default=None, help="only this run (baseline | last_good | incident)")
    g.add_argument("--seed", default=None, type=int, help="override the run's seed")
    g.add_argument("--logs-out", default="yarn-logs",
                   help="YARN container logs (stdout/stderr) served by services/nm-logs; '' to skip")
    g.set_defaults(fn=_generate)
    s = sub.add_parser("scenarios", help="list the scenarios and their runs")
    s.set_defaults(fn=_scenarios)
    v = sub.add_parser("validate", help="check scenario.yaml files against the schema and their workloads")
    v.add_argument("scenarios", nargs="*")
    v.set_defaults(fn=_validate)
    r = sub.add_parser("roundtrip", help="check that the event model reproduces a log byte for byte")
    r.add_argument("logs", nargs="+", type=Path)
    r.set_defaults(fn=_roundtrip)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
