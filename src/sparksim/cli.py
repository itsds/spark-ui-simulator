"""`sparksim` command line.

  sparksim up [<scenario>...] [--open]       the whole stack, one command: generate what is missing or out of
                                             date, start the History Server, the container log server and the
                                             portal (http://localhost:8000); Ctrl+C stops it
  sparksim prepare [<scenario>...] [--out event-logs] [--logs-out yarn-logs] [--force]
                                             generate the apps of the scenarios (default: all) that are missing
                                             or out of date (manifest: <out>/.sparksim-manifest.json)
  sparksim portal [--event-logs DIR] [--port 8000] [--hs-url URL] [--hs-api URL] [--nm-api URL]
                                             the scenario portal alone
  sparksim generate <scenario> [--out event-logs] [--logs-out yarn-logs] [--run ROLE] [--seed N]
                     every run of scenarios/<scenario>/scenario.yaml (incidents: the last good run and the
                     incident run), each event log + its YARN container logs, always regenerated
  sparksim generate tiny [--out event-logs] [--seed N]                 the Phase 2 smoke app (8 tasks)
  sparksim scenarios                                                   list the scenarios and their runs
  sparksim validate [<scenario>...]                                    check scenario.yaml files (default: all)
  sparksim roundtrip <event-log>...                                    parse + re-serialize, report byte diffs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .eventlog import event_to_line, line_to_event


def _generate(a) -> int:
    if a.app == "tiny":
        from .apps import tiny

        path = tiny.generate(a.out, tiny.TinySpec() if a.seed is None else tiny.TinySpec(seed=a.seed))
        print(f"wrote {path}")
        return 0
    from .prepare import generate_app
    from .scenario import load_scenario

    sc = load_scenario(a.app)
    logs = Path(a.logs_out) if a.logs_out else None
    for role in ([a.run] if a.run else sc.roles):
        path = generate_app(sc, role, a.out, logs, seed=a.seed)
        label = sc.model.runs[role].label
        written = ", ".join(str(p) for p in sorted(path.parent.glob(f"{sc.app_id(role)}_*")))  # one per YARN attempt
        print(f"{role:9s} {sc.app_id(role)}  {label}\n          wrote {written}"
              + (f" (+ container logs under {logs})" if logs else ""))
    return 0


def _prepare(a) -> int:
    from .prepare import prepare

    logs = Path(a.logs_out) if a.logs_out else None
    done = prepare(a.scenarios or None, a.out, logs, force=a.force)
    n = sum(1 for _, action in done if action == "generated")
    print(f"{len(done)} application(s): {n} generated, {len(done) - n} up to date ({a.out})")
    return 0


def _portal(a) -> int:
    from .portal import PortalConfig, serve

    serve(PortalConfig(event_logs=Path(a.event_logs), hs_url=a.hs_url, hs_api=a.hs_api or None,
                       nm_api=a.nm_api or None), a.host, a.port)
    return 0


def _up(a) -> int:
    from .up import UpOptions, up

    return up(UpOptions(scenarios=a.scenarios, event_logs=Path(a.event_logs).resolve(),
                        yarn_logs=Path(a.yarn_logs).resolve(), portal_port=a.port, hs_port=a.hs_port,
                        nm_port=a.nm_port, generate=not a.no_generate, force=a.force, open_browser=a.open,
                        timeout=a.timeout))


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
    from .prepare import plan_apps
    from .scenario import load_scenario, scenario_ids

    bad, loaded = 0, []
    for sid in a.scenarios or scenario_ids():
        try:
            sc = load_scenario(sid)
            faults = sum(len(r.faults) for r in sc.model.runs.values())
            print(f"OK   {sid}: {len(sc.roles)} run(s), {faults} fault(s)")
            loaded.append(sc)
        except Exception as e:  # noqa: BLE001 - report every scenario's problem
            bad += 1
            print(f"FAIL {sid}: {e}")
    try:  # runs of different scenarios may share an app id only when they are the same run
        plan_apps(loaded)
    except ValueError as e:
        bad += 1
        print(f"FAIL {e}")
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


UP_DESCRIPTION = """\
One command for the whole stack, without Docker: starts the mock NodeManager log server, the real Spark 3.5.9
History Server (PySpark's bundled Spark, JDK 17) and the scenario portal, generates every app the scenarios
need that is missing or out of date, waits until the History Server lists them, and runs until Ctrl+C.
Server output goes to build/logs/. `docker compose up` runs the same stack in containers."""


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


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
    p = sub.add_parser("prepare", help="generate the scenarios' apps that are missing or out of date")
    p.add_argument("scenarios", nargs="*", help="scenario ids (default: all)")
    p.add_argument("--out", default="event-logs", type=Path)
    p.add_argument("--logs-out", default="yarn-logs", help="YARN container logs; '' to skip")
    p.add_argument("--force", action="store_true", help="regenerate even what is up to date")
    p.set_defaults(fn=_prepare)
    w = sub.add_parser("portal", help="serve the scenario portal (tickets, hints, solutions, History Server links)")
    w.add_argument("--event-logs", default="event-logs", help="where the event logs and their manifest are")
    w.add_argument("--host", default="127.0.0.1")
    w.add_argument("--port", default=8000, type=int)
    w.add_argument("--hs-url", default="http://localhost:18080", help="the History Server, as browsers reach it")
    w.add_argument("--hs-api", default="", help="the History Server, as the portal reaches it (default: --hs-url)")
    w.add_argument("--nm-api", default="http://localhost:8042",
                   help="the container log server, as the portal reaches it ('' to not check it)")
    w.set_defaults(fn=_portal)
    u = sub.add_parser("up", help="generate what is needed and run History Server + container logs + portal",
                       description=UP_DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter)
    u.add_argument("scenarios", nargs="*", help="scenario ids (default: all)")
    u.add_argument("--open", action="store_true", help="open the portal in a browser once ready")
    u.add_argument("--port", default=8000, type=int, help="the portal's port")
    u.add_argument("--hs-port", default=18080, type=int, help="the History Server's port")
    u.add_argument("--nm-port", default=8042, type=int, help="the container log server's port")
    u.add_argument("--event-logs", default=str(_root() / "event-logs"))
    u.add_argument("--yarn-logs", default=str(_root() / "yarn-logs"))
    u.add_argument("--no-generate", action="store_true", help="serve what is there, generate nothing")
    u.add_argument("--force", action="store_true", help="regenerate every app")
    u.add_argument("--timeout", default=300, type=float, help="seconds to wait for the History Server")
    u.set_defaults(fn=_up)
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
