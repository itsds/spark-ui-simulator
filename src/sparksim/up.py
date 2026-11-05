"""`sparksim up`: everything needed to debug the scenarios, with one command and without Docker (ADR-032).

    uv run sparksim up [SCENARIO ...] [--open]

Starts the mock NodeManager log server (services/nm-logs), the real Spark 3.5.9 History Server
(services/history-server/run.sh: PySpark's bundled Spark on JDK 17) and the scenario portal; meanwhile it
generates every app the scenarios need that is missing or out of date (`sparksim prepare`), waits until the
History Server lists them all, prints where to go, and runs until Ctrl+C, which stops everything. Server
output goes to build/logs/. `docker compose up` is the same stack in containers.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path

from .prepare import plan_apps, prepare
from .scenario import load_scenario, scenario_ids

ROOT = Path(__file__).resolve().parents[2]
LOG_URL = "http://localhost:{port}/node/containerlogs/{{{{CONTAINER_ID}}}}/{{{{USER}}}}/{{{{FILE_NAME}}}}?start=-4096"


@dataclass
class UpOptions:
    scenarios: list[str] = field(default_factory=list)  # empty: every scenario
    event_logs: Path = ROOT / "event-logs"
    yarn_logs: Path = ROOT / "yarn-logs"
    portal_port: int = 8000
    hs_port: int = 18080
    nm_port: int = 8042
    host: str = "127.0.0.1"  # where the portal and the log server listen
    generate: bool = True
    force: bool = False
    open_browser: bool = False
    timeout: float = 300.0  # how long to wait for the History Server to list every app


class Stop(Exception):
    pass


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
        except OSError:
            return True
    return False


def listed_apps(hs: str) -> set[str] | None:
    try:
        with urllib.request.urlopen(f"{hs}/api/v1/applications", timeout=3) as r:  # noqa: S310 - local
            return {a["id"] for a in json.loads(r.read().decode())}
    except (OSError, ValueError):
        return None


def tail(path: Path, n: int = 25) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-n:])
    except OSError:
        return "(no output)"


def spark_home() -> str:
    if os.environ.get("SPARK_HOME"):
        return os.environ["SPARK_HOME"]
    try:
        import pyspark
    except ImportError as e:
        raise SystemExit("The History Server comes with PySpark, which is not installed: run `uv sync` "
                         "(the default groups include it).") from e
    return os.path.dirname(pyspark.__file__)


def up(o: UpOptions) -> int:
    if not (ROOT / "services" / "history-server" / "run.sh").is_file():
        raise SystemExit("sparksim up runs from a clone of the repository (services/ not found)")
    scs = [load_scenario(s) for s in (o.scenarios or scenario_ids())]
    plans = plan_apps(scs)
    busy = [f"{p} ({what})" for p, what in ((o.portal_port, "portal"), (o.hs_port, "History Server"),
                                            (o.nm_port, "container logs")) if port_in_use(p)]
    if busy:
        raise SystemExit(f"port {', '.join(busy)} already in use: is the stack already running (another "
                         "`sparksim up` or `docker compose up`)? Stop it, or choose other ports (see --help).")
    o.event_logs.mkdir(parents=True, exist_ok=True)
    o.yarn_logs.mkdir(parents=True, exist_ok=True)
    logs = ROOT / "build" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    hs_api, nm_api = f"http://127.0.0.1:{o.hs_port}", f"http://127.0.0.1:{o.nm_port}"
    portal_url, hs_url = f"http://localhost:{o.portal_port}", f"http://localhost:{o.hs_port}"

    def on_term(signum, frame):
        raise Stop()

    # Ctrl+C and SIGTERM stop the stack. SIGINT explicitly too: a shell that starts us in the background
    # without job control leaves it ignored, and Python then never raises KeyboardInterrupt.
    old_handlers = {s: signal.signal(s, on_term) for s in (signal.SIGTERM, signal.SIGINT)}
    children: list[tuple[str, subprocess.Popen, Path]] = []
    portal = None
    try:
        nm_log = logs / "nm-logs.log"
        with open(nm_log, "w") as fh:
            children.append(("container log server", subprocess.Popen(
                [sys.executable, str(ROOT / "services" / "nm-logs" / "server.py"), "--dir", str(o.yarn_logs),
                 "--port", str(o.nm_port), "--host", o.host],
                stdout=fh, stderr=subprocess.STDOUT, start_new_session=True), nm_log))
        env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}  # a local-only JVM
        env["SPARK_HOME"] = spark_home()
        env["SPARK_HISTORY_OPTS"] = (env.get("SPARK_HISTORY_OPTS", "") + " -Dspark.history.custom.executor.log.url="
                                     + LOG_URL.format(port=o.nm_port)).strip()
        hs_log = logs / "history-server.log"
        with open(hs_log, "w") as fh:
            children.append(("History Server", subprocess.Popen(
                [str(ROOT / "services" / "history-server" / "run.sh"), str(o.event_logs), str(o.hs_port)],
                env=env, stdout=fh, stderr=subprocess.STDOUT, start_new_session=True), hs_log))

        from .portal import PortalConfig, make_server

        portal = make_server(PortalConfig(event_logs=o.event_logs, hs_url=hs_url, hs_api=hs_api, nm_api=nm_api),
                             o.host, o.portal_port)
        threading.Thread(target=portal.serve_forever, daemon=True).start()
        print(f"Scenario portal       {portal_url}\n"
              f"Spark History Server  {hs_url}   (starting; output in {hs_log.relative_to(ROOT)})\n"
              f"Container logs        http://localhost:{o.nm_port}\n", flush=True)

        def check_children():
            for name, proc, log in children:
                if proc.poll() is not None:
                    print(f"\nThe {name} stopped (exit code {proc.returncode}). Last lines of {log}:\n{tail(log)}",
                          file=sys.stderr, flush=True)
                    raise SystemExit(1)

        if o.generate:
            print(f"Event logs for {len(plans)} application(s) in {o.event_logs}:", flush=True)
            prepare(scs, o.event_logs, o.yarn_logs, force=o.force, log=lambda s: print("  " + s, flush=True))
            check_children()
        want = {p.app_id for p in plans}
        print(f"\nWaiting for the History Server to list {len(want)} application(s) ...", flush=True)
        deadline = time.monotonic() + o.timeout
        while True:
            check_children()
            got = listed_apps(hs_api)
            if got is not None and want <= got:
                break
            if time.monotonic() > deadline:
                missing = sorted(want - (got or set()))
                print(f"Still not listed after {o.timeout:.0f} s: {', '.join(missing)}. The portal shows each "
                      f"run's state; the History Server's output is in {hs_log}.", file=sys.stderr, flush=True)
                break
            time.sleep(1)
        width = max(len(sc.id) for sc in scs)
        lines = "\n".join(f"  {sc.id:{width}s}  {portal_url}/s/{sc.id}" for sc in scs)
        print(f"\nReady. Open the portal: {portal_url}\n{lines}\nCtrl+C stops everything.", flush=True)
        if o.open_browser:
            webbrowser.open(portal_url)
        while True:
            check_children()
            time.sleep(1)
    except (KeyboardInterrupt, Stop):
        print("\nStopping ...", flush=True)
    finally:
        for s in old_handlers:  # a second Ctrl+C must not interrupt the clean-up and orphan a server
            signal.signal(s, signal.SIG_IGN)
        if portal is not None:
            portal.shutdown()
            portal.server_close()
        for _, proc, _ in children:
            if proc.poll() is None:
                proc.terminate()
        for _, proc, _ in children:
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
        for s, h in old_handlers.items():
            signal.signal(s, h)
    return 0
