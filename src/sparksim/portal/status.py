"""Where each scenario run stands: generated (the manifest, ADR-032), listed by the History Server, and
whether the mock NodeManager log server answers. Polled by the portal's pages through `/api/status`."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from ..prepare import Manifest, app_state, plan_apps
from ..scenario import Scenario, load_scenario, scenario_ids, scenarios_dir

# What a run's badge says, by state.
STATE_TEXT = {
    "ready": "Ready",
    "loading": "Loading into the History Server",
    "generating": "Generating",
    "queued": "Queued for generation",
    "missing": "Not generated",
    "hs-down": "History Server not reachable",
}


def _get_json(url: str, timeout: float = 2.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - local services only
            return json.loads(r.read().decode("utf-8"))
    except (OSError, ValueError):
        return None


def _reachable(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout):  # noqa: S310
            return True
    except OSError:
        return False


@dataclass
class Probe:
    """What the History Server lists and whether the NodeManager mock answers, cached for `ttl` seconds."""

    hs_api: str
    nm_api: str | None
    ttl: float = 2.0
    _at: float = 0.0
    _value: tuple = (None, False)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self) -> tuple[dict[str, str] | None, bool]:
        """({app id: attempt id} or None when the History Server is down, NodeManager mock up)."""
        with self._lock:
            if time.monotonic() - self._at < self.ttl:
                return self._value
            apps = _get_json(f"{self.hs_api.rstrip('/')}/api/v1/applications")
            listed = None
            if isinstance(apps, list):
                listed = {a["id"]: str((a.get("attempts") or [{}])[0].get("attemptId", "")) for a in apps}
            nm_up = _reachable(self.nm_api.rstrip("/") + "/") if self.nm_api else False
            self._value, self._at = (listed, nm_up), time.monotonic()
            return self._value


class ScenarioCache:
    """Scenarios loaded from their scenario.yaml, reloaded when the file changes."""

    def __init__(self, root: Path | None = None):
        self.root = root or scenarios_dir()
        self._cache: dict[str, tuple[float, Scenario]] = {}
        self._lock = threading.Lock()

    def ids(self) -> list[str]:
        return scenario_ids(self.root)

    def get(self, sid: str) -> Scenario:
        """KeyError for an id that has no scenario.yaml."""
        y = self.root / sid / "scenario.yaml"
        if sid not in self.ids():
            raise KeyError(sid)
        mtime = y.stat().st_mtime
        with self._lock:
            hit = self._cache.get(sid)
            if hit is None or hit[0] != mtime:
                self._cache[sid] = hit = (mtime, load_scenario(y))
            return hit[1]

    def all(self) -> list[Scenario]:
        return [self.get(s) for s in self.ids()]


def status(scenarios: list[Scenario], event_logs: Path, probe: Probe) -> dict:
    """{"history_server": {...}, "nm_logs": {...}, "generating": ..., "apps": {app id: {...}}}."""
    listed, nm_up = probe.get()
    m = Manifest.load(event_logs)
    progress = m.active_progress()
    apps = {}
    for p in plan_apps(scenarios):
        st = app_state(p, event_logs, None, m)
        if progress and p.app_id == progress.get("generating"):
            state = "generating"
        elif progress and p.app_id in progress.get("pending", ()) and st != "fresh":
            state = "queued"
        elif st == "missing":
            state = "missing"
        elif listed is None:
            state = "hs-down"
        elif p.app_id not in listed:
            state = "loading"
        else:
            state = "ready"
        apps[p.app_id] = {"state": state, "text": STATE_TEXT[state], "stale": st == "stale",
                          "attempt": (listed or {}).get(p.app_id) or "1", "scenarios": p.scenario_ids}
    return {
        "history_server": {"up": listed is not None},
        "nm_logs": {"up": nm_up},
        "generating": progress.get("generating") if progress else None,
        "apps": apps,
    }
