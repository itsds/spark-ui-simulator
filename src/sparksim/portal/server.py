"""The scenario portal: scenario cards, the incident ticket, progressive hints, the solution, and deep links
into the History Server for every run. Standard-library HTTP server; pages are rendered here, a small
script reveals hints and the solution, keeps notes in the browser and polls `/api/status`.

    GET /                  scenario cards (difficulty; topics behind a spoiler)
    GET /s/<scenario id>   ticket, hints, notes, solution; the runs with their History Server links
    GET /api/status        JSON: per app id, generated / loading / ready; History Server and log server up
    GET /static/<file>     portal.css, portal.js
"""

from __future__ import annotations

import html
import json
import mimetypes
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from ..prepare import Manifest
from ..scenario import Scenario
from .content import DEFAULT_HS_URL, documents
from .status import STATE_TEXT, Probe, ScenarioCache, status

STATIC = Path(__file__).resolve().parent / "static"
ROLE_TEXT = {"baseline": "Run", "last_good": "Last good run", "incident": "Incident run"}
DIFFICULTY_TEXT = {"intro": "Intro", "easy": "Easy", "medium": "Medium", "hard": "Hard"}
# History Server pages a learner starts from, per run (`/history/<app>/<attempt>/<page>`).
HS_PAGES = (("Jobs", "jobs/"), ("Stages", "stages/"), ("SQL / DataFrame", "SQL/"), ("Executors", "executors/"),
            ("Environment", "environment/"))


@dataclass
class PortalConfig:
    event_logs: Path
    hs_url: str = DEFAULT_HS_URL  # the History Server as the browser reaches it
    hs_api: str | None = None  # ... and as this server reaches it (default: hs_url)
    nm_api: str | None = "http://localhost:8042"  # the mock NodeManager log server, as this server reaches it
    scenarios_root: Path | None = None


def esc(s) -> str:
    return html.escape(str(s), quote=True)


def hs_link(cfg: PortalConfig, app_id: str, page: str = "jobs/", attempt: str = "1") -> str:
    return f"{cfg.hs_url.rstrip('/')}/history/{app_id}/{attempt}/{page}"


# ---------------------------------------------------------------------------- page pieces


def layout(title: str, body: str, page: str) -> bytes:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<link rel="stylesheet" href="/static/portal.css">
</head>
<body data-page="{esc(page)}">
<header class="top">
  <div class="wrap top-inner">
    <a class="brand" href="/">Spark UI Simulator <span>incident practice</span></a>
    <nav class="services" aria-label="Services">
      <span class="svc" data-svc="history_server"><i></i>History Server</span>
      <span class="svc" data-svc="nm_logs"><i></i>Container logs</span>
    </nav>
  </div>
</header>
<main class="wrap">
{body}
</main>
<footer class="wrap foot">
  Every page you debug in is the unmodified Apache Spark 3.5.9 History Server, reading simulated event logs.
  Not affiliated with the Apache Software Foundation.
</footer>
<script src="/static/portal.js"></script>
</body>
</html>
""".encode()


def badge_difficulty(level: str) -> str:
    return f'<span class="difficulty d-{esc(level)}">{esc(DIFFICULTY_TEXT.get(level, level))}</span>'


def topics_spoiler(sc: Scenario) -> str:
    if not sc.model.topics:
        return ""
    chips = "".join(f'<li>{esc(t)}</li>' for t in sc.model.topics)
    return f'<details class="topics"><summary>Topics (spoiler)</summary><ul class="chips">{chips}</ul></details>'


def run_state_badge(app_id: str) -> str:
    return f'<span class="state" data-app="{esc(app_id)}" data-state="unknown">Checking…</span>'


def scenario_number(sc: Scenario) -> str:
    return sc.id.split("-", 1)[0]


def index_page(cfg: PortalConfig, scenarios: list[Scenario]) -> bytes:
    cards = []
    for sc in scenarios:
        m = sc.model
        runs = "".join(
            f'<li><span class="role">{esc(ROLE_TEXT[r])}</span>{run_state_badge(sc.app_id(r))}</li>' for r in sc.roles)
        cards.append(f"""
<article class="card scenario-card">
  <div class="card-head"><span class="num">{esc(scenario_number(sc))}</span>{badge_difficulty(m.difficulty)}</div>
  <h2><a href="/s/{esc(sc.id)}">{esc(m.title)}</a></h2>
  <p class="summary">{esc(m.summary)}</p>
  <ul class="run-states">{runs}</ul>
  {topics_spoiler(sc)}
  <a class="button" href="/s/{esc(sc.id)}">Open the ticket</a>
</article>""")
    body = f"""
<section class="intro">
  <h1>Scenarios</h1>
  <p>Each scenario is an incident on a production-scale Spark job (YARN, tens of executors, thousands of tasks).
  Read the ticket, investigate in the <a href="{esc(cfg.hs_url)}" target="sparkui" rel="noopener">Spark History
  Server</a> the way you would at work, write down your diagnosis, then check it against the solution.
  Open the hints one at a time, only when you are stuck.</p>
</section>
<section class="grid">{"".join(cards) or '<p>No scenarios found.</p>'}</section>
"""
    return layout("Scenarios · Spark UI Simulator", body, "index")


def run_label(sc: Scenario, role: str) -> str:
    """The run's label without a leading "<role>: " (the panel shows the role already)."""
    label, prefix = sc.model.runs[role].label, ROLE_TEXT[role] + ":"
    return label[len(prefix):].strip() if label.lower().startswith(prefix.lower()) else label


def runs_panel(cfg: PortalConfig, sc: Scenario) -> str:
    items = []
    manifest = Manifest.load(cfg.event_logs)
    for role in sc.roles:
        app = sc.app_id(role)
        attempts = manifest.attempts(app)  # a failed YARN attempt is followed by the next one
        navs = []
        for att in attempts:
            links = "".join(f'<a href="{esc(hs_link(cfg, app, page, att))}" target="sparkui" rel="noopener" '
                            f'data-page="{esc(page)}">{esc(name)}</a>' for name, page in HS_PAGES)
            label = f'<span class="attempt">Attempt {esc(att)}</span>' if len(attempts) > 1 else ""
            navs.append(f'<nav class="hs-links" data-attempt="{esc(att)}" aria-label="History Server pages of '
                        f'{esc(app)}, attempt {esc(att)}">{label}{links}</nav>')
        items.append(f"""
<div class="run" data-run="{esc(app)}">
  <div class="run-head"><span class="role">{esc(ROLE_TEXT[role])}</span>{run_state_badge(app)}</div>
  <p class="run-label">{esc(run_label(sc, role))}</p>
  <code class="app-id">{esc(app)}</code>
  {"".join(navs)}
  <p class="run-note" data-note-for="{esc(app)}" hidden></p>
</div>""")
    return f"""
<section class="card runs">
  <h2>In the History Server</h2>
  {"".join(items)}
  <p class="muted small">Links open in one browser tab named for the Spark UI, so the ticket stays here.
  All applications: <a href="{esc(cfg.hs_url)}" target="sparkui" rel="noopener">{esc(cfg.hs_url)}</a></p>
</section>"""


def scenario_page(cfg: PortalConfig, sc: Scenario) -> bytes:
    m = sc.model
    docs = documents(sc.dir, cfg.hs_url)
    hints = "".join(f"""
<li class="hint" data-hint="{i}" hidden>
  <h3>{h.title}</h3>
  <div class="md">{h.body}</div>
</li>""" for i, h in enumerate(docs.hints))
    n = len(docs.hints)
    hints_block = f"""
<section class="card hints" id="hints" data-count="{n}">
  <h2>Hints</h2>
  <p class="muted">{n} hint{'s' if n != 1 else ''}, from where to look to what to compare. Open the next one only
  when you are stuck.</p>
  <ol class="hint-list">{hints}</ol>
  <div class="actions">
    <button type="button" class="button" data-action="next-hint">Show hint 1 of {n}</button>
    <button type="button" class="button ghost" data-action="hide-hints" hidden>Hide the hints</button>
  </div>
</section>""" if n else ""
    solution = f"""
<section class="card solution" id="solution">
  <h2>Solution</h2>
  <p class="muted">Root cause, the click path, the evidence numbers (checked against the History Server's REST
  API by the project's tests), the fix and a 60-second interview answer.</p>
  <div class="actions">
    <button type="button" class="button" data-action="reveal-solution">Reveal the solution</button>
    <button type="button" class="button ghost" data-action="hide-solution" hidden>Hide it again</button>
  </div>
  <div class="md solution-body" hidden>{docs.solution}</div>
</section>""" if docs.solution else ""
    ticket = docs.ticket or "<p>This scenario has no ticket yet.</p>"
    body = f"""
<p class="crumbs"><a href="/">All scenarios</a></p>
<section class="scenario-head">
  <div class="card-head"><span class="num">{esc(scenario_number(sc))}</span>{badge_difficulty(m.difficulty)}</div>
  <h1>{esc(m.title)}</h1>
  <p class="summary">{esc(m.summary)}</p>
  {topics_spoiler(sc)}
</section>
<div class="layout" data-scenario="{esc(sc.id)}">
  <div class="col-main">
    <section class="card ticket" id="ticket"><div class="md">{ticket}</div></section>
    {hints_block}
    <section class="card notes" id="notes">
      <h2>Your diagnosis</h2>
      <p class="muted">Write it down before you open the solution: the root cause, the evidence (stage, task,
      numbers) and the fix. Saved in this browser only.</p>
      <textarea rows="9" spellcheck="true"
        placeholder="Root cause:&#10;Evidence:&#10;Fix (tonight / durable):"></textarea>
      <div class="actions"><span class="muted small saved" aria-live="polite"></span>
        <button type="button" class="button ghost small" data-action="reset">Reset hints, notes and solution</button>
      </div>
    </section>
    {solution}
  </div>
  <aside class="col-side">
    {runs_panel(cfg, sc)}
  </aside>
</div>"""
    return layout(f"{scenario_number(sc)} · {m.title}", body, "scenario")


def not_found(what: str) -> bytes:
    return layout("Not found · Spark UI Simulator",
                  f'<p class="crumbs"><a href="/">All scenarios</a></p><h1>Not found</h1><p>{esc(what)}</p>', "404")


# ---------------------------------------------------------------------------- server


def make_server(cfg: PortalConfig, host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    scenarios = ScenarioCache(cfg.scenarios_root)
    probe = Probe(cfg.hs_api or cfg.hs_url, cfg.nm_api)

    class Handler(BaseHTTPRequestHandler):
        server_version = "sparksim-portal"

        def log_message(self, fmt, *args):  # quiet
            pass

        def _send(self, code: int, body: bytes, ctype: str = "text/html; charset=utf-8",
                  cache: str = "no-cache") -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", cache)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):  # noqa: N802
            self.do_GET()

        def do_GET(self):  # noqa: N802
            path = unquote(urlparse(self.path).path)
            parts = [p for p in path.split("/") if p]
            try:
                if not parts:
                    return self._send(200, index_page(cfg, scenarios.all()))
                if parts[0] == "s" and len(parts) == 2:
                    try:
                        sc = scenarios.get(parts[1])
                    except KeyError:
                        return self._send(404, not_found(f"No scenario {parts[1]!r}."))
                    return self._send(200, scenario_page(cfg, sc))
                if parts == ["api", "status"]:
                    st = status(scenarios.all(), cfg.event_logs, probe)
                    st["history_server"]["url"] = cfg.hs_url
                    st["states"] = STATE_TEXT
                    return self._send(200, json.dumps(st).encode(), "application/json")
                if parts[0] == "static" and len(parts) == 2 and (STATIC / parts[1]).is_file() \
                        and parts[1] in {p.name for p in STATIC.iterdir()}:
                    ctype = mimetypes.guess_type(parts[1])[0] or "application/octet-stream"
                    if ctype.startswith("text/") or ctype.endswith("javascript"):
                        ctype += "; charset=utf-8"
                    return self._send(200, (STATIC / parts[1]).read_bytes(), ctype)
                if parts == ["favicon.ico"]:
                    return self._send(204, b"")
                return self._send(404, not_found(f"No page {path!r}."))
            except BrokenPipeError:
                pass

    return ThreadingHTTPServer((host, port), Handler)


def serve(cfg: PortalConfig, host: str = "127.0.0.1", port: int = 8000) -> None:
    srv = make_server(cfg, host, port)
    shown = "localhost" if host in ("127.0.0.1", "0.0.0.0") else host
    print(f"scenario portal -> http://{shown}:{port}/   (event logs: {cfg.event_logs}, History Server: "
          f"{cfg.hs_url})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
