"""The scenario portal (ADR-031): rendered documents, progressive hints, run states, and its pages."""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sparksim.portal import PortalConfig, make_server
from sparksim.portal.content import documents, parse_hints, render
from sparksim.portal.status import Probe, ScenarioCache, status
from sparksim.prepare import Manifest, app_fingerprint
from sparksim.scenario import load_scenario

S00, S01 = "00-healthy-baseline", "01-join-skew"


@contextmanager
def serve(server):
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


@contextmanager
def fake_history_server(listed: set[str]):
    """Answers /api/v1/applications with `listed` (mutable: tests change it)."""

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):  # noqa: N802
            body = json.dumps([{"id": a, "attempts": [{"attemptId": "1"}]} for a in sorted(listed)]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with serve(ThreadingHTTPServer(("127.0.0.1", 0), H)) as url:
        yield url


def get(url: str) -> tuple[int, str, str]:
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return r.status, r.headers.get("Content-Type", ""), r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type", ""), e.read().decode()


# ---------------------------------------------------------------------------- documents


@pytest.mark.parametrize("sid", [S00, S01])
def test_documents_render(sid):
    sc = load_scenario(sid)
    docs = documents(sc.dir)
    assert len(docs.hints) == 8
    assert all(h.title.startswith(f"Hint {i}:") for i, h in enumerate(docs.hints, 1))
    assert all("<details" not in h.body and "<p>" in h.body for h in docs.hints)
    for html in (docs.ticket, docs.solution):
        assert '<div class="table-wrap"><table>' in html  # the ticket's header table, the evidence table
        assert "**" not in html and "\n| " not in html  # no raw Markdown left
    assert "<h2>Evidence</h2>" in docs.solution


def test_localhost_urls_become_links_and_follow_the_history_server_port():
    out = render("History Server: `http://localhost:18080` (`uv run sparksim up`).", "http://localhost:28080")
    link = '<a href="http://localhost:28080" target="sparkui" rel="noopener"><code>http://localhost:28080</code></a>'
    assert link in out
    assert "<code>uv run sparksim up</code>" in out


def test_hints_are_the_details_blocks_in_order():
    hints = parse_hints("# Hints\n\n<details><summary>Hint 1: `a`</summary>\n\nFirst **one**.\n</details>\n\n"
                        "<details><summary>Hint 2: b</summary>\n\nSecond.\n</details>\n")
    assert [h.title for h in hints] == ["Hint 1: <code>a</code>", "Hint 2: b"]
    assert hints[0].body == "<p>First <strong>one</strong>.</p>\n"


# ---------------------------------------------------------------------------- run states


def write_log(ev, sc, role, fingerprint=None):
    app = sc.app_id(role)
    (ev / f"{app}_1").write_text("{}\n")
    m = Manifest.load(ev)
    m.record(app, fingerprint or app_fingerprint(sc, role), [sc.id], f"{app}_1")
    m.save()
    return app


def test_run_states(tmp_path):
    ev = tmp_path
    scs = ScenarioCache().all()
    s01 = load_scenario(S01)
    good, bad = s01.app_id("last_good"), s01.app_id("incident")
    listed: set[str] = set()
    with fake_history_server(listed) as hs:
        probe = Probe(hs, None, ttl=0)
        st = status(scs, ev, probe)
        assert st["history_server"]["up"] and not st["nm_logs"]["up"]
        assert {good, bad} <= set(st["apps"])  # one entry per app of every scenario
        assert {i["state"] for i in st["apps"].values()} == {"missing"}
        assert st["apps"][good]["scenarios"] == [S00, S01]

        write_log(ev, s01, "last_good")
        write_log(ev, s01, "incident", fingerprint="older")
        assert status(scs, ev, probe)["apps"][good]["state"] == "loading"
        listed.add(good)
        st = status(scs, ev, probe)
        assert st["apps"][good] | {"scenarios": None} == {"state": "ready", "text": "Ready", "stale": False,
                                                          "attempt": "1", "scenarios": None}
        assert st["apps"][bad]["state"] == "loading" and st["apps"][bad]["stale"]

        m = Manifest.load(ev)
        m.progress = {"pending": [bad], "generating": bad, "updated": time.time()}
        m.save()
        st = status(scs, ev, probe)
        assert st["apps"][bad]["state"] == "generating" and st["generating"] == bad
        (ev / f"{bad}_1").unlink()
        m.progress = {"pending": [bad], "generating": None, "updated": time.time()}
        m.save()
        assert status(scs, ev, probe)["apps"][bad]["state"] == "queued"
        assert status(scs, ev, probe)["apps"][good]["state"] == "ready"  # fresh: not queued

    down = Probe("http://127.0.0.1:9", None, ttl=0)  # nothing listens on the discard port
    st = status(scs, ev, down)
    assert not st["history_server"]["up"]
    assert st["apps"][good]["state"] == "hs-down"


# ---------------------------------------------------------------------------- pages


def test_pages(tmp_path):
    s01 = load_scenario(S01)
    write_log(tmp_path, s01, "last_good")
    with fake_history_server({s01.app_id("last_good")}) as hs:
        cfg = PortalConfig(event_logs=tmp_path, hs_url="http://localhost:18080", hs_api=hs, nm_api=None)
        with serve(make_server(cfg, "127.0.0.1", 0)) as url:
            code, ctype, index = get(url + "/")
            assert code == 200 and ctype.startswith("text/html")
            for sid in (S00, S01):
                sc = load_scenario(sid)
                assert f'href="/s/{sid}"' in index and sc.model.title.replace("'", "&#x27;") in index
                # topics are spoilers: only inside the collapsed <details>
                for t in sc.model.topics:
                    assert re.search(r"Topics \(spoiler\)</summary><ul class=\"chips\">.*" + re.escape(t), index)

            code, _, page = get(url + f"/s/{S01}")
            assert code == 200
            for role in ("last_good", "incident"):
                app = s01.app_id(role)
                for p in ("jobs/", "stages/", "SQL/", "executors/", "environment/"):
                    assert f'href="http://localhost:18080/history/{app}/1/{p}"' in page
            assert len(re.findall(r'<li class="hint" data-hint="\d" hidden>', page)) == 8
            assert '<div class="md solution-body" hidden>' in page
            assert "DATAOPS-1194" in page and "<textarea" in page
            assert ">Last good run<" in page and ">2026-11-03 (dt=2026-11-02)<" in page  # label without its role

            code, ctype, body = get(url + "/api/status")
            st = json.loads(body)
            assert code == 200 and ctype == "application/json"
            assert st["history_server"]["url"] == "http://localhost:18080"
            assert st["apps"][s01.app_id("last_good")]["state"] == "ready"
            assert st["apps"][s01.app_id("incident")]["state"] == "missing"

            assert get(url + "/static/portal.css")[:2] == (200, "text/css; charset=utf-8")
            assert get(url + "/static/portal.js")[0] == 200
            for missing in ("/s/99-nope", "/static/nope.css", "/static/..%2F__init__.py", "/x"):
                assert get(url + missing)[0] == 404, missing
