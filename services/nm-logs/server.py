#!/usr/bin/env python3
"""Mock YARN NodeManager log server for the generated container logs.

Serves `<log dir>/<appId>/<containerId>/{stdout,stderr}` (written by `sparksim generate`) under the
NodeManager web UI's paths, so the Spark History Server's executor log links work:

    /node/containerlogs/<containerId>/<user>/<file>?start=-4096   last 4 KiB (the link Spark builds)
    /node/containerlogs/<containerId>/<user>/<file>?start=0       the whole log
    /node/containerlogs/<containerId>/<user>                      the container's log files
    /                                                             applications and containers

`start`/`end` are byte offsets as in Hadoop's ContainerLogsPage (negative: from the end). The page
mirrors what a finished application's logs look like on YARN (log aggregation on): log type, upload
time, length, the "Showing N bytes" line and the text. Standard library only.

    python3 services/nm-logs/server.py [--dir yarn-logs] [--port 8042]
"""

from __future__ import annotations

import argparse
import html
import re
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

CONTAINER = re.compile(r"^container_(?:e\d+_)?(\d+)_(\d+)_\d+_\d+$")
STYLE = ("body{font:13px/1.4 Verdana,Arial,sans-serif;margin:0;color:#222}"
         "#hd{background:#1d3e5c;color:#fff;padding:8px 14px}#hd h1{font-size:18px;margin:0}"
         "#nav{float:left;width:150px;padding:10px 14px;font-size:12px}#nav a{color:#1d3e5c}"
         "#main{margin-left:180px;padding:10px 14px}p{margin:4px 0}"
         "pre{background:#f7f7f7;border:1px solid #ddd;padding:8px;white-space:pre-wrap;word-break:break-all}"
         "table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:3px 8px;text-align:left}")


def app_of(container_id: str) -> str | None:
    m = CONTAINER.match(container_id)
    return f"application_{m.group(1)}_{m.group(2)}" if m else None


def page(title: str, body: str, nav: str = "") -> bytes:
    return (f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{html.escape(title)}</title>"
            f"<style>{STYLE}</style></head><body><div id='hd'><h1>{html.escape(title)}</h1></div>"
            f"<div id='nav'><a href='/'>Applications</a>{nav}</div><div id='main'>{body}</div></body></html>"
            ).encode()


def byte_range(length: int, start: str | None, end: str | None) -> tuple[int, int]:
    """ContainerLogsPage: start/end may be negative (relative to the end); default start=-4096."""
    s = int(start) if start not in (None, "") else -4096
    e = int(end) if end not in (None, "") else length
    s = max(0, length + s) if s < 0 else min(s, length)
    e = max(0, length + e) if e < 0 else min(e, length)
    return s, max(s, e)


class Handler(BaseHTTPRequestHandler):
    log_dir: Path = Path("yarn-logs")
    server_version = "Jetty(9.4.51.v20230217)"

    def log_message(self, fmt, *args):  # quiet
        pass

    def _send(self, code: int, body: bytes, ctype: str = "text/html; charset=utf-8") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if not parts:
            return self._index()
        if parts[:2] == ["node", "containerlogs"] and len(parts) in (4, 5):
            container, user = parts[2], parts[3]
            if len(parts) == 4:
                return self._files(container, user)
            return self._log(container, user, parts[4], q)
        self._send(404, page("Not found", f"<p>No such page: {html.escape(url.path)}</p>"))

    def _container_dir(self, container: str) -> Path | None:
        app = app_of(container)
        if app is None:
            return None
        d = self.log_dir / app / container
        return d if d.is_dir() else None

    def _index(self):
        rows = []
        for app in sorted(p for p in self.log_dir.iterdir() if p.is_dir()) if self.log_dir.is_dir() else []:
            for c in sorted(p.name for p in app.iterdir() if p.is_dir()):
                links = " ".join(f"<a href='/node/containerlogs/{c}/-/{f}?start=0'>{f}</a>"
                                 for f in ("stdout", "stderr") if (app / c / f).exists())
                rows.append(f"<tr><td>{app.name}</td><td>{c}</td><td>{links}</td></tr>")
        body = ("<table><tr><th>Application</th><th>Container</th><th>Logs</th></tr>" + "".join(rows) + "</table>"
                if rows else "<p>No container logs. Run <code>uv run sparksim generate ...</code> first.</p>")
        self._send(200, page("NodeManager container logs", body))

    def _files(self, container: str, user: str):
        d = self._container_dir(container)
        if d is None:
            return self._send(404, page(f"Logs for {container}", f"<p>Container {html.escape(container)} not found."
                                        "</p>"))
        items = "".join(f"<p>Log Type: <a href='/node/containerlogs/{container}/{user}/{f.name}?start=-4096'>"
                        f"{f.name}</a> ({f.stat().st_size} bytes)</p>" for f in sorted(d.iterdir()))
        self._send(200, page(f"Logs for {container}", items))

    def _log(self, container: str, user: str, name: str, q: dict):
        d = self._container_dir(container)
        f = d / name if d is not None and name in ("stdout", "stderr") else None
        if f is None or not f.exists():
            body = f"<p>Logs not available for {html.escape(container)}: no such log file {html.escape(name)}.</p>"
            return self._send(404, page(f"Logs for {container}", body))
        data = f.read_bytes()
        s, e = byte_range(len(data), q.get("start"), q.get("end"))
        uploaded = datetime.fromtimestamp(f.stat().st_mtime, UTC).strftime("%a %b %d %H:%M:%S %z %Y")
        base = f"/node/containerlogs/{container}/{user}/{name}"
        showing = ""
        if (s, e) != (0, len(data)):
            showing = (f"<p>Showing {e - s} bytes of {len(data)} total. Click <a href='{base}?start=0'>here</a> "
                       "for the full log.</p>")
        nav = f"<p><a href='/node/containerlogs/{container}/{user}'>{container}</a></p>"
        body = (f"<p>Log Type: {name}</p><p>Log Upload Time: {uploaded}</p><p>Log Length: {len(data)}</p>"
                f"{showing}<pre>{html.escape(data[s:e].decode('utf-8', 'replace'))}</pre>")
        self._send(200, page(f"Logs for {container}", body, nav))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default="yarn-logs", type=Path, help="generated container logs")
    ap.add_argument("--port", default=8042, type=int)
    ap.add_argument("--host", default="0.0.0.0")
    a = ap.parse_args(argv)
    Handler.log_dir = a.dir.resolve()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"mock NodeManager logs -> http://localhost:{a.port}/   (logs: {Handler.log_dir})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
