"""A scenario's documents for the portal: `ticket.md`, `hints.md` (one hint per `<details>` block) and
`solution.md`, rendered from Markdown (CommonMark + GitHub tables, inline HTML allowed: the files are ours)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from markdown_it import MarkdownIt

DEFAULT_HS_URL = "http://localhost:18080"

_MD = MarkdownIt("commonmark", {"html": True}).enable("table")
_HINT = re.compile(r"<details>\s*<summary>(.*?)</summary>(.*?)</details>", re.S)
_LOCAL_URL = re.compile(r"<code>(https?://localhost:\d+[^<\s]*)</code>")


def render(text: str, hs_url: str = DEFAULT_HS_URL) -> str:
    """Markdown → HTML; wide tables scroll on their own, localhost URLs in code spans become links."""
    if hs_url != DEFAULT_HS_URL:
        text = text.replace(DEFAULT_HS_URL, hs_url)
    out = _MD.render(text)
    out = out.replace("<table>", '<div class="table-wrap"><table>').replace("</table>", "</table></div>")
    return _LOCAL_URL.sub(r'<a href="\1" target="sparkui" rel="noopener"><code>\1</code></a>', out)


def render_inline(text: str) -> str:
    return _MD.renderInline(text.strip())


@dataclass(frozen=True)
class Hint:
    title: str  # inline HTML
    body: str  # HTML


def parse_hints(text: str, hs_url: str = DEFAULT_HS_URL) -> list[Hint]:
    """Each `<details><summary>title</summary> … </details>` block of hints.md is one hint, in file order."""
    return [Hint(render_inline(t), render(b.strip(), hs_url)) for t, b in _HINT.findall(text)]


@dataclass(frozen=True)
class Documents:
    ticket: str | None
    hints: list[Hint]
    solution: str | None


def documents(scenario_dir: Path, hs_url: str = DEFAULT_HS_URL) -> Documents:
    """Read and render a scenario's documents (read on every request: edits show without a restart)."""

    def read(name: str) -> str | None:
        p = scenario_dir / name
        return p.read_text(encoding="utf-8") if p.is_file() else None

    ticket, hints, solution = read("ticket.md"), read("hints.md"), read("solution.md")
    return Documents(
        ticket=render(ticket, hs_url) if ticket is not None else None,
        hints=parse_hints(hints, hs_url) if hints is not None else [],
        solution=render(solution, hs_url) if solution is not None else None,
    )
