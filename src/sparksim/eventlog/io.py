"""Read and write event-log files (UTF-8 JSON lines, as `EventLoggingListener` writes them)."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path

from .events import Event
from .jsonfmt import dumps


def event_to_line(ev: Event) -> str:
    return dumps(ev.to_json())


def line_to_event(line: str) -> Event:
    return Event.decode(json.loads(line))


def read_event_log(path: str | os.PathLike) -> Iterator[Event]:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield line_to_event(line)


def write_event_log(path: str | os.PathLike, events: Iterable[Event]) -> Path:
    """Write `events` to `path` atomically: write a hidden temp file, then rename it.

    The History Server skips hidden files, so it never sees a half-written log.

    File names must not contain dots: the History Server treats any extension as a
    compression codec (EVENT_LOG_FORMAT.md §2).
    """
    path = Path(path)
    if "." in path.name:
        raise ValueError(f"event-log file name must not contain '.': {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        for ev in events:
            fh.write(event_to_line(ev))
            fh.write("\n")
    os.replace(tmp, path)
    return path
