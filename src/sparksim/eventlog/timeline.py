"""Order simulated events the way Spark's listener bus would log them.

Generators schedule events at simulated times. `Timeline.events()` sorts them
by (time, insertion order) and only then builds each one. Building at emission
time matters because some fields depend on what came before: a TaskEnd's
accumulator `Value` is the running total over the TaskEnds already logged.

A real log isn't strictly time-sorted. Its preamble (LogStart,
ResourceProfileAdded, driver BlockManagerAdded, EnvironmentUpdate,
ApplicationStart) is written in a fixed order even though ApplicationStart
carries the earliest timestamp. Pass such events as `preamble`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .events import Event

EventOrFactory = Event | Callable[[], Event]


class Timeline:
    def __init__(self) -> None:
        self._items: list[tuple[int, int, EventOrFactory]] = []

    def add(self, time_ms: int, ev: EventOrFactory) -> None:
        self._items.append((int(time_ms), len(self._items), ev))

    def events(self, preamble: Iterable[Event] = ()) -> list[Event]:
        out = list(preamble)
        for _, _, ev in sorted(self._items, key=lambda x: (x[0], x[1])):
            out.append(ev() if callable(ev) else ev)
        return out
