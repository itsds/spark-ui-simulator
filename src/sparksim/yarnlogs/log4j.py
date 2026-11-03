"""Spark's default log4j2 layout and the formatting helpers its messages use.

`org/apache/spark/log4j2-defaults.properties`: `%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex`, root level INFO,
written to the container's stderr. Seen in every reference container log (`reference/yarn-logs`).
The containers' time zone is the cluster's (UTC, like `user.timezone` in the Environment tab).
"""

from __future__ import annotations

import heapq
from datetime import UTC, datetime

BANNER = "Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties"


def ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%y/%m/%d %H:%M:%S")


_SI = [(1 << 60, "EiB"), (1 << 50, "PiB"), (1 << 40, "TiB"), (1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")]


def bytes_to_string(size: int) -> str:
    """`Utils.bytesToString(Long)`: the largest unit the size reaches twice, one decimal ("1536.0 MiB")."""
    for unit, suffix in _SI:
        if size >= 2 * unit:
            return f"{size / unit:.1f} {suffix}"
    return f"{float(size):.1f} B"


class ContainerLog:
    """Lines of one container log, kept in time order (ties: insertion order, like one thread's writes)."""

    def __init__(self):
        self._heap: list[tuple[int, int, str]] = []
        self._seq = 0
        self.cutoff: int | None = None  # the JVM died: nothing after this is written

    def add(self, t: int, level: str, logger: str, msg: str) -> None:
        if self.cutoff is not None and t > self.cutoff:
            return
        heapq.heappush(self._heap, (t, self._seq, f"{ts(t)} {level} {logger}: {msg}"))
        self._seq += 1

    def info(self, t: int, logger: str, msg: str) -> None:
        self.add(t, "INFO", logger, msg)

    def warn(self, t: int, logger: str, msg: str) -> None:
        self.add(t, "WARN", logger, msg)

    def error(self, t: int, logger: str, msg: str) -> None:
        self.add(t, "ERROR", logger, msg)

    def raw(self, t: int, text: str) -> None:
        """A line without the layout (stack traces, multi-line messages continue like this)."""
        if self.cutoff is not None and t > self.cutoff:
            return
        heapq.heappush(self._heap, (t, self._seq, text))
        self._seq += 1

    def text(self, banner: bool = True) -> str:
        lines = [BANNER] if banner else []
        lines += [line for _, _, line in sorted(self._heap)]
        return "\n".join(lines) + "\n"
