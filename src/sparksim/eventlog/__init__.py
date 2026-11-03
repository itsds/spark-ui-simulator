"""Spark 3.5 event-log model: typed events serialized exactly like `JsonProtocol`.

Layers:
* `codec`, `jsonfmt`: dataclass <-> JSON mapping and Jackson-identical bytes.
* `common`, `metrics`, `reasons`, `events`: one dataclass per Spark type/event.
* `io`: read/write event-log files.
* `driver`: what Spark's driver derives on its own (accumulator values, internal
  task-metric accumulables, stage accumulables, IDs), so generators only supply
  task-level facts.
"""

from .codec import DecodeError
from .common import *  # noqa: F403
from .driver import AccumulatorDef, DriverLedger, IdAllocator
from .events import *  # noqa: F403
from .io import event_to_line, line_to_event, read_event_log, write_event_log
from .metrics import *  # noqa: F403
from .reasons import *  # noqa: F403

__all__ = [  # noqa: F405
    "AccumulatorDef", "DecodeError", "DriverLedger", "IdAllocator", "event_to_line", "line_to_event",
    "read_event_log", "write_event_log",
]
