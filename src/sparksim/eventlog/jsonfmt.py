"""Compact JSON exactly as Spark 3.5 writes event-log lines (Jackson defaults).

Spark's `JsonProtocol` streams events through a Jackson `JsonGenerator`, and the
SQL events go through a Jackson `ObjectMapper`. The bytes we need to match:

* no whitespace: `{"a":1,"b":[1,2]}`;
* non-ASCII written as raw UTF-8 (not `\\uXXXX`);
* `"` and `\\` escaped; `\\b \\t \\n \\f \\r` use the short forms; every other
  control character below 0x20 is `\\u00XX` with **upper-case** hex (Python's
  `json` writes lower-case, so we patch that);
* `/` is not escaped;
* doubles in Java's `Double.toString` form. Python's `repr` matches it inside
  [1e-3, 1e7), the only range Spark's event logs use (resource amounts such as
  `1.0`). Anything outside that range is rejected rather than written wrongly.
"""

from __future__ import annotations

import json
import math
import re

_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False, check_circular=False)

# A \u escape preceded by an even number of backslashes is a real escape
# (an odd number means the backslash itself was escaped).
_LOWER_HEX_ESCAPE = re.compile(r"(?<!\\)((?:\\\\)*)\\u([0-9a-f]{4})")
_LOWER_HEX_CANDIDATE = re.compile(r"\\u[0-9A-Fa-f]{0,3}[a-f]")


def _upper(m: re.Match[str]) -> str:
    return f"{m.group(1)}\\u{m.group(2).upper()}"


def _check_floats(obj) -> None:
    if isinstance(obj, float):
        if not (obj == 0.0 or 1e-3 <= abs(obj) < 1e7) or math.isnan(obj):
            raise ValueError(f"float {obj!r} would not be written like Java's Double.toString")
    elif isinstance(obj, dict):
        for v in obj.values():
            _check_floats(v)
    elif isinstance(obj, list):
        for v in obj:
            _check_floats(v)


def dumps(obj, *, check_floats: bool = False) -> str:
    """Serialize a JSON-ready object (dicts keep insertion order) to one event-log line, without the newline."""
    if check_floats:
        _check_floats(obj)
    s = _ENCODER.encode(obj)
    if "\\u" in s and _LOWER_HEX_CANDIDATE.search(s):
        s = _LOWER_HEX_ESCAPE.sub(_upper, s)
    return s
