"""The typed event model reproduces every real reference log byte for byte, and its parser is strict."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sparksim.eventlog import (
    EVENT_TYPES,
    DecodeError,
    Event,
    StorageLevel,
    TaskEnd,
    event_to_line,
    line_to_event,
    read_event_log,
    write_event_log,
)
from sparksim.eventlog.jsonfmt import dumps

ROOT = Path(__file__).resolve().parents[1]
LOGS = sorted(p for p in (ROOT / "reference" / "event-logs").iterdir() if p.is_file())


@pytest.mark.parametrize("path", LOGS, ids=lambda p: p.name)
def test_reference_log_roundtrips_byte_for_byte(path: Path):
    lines = path.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines, 1):
        out = event_to_line(line_to_event(line))
        if out != line:
            k = next((j for j, (a, b) in enumerate(zip(out, line, strict=False)) if a != b), min(len(out), len(line)))
            pytest.fail(f"{path.name}:{i} differs at char {k}\n got: …{out[max(0, k - 80):k + 80]}\n"
                        f" ref: …{line[max(0, k - 80):k + 80]}")


def test_every_reference_event_type_is_modeled():
    seen = {json.loads(line)["Event"] for p in LOGS for line in p.open(encoding="utf-8")}
    assert seen <= set(EVENT_TYPES), f"unmodeled: {seen - set(EVENT_TYPES)}"


def test_parser_rejects_unknown_keys_and_events():
    line = next(iter(LOGS[0].open(encoding="utf-8")))
    d = json.loads(line)
    d["Surprise"] = 1
    with pytest.raises(DecodeError, match="unknown keys"):
        Event.decode(d)
    with pytest.raises(DecodeError, match="unsupported event"):
        Event.decode({"Event": "SparkListenerNotAThing"})


def test_parser_rejects_wrong_scalar_types():
    task_end = next(e for e in read_event_log(LOGS[1]) if isinstance(e, TaskEnd)).to_json()
    task_end["Stage ID"] = "0"
    with pytest.raises(DecodeError):
        Event.decode(task_end)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("tab\tnl\ncr\rbs\\q\"", r'"tab\tnl\ncr\rbs\\q\""'),
        ("\x01\x1f\x7f", '"\\u0001\\u001F\x7f"'),  # Jackson: upper-case hex, DEL unescaped
        ("back\\u001f", r'"back\\u001f"'),  # an escaped backslash before 'u' is not an escape
        ("naïve/ü 日本", '"naïve/ü 日本"'),  # raw UTF-8, '/' not escaped
    ],
)
def test_string_escaping_matches_jackson(value, expected):
    assert dumps(value) == expected


def test_float_guard():
    assert dumps({"Amount": 1.0}, check_floats=True) == '{"Amount":1.0}'
    with pytest.raises(ValueError):
        dumps({"x": 1e-5}, check_floats=True)


def test_write_event_log_rejects_dotted_names(tmp_path):
    with pytest.raises(ValueError):
        write_event_log(tmp_path / "app.v1", [])


def test_write_then_read(tmp_path):
    events = list(read_event_log(LOGS[1]))
    out = write_event_log(tmp_path / "copy", events)
    assert out.read_bytes() == LOGS[1].read_bytes()
    assert not list(tmp_path.glob(".*")), "temp file left behind"


def test_storage_level_constants_are_shared_safely():
    with pytest.raises(AttributeError):
        StorageLevel.NONE.use_disk = True  # frozen
