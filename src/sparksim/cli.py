"""`sparksim` command line.

  sparksim generate tiny [--out event-logs] [--seed 7]   write the Phase 2 smoke app
  sparksim roundtrip <event-log>...                        parse + re-serialize, report any byte difference
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .eventlog import event_to_line, line_to_event


def _generate(a) -> int:
    from .apps import tiny

    path = tiny.generate(a.out, tiny.TinySpec(seed=a.seed))
    print(f"wrote {path}")
    return 0


def _roundtrip(a) -> int:
    bad = 0
    for p in a.logs:
        lines = Path(p).read_text(encoding="utf-8").splitlines()
        diffs = [i for i, ln in enumerate(lines, 1) if event_to_line(line_to_event(ln)) != ln]
        bad += bool(diffs)
        where = f", first diff at line {diffs[0]}" if diffs else ""
        print(f"{'OK  ' if not diffs else 'DIFF'} {p}: {len(lines)} lines{where}")
    return 1 if bad else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sparksim", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write a synthetic application's event log")
    g.add_argument("app", choices=["tiny"])
    g.add_argument("--out", default="event-logs", type=Path)
    g.add_argument("--seed", default=7, type=int)
    g.set_defaults(fn=_generate)
    r = sub.add_parser("roundtrip", help="check that the event model reproduces a log byte for byte")
    r.add_argument("logs", nargs="+", type=Path)
    r.set_defaults(fn=_roundtrip)
    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
