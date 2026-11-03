"""A minimal discrete-event kernel: an event heap, one-shot signals, and generator processes.

Time is integer milliseconds. Events at the same time run in the order they
were scheduled, so the simulation is deterministic.

A process is a generator that yields what it waits for:
`yield 250` (sleep 250 ms), `yield signal`, `yield [sig_a, sig_b]` (all of them).
"""

from __future__ import annotations

import heapq
from collections.abc import Callable, Generator


class Sim:
    def __init__(self, start: int = 0):
        self.now = start
        self._q: list = []
        self._seq = 0

    def at(self, t: int, fn: Callable, *args) -> None:
        t = int(t)
        if t < self.now:
            raise ValueError(f"cannot schedule in the past ({t} < {self.now})")
        heapq.heappush(self._q, (t, self._seq, fn, args))
        self._seq += 1

    def after(self, delay: int, fn: Callable, *args) -> None:
        self.at(self.now + int(delay), fn, *args)

    def run(self, until: int | None = None) -> None:
        while self._q:
            t, _, fn, args = self._q[0]
            if until is not None and t > until:
                break
            heapq.heappop(self._q)
            self.now = t
            fn(*args)

    def signal(self) -> Signal:
        return Signal(self)

    def spawn(self, gen: Generator) -> Signal:
        """Run a generator process; the returned signal fires when it returns."""
        done = Signal(self)

        def step(_=None):
            try:
                what = next(gen)
            except StopIteration:
                done.fire()
                return
            if isinstance(what, int):
                self.after(what, step)
            elif isinstance(what, Signal):
                what.wait(step)
            elif isinstance(what, (list, tuple)):
                all_of(self, list(what)).wait(step)
            else:
                raise TypeError(f"process yielded {what!r}")

        self.at(self.now, step)
        return done


class Signal:
    def __init__(self, sim: Sim):
        self.sim = sim
        self.fired = False
        self.time: int | None = None
        self._waiters: list[Callable] = []

    def fire(self) -> None:
        if self.fired:
            return
        self.fired = True
        self.time = self.sim.now
        for w in self._waiters:
            self.sim.at(self.sim.now, w)
        self._waiters.clear()

    def wait(self, fn: Callable) -> None:
        if self.fired:
            self.sim.at(self.sim.now, fn)
        else:
            self._waiters.append(fn)


def any_of(sim: Sim, signals: list[Signal]) -> Signal:
    out = Signal(sim)
    if not signals or any(s.fired for s in signals):
        sim.at(sim.now, out.fire)
        return out
    for s in signals:
        s.wait(out.fire)
    return out


def all_of(sim: Sim, signals: list[Signal]) -> Signal:
    out = Signal(sim)
    pending = [s for s in signals if not s.fired]
    if not pending:
        sim.at(sim.now, out.fire)
        return out
    left = {"n": len(pending)}

    def one():
        left["n"] -= 1
        if left["n"] == 0:
            out.fire()

    for s in pending:
        s.wait(one)
    return out
