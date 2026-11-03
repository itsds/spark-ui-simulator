"""Discrete-event scheduler: slots, FIFO, delay scheduling, and Spark's locality labels."""

from __future__ import annotations

import random
from collections import defaultdict

from sparksim.cost import calibration as cal
from sparksim.model.cluster import Executor, Host
from sparksim.sched.scheduler import ANY, NODE_LOCAL, PROCESS_LOCAL, RACK_LOCAL, TaskScheduler, TaskSet
from sparksim.sched.sim import Sim

RACKS = {"a1": "/r1", "a2": "/r1", "b1": "/r2", "b2": "/r2", "c1": "/r3"}


def executor(eid: str, host: str, cores: int = 2) -> Executor:
    h = Host(host, "10.0.0.1", RACKS[host], 1.0, 48, 1)
    return Executor(eid, h, int(eid) + 1, f"c{eid}", cores, 1 << 30, 0, 0, 0, 0, 0)


def run_tasksets(executors, tasksets, durations, start=0):
    """tasksets: list of (priority, stage_id, preferred list, submit_time)."""
    sim = Sim(start)
    sched = TaskScheduler(sim, random.Random(1), lambda h: RACKS[h])
    for ex in executors:
        sched.add_executor(ex)
    sched.start()
    out = []
    for prio, sid, prefs, at in tasksets:
        ts = TaskSet(sid, 0, prio, prefs, on_launch=lambda a, d=durations: d(a), on_finish=lambda a: None)
        out.append(ts)
        sim.at(at, sched.submit, ts)
    sim.run()
    return sched, out


def check_slots(launches, executors):
    cores = {e.id: e.cores for e in executors}
    spans = defaultdict(list)
    for a in launches:
        spans[a.executor.id].append((a.launch_time, a.finish_time))
    for eid, iv in spans.items():
        points = sorted([(s, 1) for s, _ in iv] + [(f, -1) for _, f in iv], key=lambda p: (p[0], p[1]))
        running = 0
        for _, d in points:
            running += d
            assert running <= cores[eid], f"executor {eid} oversubscribed"


def test_no_prefs_waves_round_robin():
    exs = [executor("1", "a1"), executor("2", "b1")]
    sched, (ts,) = run_tasksets(exs, [(0, 0, [()] * 10, 0)], lambda a: 1000)
    assert len(ts.launches) == 10 and sorted(a.index for a in ts.launches) == list(range(10))
    assert {a.locality for a in ts.launches} == {PROCESS_LOCAL}  # noPref tasks are reported PROCESS_LOCAL
    first = [a for a in ts.launches if a.launch_time < 100]
    assert len(first) == 4 and {a.executor.id for a in first} == {"1", "2"}
    assert sorted(a.index for a in first) == [0, 1, 2, 3]  # dequeue() takes the lowest index first
    check_slots(ts.launches, exs)


def test_node_local_preferred_then_wait_then_fallback():
    """All tasks prefer host a1 (1 core). The idle executor on b1 is rejected for spark.locality.wait,
    then the task set falls back (no rack-local option for b1 → ANY)."""
    exs = [executor("1", "a1", cores=1), executor("2", "b1", cores=1)]
    sched, (ts,) = run_tasksets(exs, [(0, 0, [("a1",)] * 6, 0)], lambda a: 10_000)
    by_exec = defaultdict(list)
    for a in ts.launches:
        by_exec[a.executor.id].append(a)
    assert by_exec["1"][0].locality == NODE_LOCAL and by_exec["1"][0].launch_time < 100
    remote = by_exec["2"]
    assert remote and all(a.locality == ANY for a in remote)
    assert remote[0].launch_time >= cal.LOCALITY_WAIT_MS  # waited out the node-local level
    assert remote[0].launch_time < 2 * cal.LOCALITY_WAIT_MS + cal.REVIVE_INTERVAL_MS


def test_rack_local_level():
    exs = [executor("1", "a1", cores=1), executor("2", "a2", cores=1)]
    sched, (ts,) = run_tasksets(exs, [(0, 0, [("a1",)] * 4, 0)], lambda a: 10_000)
    locs = {a.executor.id: a.locality for a in ts.launches}
    assert locs["1"] == NODE_LOCAL and locs["2"] == RACK_LOCAL


def test_fifo_between_jobs():
    exs = [executor("1", "a1", cores=2)]
    sched, (job0, job1) = run_tasksets(exs, [(0, 0, [()] * 6, 0), (1, 1, [()] * 2, 0)], lambda a: 1000)
    last0 = max(a.launch_time for a in job0.launches)
    first1 = min(a.launch_time for a in job1.launches)
    assert first1 >= last0, "the later job only gets cores once the earlier one has nothing pending"
    check_slots(job0.launches + job1.launches, exs)


def test_executor_added_later_gets_work():
    sim = Sim(0)
    sched = TaskScheduler(sim, random.Random(2), lambda h: RACKS[h])
    e1, e2 = executor("1", "a1"), executor("2", "b1")
    sched.add_executor(e1)
    sched.start()
    ts = TaskSet(0, 0, 0, [()] * 20, on_launch=lambda a: 500, on_finish=lambda a: None)
    sched.submit(ts)
    sim.at(700, sched.add_executor, e2)
    sim.run()
    assert {a.executor.id for a in ts.launches} == {"1", "2"}
    assert min(a.launch_time for a in ts.launches if a.executor.id == "2") >= 700


def test_deterministic():
    exs = [executor("1", "a1"), executor("2", "b1"), executor("3", "c1")]
    prefs = [(random.Random(i).choice(list(RACKS)),) for i in range(40)]

    def launches():
        _, (ts,) = run_tasksets(exs, [(0, 0, prefs, 0)], lambda a: 300 + 7 * a.index)
        return [(a.index, a.executor.id, a.locality, a.launch_time) for a in ts.launches]

    assert launches() == launches()
