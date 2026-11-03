"""Cluster model, HDFS placement, Spark's file-split algorithm, and shuffle exchanges."""

from __future__ import annotations

import random

import numpy as np
import pytest

from sparksim.model.cluster import ClusterSpec, ExecutorSizing, build_cluster, place_application, scheduling_start
from sparksim.model.files import (
    FilePartition,
    PartitionedFile,
    file_partitions,
    max_split_bytes,
    plan_scan,
    split_files,
)
from sparksim.model.hdfs import Block, Hdfs, HdfsFile
from sparksim.model.shuffle import ShuffleExchange, coalesce_partitions, key_shares, row_shares
from sparksim.model.workload import KeySpace, expected_distinct
from sparksim.yarn import YarnApp

MiB = 1024 * 1024
SPEC = ClusterSpec(cluster_timestamp=1791902144377)


@pytest.fixture(scope="module")
def cluster():
    return build_cluster(SPEC, seed=1)


# --------------------------------------------------------------------------- cluster / YARN


def test_container_sizing():
    s = ExecutorSizing(memory_mb=16 * 1024)
    assert s.executor_overhead_mb == 1638  # max(384, 10 %)
    assert s.container_mb(1024) == 18432  # 16384 + 1638 rounded up to the 1 GiB allocation step
    assert ExecutorSizing(memory_mb=2048).executor_overhead_mb == 384
    assert ExecutorSizing(memory_mb=4096, overhead_mb=2048).container_mb(1024) == 6144


def test_cluster_layout(cluster):
    assert len(cluster.hosts) == SPEC.racks * SPEC.hosts_per_rack
    assert len(cluster.racks) == SPEC.racks
    assert len({h.ip for h in cluster.hosts}) == len(cluster.hosts)
    assert all(0.85 < h.speed < 1.15 for h in cluster.hosts)


def test_placement_respects_capacity(cluster):
    sizing = ExecutorSizing(instances=60, cores=4, memory_mb=16 * 1024)
    p = place_application(cluster, sizing, YarnApp(SPEC.cluster_timestamp, 7), driver_start=1_000_000, seed=3)
    assert len(p.executors) == 60
    used: dict[str, int] = {}
    for e in p.executors:
        used[e.host.name] = used.get(e.host.name, 0) + sizing.container_mb(SPEC.min_allocation_mb)
    used[p.am_host.name] = used.get(p.am_host.name, 0) + sizing.am_container_mb(SPEC.min_allocation_mb)
    for h, mb in used.items():
        assert mb <= cluster.free_mb[h], h
    # executor ids follow container order; registration happens after allocation
    by_id = sorted(p.executors, key=lambda e: int(e.id))
    assert [e.container_n for e in by_id] == list(range(2, 62))
    assert all(e.registered_at > e.allocated_at >= p.request_time for e in p.executors)
    assert [e.registered_at for e in p.executors] == sorted(e.registered_at for e in p.executors)
    start = scheduling_start(p, 60, driver_ready=1_001_500)
    assert sum(e.registered_at <= start for e in p.executors) >= 48  # minRegisteredResourcesRatio 0.8


def test_busy_cluster_packs_executors(cluster):
    """When few nodes have room, YARN puts several containers on the same node (one per heartbeat)."""
    tight = build_cluster(ClusterSpec(cluster_timestamp=1, racks=2, hosts_per_rack=6, occupancy=(0.6, 0.8)), seed=2)
    p = place_application(tight, ExecutorSizing(instances=30, memory_mb=8 * 1024), YarnApp(1, 1), 0, seed=2)
    hosts = {e.host.name for e in p.executors}
    assert len(hosts) < 30


# --------------------------------------------------------------------------- HDFS


def test_replica_placement(cluster):
    hdfs = Hdfs(cluster, random.Random(5))
    writer = cluster.hosts[7].name
    for _ in range(200):
        a, b, c = hdfs.replicas(writer)
        assert a == writer and len({a, b, c}) == 3
        assert cluster.rack_of(b) == cluster.rack_of(c) != cluster.rack_of(a)
    f = hdfs.write("/t/part-0", 300 * MiB, 1000, writer)
    assert [b.length for b in f.blocks] == [128 * MiB, 128 * MiB, 44 * MiB]


# --------------------------------------------------------------------------- Spark file splits


def _file(size_mb: float, hosts_per_block=(("h1", "h2", "h3"),), rows=1_000_000, path="/t/f") -> HdfsFile:
    size = int(size_mb * MiB)
    blocks, off, i = [], 0, 0
    while off < size:
        n = min(128 * MiB, size - off)
        blocks.append(Block(off, n, hosts_per_block[i % len(hosts_per_block)]))
        off += n
        i += 1
    return HdfsFile(path, size, rows, tuple(blocks))


def test_max_split_bytes():
    small = [_file(10) for _ in range(10)]
    # bytesPerCore = (10 x 14 MiB) / 160 cores < openCost → openCost
    assert max_split_bytes(small, 128 * MiB, 4 * MiB, 160) == 4 * MiB
    big = [_file(1000) for _ in range(10)]
    assert max_split_bytes(big, 128 * MiB, 4 * MiB, 16) == 128 * MiB
    assert max_split_bytes(big, 128 * MiB, 4 * MiB, 1000) == (10 * 1004 * MiB) // 1000


def test_split_and_block_hosts():
    f = _file(300, hosts_per_block=(("a1", "a2", "a3"), ("b1", "b2", "b3"), ("c1", "c2", "c3")))
    splits = split_files([f], 128 * MiB)
    assert [s.length for s in splits] == [128 * MiB, 128 * MiB, 44 * MiB]
    assert [s.locations for s in splits] == [("a1", "a2", "a3"), ("b1", "b2", "b3"), ("c1", "c2", "c3")]
    assert sum(s.rows for s in splits) == f.rows
    # a split straddling two blocks takes the hosts of the block it overlaps most
    s = split_files([f], 200 * MiB)
    assert s[0].length == 200 * MiB and s[0].locations == ("a1", "a2", "a3")
    assert s[1].locations == ("b1", "b2", "b3")  # 200..300 MiB: 56 MiB in block 1, 44 MiB in block 2


def test_next_fit_decreasing_packing():
    files = [_file(mb, path=f"/t/{mb}") for mb in (100, 60, 50, 30, 10)]
    parts = file_partitions(split_files(files, 128 * MiB), 128 * MiB, 4 * MiB)
    assert [[round(p.length / MiB) for p in part.files] for part in parts] == [[100], [60, 50], [30, 10]]


def test_preferred_locations():
    a = PartitionedFile(_file(10), 0, 60, ("h1", "h2", "localhost"))
    b = PartitionedFile(_file(10), 0, 50, ("h2", "h3", "h4"))
    part = FilePartition(0, (a, b))
    assert part.preferred_locations() == ["h2", "h1", "h3"]  # by bytes; "localhost" ignored


def test_plan_scan_from_hdfs(cluster):
    hdfs = Hdfs(cluster, random.Random(9))
    files = hdfs.create_dataset("/w/t", 50, 200 * MiB, 0.2, 290, lambda i: f"part-{i:05d}")
    parts = plan_scan(files, 160, 256 * MiB)
    assert sum(p.length for p in parts) == sum(f.size for f in files)
    assert sum(p.rows for p in parts) == sum(f.rows for f in files)
    assert all(1 <= len(p.preferred_locations()) <= 3 for p in parts)


# --------------------------------------------------------------------------- keys and shuffle


def test_expected_distinct():
    assert expected_distinct(1000, 0) == 0
    assert expected_distinct(1000, 10) == 10
    assert expected_distinct(57_600, 1_700_000) == 57_600
    assert 1_690_000 < expected_distinct(61_000_000, 1_720_000) < 1_700_000


def test_shares_are_distributions():
    k = KeySpace("customer_id", 208_000_000, freq_cv=2.5)
    for shares in (key_shares(k, 2400, 1), row_shares(k, 2400, 1)):
        assert shares.shape == (2400,) and abs(shares.sum() - 1) < 1e-9
        assert shares.max() / np.median(shares) < 1.2  # high-cardinality key: even partitions
    assert np.array_equal(key_shares(k, 2400, 1), key_shares(k, 2400, 1))  # same partitioner both sides
    few = KeySpace("country", 60)
    assert key_shares(few, 200, 1).max() / np.median(key_shares(few, 200, 1)) > 2  # few keys: uneven


def test_exchange_conservation_and_locality():
    rng = np.random.Generator(np.random.PCG64(4))
    ex = ShuffleExchange("x", 0, num_maps=30, num_partitions=50, shares=key_shares(KeySpace("k", 10_000), 50, 1))
    hosts = ["h1", "h2", "h3"]
    written = []
    for m in range(30):
        b, r = ex.write(m, rows=2_000 + 10 * m, bytes_per_record=40.5, rng=rng)
        written.append((b, r))
        ex.commit(m, str(m % 3), hosts[m % 3])
    assert ex.complete
    assert int(ex.bytes.sum()) == sum(b for b, _ in written)
    assert int(ex.records.sum()) == sum(r for _, r in written)
    total_b = total_r = 0
    for r in range(50):
        st = ex.read(r, r + 1, "h1", batch=False)
        total_b += st.bytes
        total_r += st.records
        assert st.local_blocks == 10 and st.remote_blocks == 20  # one non-empty block per map task
        assert st.local_bytes == int(ex.bytes[0::3, r].sum())
    assert total_b == int(ex.bytes.sum()) and total_r == int(ex.records.sum())
    batched = ex.read(0, 10, "h2", batch=True)
    assert batched.local_blocks + batched.remote_blocks == 30  # contiguous blocks of one map fetched as one


def test_empty_blocks_are_not_fetched():
    rng = np.random.Generator(np.random.PCG64(1))
    ex = ShuffleExchange("y", 1, num_maps=20, num_partitions=400, shares=np.full(400, 1 / 400))
    for m in range(20):
        ex.write(m, rows=100, bytes_per_record=30, rng=rng)
        ex.commit(m, "1", "h1")
    st = ex.read(0, 1, "h9", batch=False)
    assert st.remote_blocks == int((ex.records[:, 0] > 0).sum()) < 20


def test_coalesce_partitions():
    # identity when every partition already exceeds half the target
    assert coalesce_partitions([60 * MiB] * 10, 4) == [(i, i + 1) for i in range(10)]
    # small partitions packed up to the target (total / parallelism here)
    sizes = [1 * MiB] * 100
    out = coalesce_partitions(sizes, default_parallelism=10)
    assert len(out) == 10 and out[0] == (0, 10) and out[-1][1] == 100
    # contiguous, ordered, and covering every non-empty partition
    sizes = [0, 0, 5 * MiB, 0, 70 * MiB, 3 * MiB, 3 * MiB, 0, 90 * MiB, 2 * MiB]
    out = coalesce_partitions(sizes, default_parallelism=1)
    covered = [i for a, b in out for i in range(a, b)]
    assert covered == sorted(covered) and len(set(covered)) == len(covered)
    assert {i for i, s in enumerate(sizes) if s} <= set(covered)
