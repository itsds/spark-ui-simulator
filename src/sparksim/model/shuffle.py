"""Shuffle exchanges: map output blocks, where they live, and what each reduce task fetches.

Every map task splits its output rows over the reducers with a multinomial
draw on the partitioner's shares (hash partitioning of the key). The block
matrix `records[map, reducer]` / `bytes[map, reducer]` is kept, so:

* a map task's "Shuffle Bytes/Records Written" is its row of the matrix;
* a reduce task's "Shuffle Read" is a column range of it, split into local
  (map ran on the same host: host-local disk read, `spark.shuffle.readHostLocalDisk`)
  and remote blocks, with empty blocks skipped as Spark does;
* totals are conserved block by block (`tests/test_shuffle.py`).

AQE's `CoalesceShufflePartitions` runs on the real per-partition sizes once
the map stages have finished.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from ..rng import numpy_rng
from .murmur3 import partition_id
from .workload import KeySpace

MiB = 1024 * 1024


def key_shares(key: KeySpace, num_partitions: int, seed: int) -> np.ndarray:
    """Share of the key's distinct values that hash to each partition (multinomial spread)."""
    rng = numpy_rng(seed, f"keys/{key.name}/{num_partitions}")
    rel = math.sqrt(num_partitions / max(1, key.distinct))
    w = _gamma_spread(rng, rel, num_partitions)
    return w / w.sum()


def row_shares(key: KeySpace, num_partitions: int, seed: int) -> np.ndarray:
    """Share of the rows that hash to each partition: key shares widened by uneven rows per key,
    plus the key's heavy hitters (hot keys, Zipf head) in the partitions their values hash to."""
    return row_shares_split(key, num_partitions, seed)[0]


def row_shares_split(key: KeySpace, num_partitions: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """(row shares, the tail's shares alone). The tail (every key but the heavy hitters) is what a
    partition's share of the *distinct* keys follows; files written from it are clustered on that share."""
    n = num_partitions
    base = key_shares(key, n, seed)
    rng = numpy_rng(seed, f"rows/{key.name}/{n}")
    head = np.zeros(n)
    cv = key.freq_cv
    if key.skewed:
        for hk in key.hot:
            head[partition_id(hk.value, key.dtypes, n)] += hk.share
        if key.zipf > 0:
            zh, cv = _zipf_head(key, n, seed)
            head += zh
    rel = math.sqrt(n / max(1, key.distinct)) * cv
    w = base * _gamma_spread(rng, rel, n)
    tail = w / w.sum()
    if not key.skewed:
        return tail, tail
    return tail * (1.0 - head.sum()) + head, tail


# Zipf ranks placed one by one; the rest is summarized by its second moment.
ZIPF_EXPLICIT_RANKS = 4096


def _power_sum(a: int, b: int, t: float) -> float:
    """sum_{k=a}^{b} k^-t: exact up to ZIPF_EXPLICIT_RANKS, then the midpoint integral."""
    if b < a:
        return 0.0
    exact_to = min(b, ZIPF_EXPLICIT_RANKS)
    total = float(np.sum(np.arange(a, exact_to + 1, dtype=float) ** -t)) if exact_to >= a else 0.0
    lo = max(a, exact_to + 1)
    if lo <= b:
        x0, x1 = lo - 0.5, b + 0.5
        total += math.log(x1 / x0) if abs(t - 1.0) < 1e-12 else (x1 ** (1 - t) - x0 ** (1 - t)) / (1 - t)
    return total


def _zipf_head(key: KeySpace, n: int, seed: int) -> tuple[np.ndarray, float]:
    """Rows of the top Zipf ranks per partition, and the rows-per-key CV of the remaining tail.

    p_k = k^-s / H(N, s) of the rows not taken by explicit hot keys. A rank's partition is the pmod of a
    seeded 32-bit hash (its value is unknown), so the same key lands in the same partition for any n.
    """
    s = key.zipf
    keys = max(1, key.distinct - len(key.hot))
    mass = 1.0 - key.hot_share
    h_all = _power_sum(1, keys, s)
    k_top = min(keys, ZIPF_EXPLICIT_RANKS)
    ranks = np.arange(1, k_top + 1, dtype=float)
    p = ranks ** -s / h_all * mass
    hashes = numpy_rng(seed, f"zipf/{key.name}").integers(-(2 ** 31), 2 ** 31, k_top)
    head = np.bincount(np.mod(hashes, n), weights=p, minlength=n).astype(float)
    tail_keys = keys - k_top
    if tail_keys <= 0:
        return head, 0.0
    t_share = _power_sum(k_top + 1, keys, s) / h_all  # tail rows / Zipf rows
    t_sq = _power_sum(k_top + 1, keys, 2 * s) / h_all ** 2
    cv2 = max(0.0, tail_keys * t_sq / t_share ** 2 - 1.0)  # n Σp² / (Σp)² - 1
    return head, math.sqrt(cv2)


def _gamma_spread(rng, rel_std: float, n: int) -> np.ndarray:
    if rel_std <= 1e-9:
        return np.ones(n)
    k = 1.0 / (rel_std * rel_std)
    return rng.gamma(k, 1.0 / k, n)


@dataclass(frozen=True)
class ReadStats:
    """What one reduce task fetches from one exchange."""

    records: int
    local_bytes: int
    remote_bytes: int
    local_blocks: int
    remote_blocks: int
    remote_hosts: int  # distinct remote hosts it fetches from

    @property
    def bytes(self) -> int:
        return self.local_bytes + self.remote_bytes


@dataclass
class ShuffleExchange:
    name: str
    shuffle_id: int
    num_maps: int
    num_partitions: int
    shares: np.ndarray
    # Share of the key's distinct values per partition (files written from a reducer range hold those keys);
    # None: the same as `shares`.
    cluster_shares: np.ndarray | None = field(default=None, repr=False)
    records: np.ndarray = field(init=False, repr=False)
    bytes: np.ndarray = field(init=False, repr=False)
    map_host: list[str | None] = field(init=False)
    map_executor: list[str | None] = field(init=False)
    _cache: dict = field(default_factory=dict, repr=False)
    _hosts: np.ndarray | None = field(default=None, repr=False)
    _written: set[int] = field(default_factory=set, repr=False)

    def __post_init__(self):
        self.records = np.zeros((self.num_maps, self.num_partitions), dtype=np.int64)
        self.bytes = np.zeros((self.num_maps, self.num_partitions), dtype=np.int64)
        self.map_host = [None] * self.num_maps
        self.map_executor = [None] * self.num_maps

    # ------------------------------------------------------------------ map side

    def write(self, map_index: int, rows: int, bytes_per_record: float, rng) -> tuple[int, int]:
        """Split one map task's output rows over the reducers; return (bytes written, records written).

        Another attempt of the same map task (a speculative copy, a re-run after executor loss) hashes the
        same rows into the same partitions: it writes the same blocks, so the first attempt's split stands.
        """
        if map_index in self._written:
            return int(self.bytes[map_index].sum()), int(self.records[map_index].sum())
        self._written.add(map_index)
        rec = rng.multinomial(rows, self.shares) if rows else np.zeros(self.num_partitions, dtype=np.int64)
        byt = np.rint(rec * bytes_per_record).astype(np.int64)
        self.records[map_index] = rec
        self.bytes[map_index] = byt
        return int(byt.sum()), int(rows)

    def commit(self, map_index: int, executor_id: str, host: str) -> None:
        """Register the map output (MapOutputTracker) once the map task succeeded."""
        self.map_host[map_index] = host
        self.map_executor[map_index] = executor_id
        self._cache.clear()
        self._hosts = None

    @property
    def complete(self) -> bool:
        return all(h is not None for h in self.map_host)

    def key_fraction(self, start: int, end: int) -> float:
        """Share of the key's values hashed to reducers [start, end)."""
        sh = self.shares if self.cluster_shares is None else self.cluster_shares
        return float(sh[start:end].sum())

    def bytes_by_partition(self) -> np.ndarray:
        return self.bytes.sum(axis=0)

    def records_by_partition(self) -> np.ndarray:
        return self.records.sum(axis=0)

    # ------------------------------------------------------------------ reduce side

    def read(self, start: int, end: int, host: str, batch: bool) -> ReadStats:
        """Reduce partition = reducers [start, end). `batch`: AQE fetches contiguous blocks of one map as one."""
        hosts, seg_bytes, seg_blocks, seg_rec = self._segment(start, end, batch)
        local = hosts == host
        lb = int(seg_bytes[local].sum())
        tb = int(seg_bytes.sum())
        lbk = int(seg_blocks[local].sum())
        tbk = int(seg_blocks.sum())
        remote_hosts = len(set(hosts[(~local) & (seg_blocks > 0)].tolist()))
        return ReadStats(int(seg_rec.sum()), lb, tb - lb, lbk, tbk - lbk, remote_hosts)

    def _segment(self, start: int, end: int, batch: bool):
        key = (start, end, batch)
        hit = self._cache.get(key)
        if hit is None:
            rec = self.records[:, start:end]
            seg_rec = rec.sum(axis=1)
            seg_bytes = self.bytes[:, start:end].sum(axis=1)
            seg_blocks = (seg_rec > 0).astype(np.int64) if batch else (rec > 0).sum(axis=1)
            if self._hosts is None:
                self._hosts = np.array(self.map_host, dtype=object)
            hit = (self._hosts, seg_bytes, seg_blocks, seg_rec)
            self._cache[key] = hit
        return hit


# --------------------------------------------------------------------------- AQE partition coalescing


def coalesce_partitions(sizes: list[int] | np.ndarray, default_parallelism: int,
                        advisory: int = 64 * MiB, min_partition_size: int = 1 * MiB) -> list[tuple[int, int]]:
    """`CoalesceShufflePartitions` with Spark 3.5 defaults (parallelismFirst = true).

    target = max(min(ceil(total / defaultParallelism), advisory), minPartitionSize). Greedy packing of
    contiguous partitions up to the target (ShufflePartitionsUtil), empty partitions skipped, partitions
    under the minimum size merged into the smaller neighbour. Returns [start, end) reducer ranges, or
    the identity when nothing would be coalesced (AQE then leaves the plan unchanged).
    """
    sizes = [int(s) for s in sizes]
    n = len(sizes)
    total = sum(sizes)
    target = max(min(math.ceil(total / max(1, default_parallelism)), advisory), min_partition_size)
    specs: list[list[int]] = []  # [start, end, size]
    start, acc = 0, 0
    for i, s in enumerate(sizes):
        if i > start and acc + s > target:
            if acc > 0:
                specs.append([start, i, acc])
            start, acc = i, s
        else:
            acc += s
    if acc > 0 or not specs:
        specs.append([start, n, acc])
    # merge partitions below the minimum size into their smaller neighbour
    i = 0
    while len(specs) > 1 and i < len(specs):
        if specs[i][2] < min_partition_size:
            j = i - 1 if i == len(specs) - 1 or (i > 0 and specs[i - 1][2] <= specs[i + 1][2]) else i + 1
            a, b = sorted((i, j))
            specs[a] = [specs[a][0], specs[b][1], specs[a][2] + specs[b][2]]
            del specs[b]
            i = max(0, a - 1)
        else:
            i += 1
    out = [(s[0], s[1]) for s in specs]
    if len(out) >= n:
        return [(i, i + 1) for i in range(n)]
    return out
