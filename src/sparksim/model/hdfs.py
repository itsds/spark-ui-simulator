"""HDFS as Spark sees it: files, blocks and the DataNodes holding each replica.

Block placement follows `BlockPlacementPolicyDefault` with rack awareness:
the first replica on the writer's node (when the writer is a DataNode, as
Spark executors on a YARN cluster are), the second on a node in another
rack, the third on another node of the second replica's rack. Files a
scenario's job writes therefore have a replica on the executor host that wrote
them, and later scans of them get NODE_LOCAL preferences on those hosts.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from .cluster import Cluster

MiB = 1024 * 1024


@dataclass(frozen=True)
class Block:
    offset: int
    length: int
    hosts: tuple[str, ...]


@dataclass(frozen=True)
class RowGroup:
    """A Parquet row group: Spark's reader assigns it to the split that contains its midpoint."""

    offset: int
    length: int
    rows: int

    @property
    def midpoint(self) -> int:
        return self.offset + self.length // 2


@dataclass(frozen=True)
class HdfsFile:
    path: str
    size: int
    rows: int
    blocks: tuple[Block, ...]
    row_groups: tuple[RowGroup, ...] = ()
    # Written by a task that read one hash-partition range of a shuffle on this key: the file holds every row
    # of a `key_fraction` share of the key's values, and no other file does.
    clustered_on: str | None = None
    key_fraction: float = 0.0

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]


@dataclass
class Hdfs:
    cluster: Cluster
    rng: random.Random
    block_size: int = 128 * MiB  # dfs.blocksize
    replication: int = 3  # dfs.replication
    dirs: dict[str, list[HdfsFile]] = field(default_factory=dict)

    def replicas(self, writer: str | None) -> tuple[str, ...]:
        hosts = self.cluster.hosts
        first = writer if writer is not None else self.rng.choice(hosts).name
        rack1 = self.cluster.rack_of(first)
        other_racks = [r for r in self.cluster.racks if r != rack1]
        rack2 = self.rng.choice(other_racks)
        second, third = self.rng.sample(self.cluster.by_rack[rack2], 2)
        return (first, second.name, third.name)[: self.replication]

    def write(self, path: str, size: int, rows: int, writer: str | None, row_group_bytes: int = 0,
              clustered: tuple[str, float] | None = None) -> HdfsFile:
        blocks = []
        off = 0
        while off < size:
            n = min(self.block_size, size - off)
            blocks.append(Block(off, n, self.replicas(writer)))
            off += n
        groups = self.row_groups(size, rows, row_group_bytes) if row_group_bytes else ()
        f = HdfsFile(path, size, rows, tuple(blocks), groups, *(clustered or (None, 0.0)))
        self.dirs.setdefault(path.rsplit("/", 1)[0], []).append(f)
        return f

    def row_groups(self, size: int, rows: int, target: int) -> tuple[RowGroup, ...]:
        """Row groups of a Parquet file: the writer flushes when its buffer reaches parquet.block.size, so
        compressed groups vary around `target`; the footer (~64 KiB) sits after the last one."""
        footer = min(64 * 1024, size // 50)
        data = size - footer
        lengths = []
        left = data
        while left > 0:
            n = int(target * math.exp(self.rng.gauss(0.0, 0.08)))
            if left - n < target * 0.15:  # the last group takes the tail
                n = left
            lengths.append(n)
            left -= n
        out, off, rows_left = [], 0, rows
        for i, n in enumerate(lengths):
            r = rows_left if i == len(lengths) - 1 else round(rows * n / data)
            r = min(r, rows_left)
            out.append(RowGroup(off, n, r))
            off += n
            rows_left -= r
        return tuple(out)

    def listdir(self, directory: str) -> list[HdfsFile]:
        return sorted(self.dirs.get(directory, []), key=lambda f: f.path)

    def create_dataset(self, directory: str, files: int, mean_bytes: float, sigma: float, bytes_per_row: float,
                       name_fn, row_group_bytes: int = 96 * MiB) -> list[HdfsFile]:
        """An existing table written earlier by jobs we don't simulate (writers on random cluster nodes)."""
        mu = math.log(mean_bytes) - sigma * sigma / 2
        out = []
        for i in range(files):
            size = max(1 * MiB, int(math.exp(self.rng.gauss(mu, sigma))))
            rows = max(1, round(size / bytes_per_row))
            out.append(self.write(f"{directory}/{name_fn(i)}", size, rows, writer=None,
                                  row_group_bytes=row_group_bytes))
        return out
