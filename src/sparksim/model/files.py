"""How Spark 3.5 turns a table's files into scan tasks (`FileSourceScanExec.createReadRDD`).

Ported from `FilePartition.maxSplitBytes`, `PartitionedFileUtil.splitFiles`,
`getBlockHosts`, `FilePartition.getFilePartitions` (Next Fit Decreasing) and
`FilePartition.preferredLocations`. The number of scan tasks, their input
sizes and their preferred hosts all come out of this, from the file sizes and
the HDFS block placement.
"""

from __future__ import annotations

from dataclasses import dataclass

from .hdfs import HdfsFile

MiB = 1024 * 1024


@dataclass(frozen=True)
class PartitionedFile:
    file: HdfsFile
    start: int
    length: int
    locations: tuple[str, ...]

    def _groups(self):
        end = self.start + self.length
        return [g for g in self.file.row_groups if self.start <= g.midpoint < end]

    @property
    def rows(self) -> int:
        """Rows this split reads. Parquet: the row groups whose midpoint falls in the byte range
        (ParquetFileFormat / ParquetInputSplit), so a file's splits sum to its rows and a split can be empty.
        Files without row-group layout: a proportional share of the bytes."""
        f = self.file
        if f.row_groups:
            return sum(g.rows for g in self._groups())
        if f.size == 0:
            return 0
        return (f.rows * (self.start + self.length)) // f.size - (f.rows * self.start) // f.size

    @property
    def data_bytes(self) -> int:
        """Bytes of row-group data this split actually reads (before column pruning)."""
        if self.file.row_groups:
            return sum(g.length for g in self._groups())
        return self.length


@dataclass(frozen=True)
class FilePartition:
    index: int
    files: tuple[PartitionedFile, ...]

    @property
    def length(self) -> int:
        return sum(f.length for f in self.files)

    @property
    def rows(self) -> int:
        return sum(f.rows for f in self.files)

    @property
    def data_bytes(self) -> int:
        return sum(f.data_bytes for f in self.files)

    def preferred_locations(self) -> list[str]:
        """Top 3 hosts by bytes of this partition they hold ("localhost" is ignored)."""
        by_host: dict[str, int] = {}
        for f in self.files:
            for h in f.locations:
                if h != "localhost":
                    by_host[h] = by_host.get(h, 0) + f.length
        # Scala: hostToNumBytes.toSeq.sortBy(_._2).reverse.take(3). Ties keep a fixed order here.
        ranked = sorted(by_host.items(), key=lambda kv: kv[1], reverse=True)
        return [h for h, _ in ranked[:3]]


def max_split_bytes(files: list[HdfsFile], max_partition_bytes: int = 128 * MiB, open_cost: int = 4 * MiB,
                    min_partition_num: int = 1) -> int:
    """`FilePartition.maxSplitBytes`: min(maxPartitionBytes, max(openCost, totalBytes / parallelism))."""
    total = sum(f.size + open_cost for f in files)
    bytes_per_core = total // max(1, min_partition_num)
    return min(max_partition_bytes, max(open_cost, bytes_per_core))


def block_hosts(f: HdfsFile, offset: int, length: int) -> tuple[str, ...]:
    """`PartitionedFileUtil.getBlockHosts`: hosts of the block overlapping this range the most."""
    best, best_size = (), 0
    for b in f.blocks:
        if b.offset <= offset < b.offset + b.length:
            size = min(b.offset + b.length - offset, length)
        elif b.offset < offset + length < b.offset + b.length:
            size = offset + length - b.offset
        elif offset <= b.offset and b.offset + b.length <= offset + length:
            size = b.length
        else:
            size = 0
        if size > best_size:  # maxBy keeps the first maximum
            best, best_size = b.hosts, size
    return best


def split_files(files: list[HdfsFile], max_split: int, splitable: bool = True) -> list[PartitionedFile]:
    """`PartitionedFileUtil.splitFiles` for every file, then sorted by length, largest first."""
    out = []
    for f in files:
        if splitable:
            for off in range(0, f.size, max_split):
                n = min(max_split, f.size - off)
                out.append(PartitionedFile(f, off, n, block_hosts(f, off, n)))
        else:
            out.append(PartitionedFile(f, 0, f.size, block_hosts(f, 0, f.size)))
    out.sort(key=lambda p: p.length, reverse=True)  # stable, like Scala's sortBy
    return out


def file_partitions(splits: list[PartitionedFile], max_split: int, open_cost: int = 4 * MiB) -> list[FilePartition]:
    """`FilePartition.getFilePartitions`: Next Fit Decreasing packing."""
    parts: list[FilePartition] = []
    current: list[PartitionedFile] = []
    size = 0
    for s in splits:
        if size + s.length > max_split and current:
            parts.append(FilePartition(len(parts), tuple(current)))
            current, size = [], 0
        size += s.length + open_cost
        current.append(s)
    if current:
        parts.append(FilePartition(len(parts), tuple(current)))
    return parts


def plan_scan(files: list[HdfsFile], default_parallelism: int, max_partition_bytes: int = 128 * MiB,
              open_cost: int = 4 * MiB) -> list[FilePartition]:
    ms = max_split_bytes(files, max_partition_bytes, open_cost, default_parallelism)
    return file_partitions(split_files(files, ms), ms, open_cost)
