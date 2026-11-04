"""Spark's hash partitioning: `HashPartitioning(keys, n).partitionIdExpression = pmod(Murmur3Hash(keys, 42), n)`.

A port of `org.apache.spark.unsafe.hash.Murmur3_x86_32` and of how `Murmur3Hash` folds several key
columns (the hash of one column is the seed of the next). It decides which shuffle partition a given key
value lands in, so a hot key's partition in the simulation is the one Spark would pick
(`tests/test_keys.py` checks it against PySpark's `F.hash`).
"""

from __future__ import annotations

import datetime as _dt

SEED = 42
_C1 = 0xCC9E2D51
_C2 = 0x1B873593
_M32 = 0xFFFFFFFF


def _i32(x: int) -> int:
    x &= _M32
    return x - (1 << 32) if x & 0x80000000 else x


def _rotl(x: int, r: int) -> int:
    x &= _M32
    return ((x << r) | (x >> (32 - r))) & _M32


def _mix_k1(k1: int) -> int:
    k1 = (k1 * _C1) & _M32
    k1 = _rotl(k1, 15)
    return (k1 * _C2) & _M32


def _mix_h1(h1: int, k1: int) -> int:
    h1 = (h1 ^ k1) & _M32
    h1 = _rotl(h1, 13)
    return (h1 * 5 + 0xE6546B64) & _M32


def _fmix(h1: int, length: int) -> int:
    h1 = (h1 ^ length) & _M32
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & _M32
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & _M32
    h1 ^= h1 >> 16
    return _i32(h1)


def hash_int(value: int, seed: int) -> int:
    """`Murmur3_x86_32.hashInt` (int, date as days since the epoch, boolean as 0/1, …)."""
    return _fmix(_mix_h1(seed & _M32, _mix_k1(value & _M32)), 4)


def hash_long(value: int, seed: int) -> int:
    """`Murmur3_x86_32.hashLong` (bigint, timestamp in µs)."""
    v = value & 0xFFFFFFFFFFFFFFFF
    h1 = _mix_h1(seed & _M32, _mix_k1(v & _M32))
    h1 = _mix_h1(h1, _mix_k1(v >> 32))
    return _fmix(h1, 8)


def hash_bytes(data: bytes, seed: int) -> int:
    """`Murmur3_x86_32.hashUnsafeBytes` (strings as UTF-8): 4-byte words, then each tail byte as a word."""
    h1 = seed & _M32
    n = len(data) - len(data) % 4
    for i in range(0, n, 4):
        h1 = _mix_h1(h1, _mix_k1(int.from_bytes(data[i:i + 4], "little")))
    for b in data[n:]:
        h1 = _mix_h1(h1, _mix_k1(b - 256 if b > 127 else b))  # Platform.getByte: signed byte
    return _fmix(h1, len(data))


def hash_value(value, dtype: str, seed: int) -> int:
    """`Murmur3Hash` of one non-null value of a Spark SQL type (a null leaves the seed unchanged)."""
    if value is None:
        return seed
    t = dtype.lower()
    if t in ("bigint", "long"):
        return hash_long(int(value), seed)
    if t in ("int", "integer", "smallint", "tinyint"):
        return hash_int(int(value), seed)
    if t == "boolean":
        return hash_int(1 if value else 0, seed)
    if t == "date":
        d = value if isinstance(value, _dt.date) else _dt.date.fromisoformat(str(value))
        return hash_int((d - _dt.date(1970, 1, 1)).days, seed)
    if t == "string":
        return hash_bytes(str(value).encode("utf-8"), seed)
    raise ValueError(f"no Murmur3 hash for type {dtype!r}")


def murmur3_hash(values, dtypes, seed: int = SEED) -> int:
    """`Murmur3Hash(children, 42)`: each column's hash seeds the next one."""
    h = seed
    for v, t in zip(values, dtypes, strict=True):
        h = hash_value(v, t, h)
    return h


def partition_id(values, dtypes, num_partitions: int) -> int:
    """`pmod(Murmur3Hash(keys, 42), numPartitions)`: the shuffle partition of a key."""
    return murmur3_hash(values, dtypes) % num_partitions
