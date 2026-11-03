"""Iteration orders of the JVM collections Spark serializes into the event log.

Spark writes several fields by iterating a hash-based collection, so their
order looks random but is fully determined by the keys' hash codes:

* `JobStart."Stage IDs"` (and `"Stage Infos"`): `DAGScheduler.jobIdToStageIds`
  is a Scala 2.12 `mutable.HashSet[Int]` (open addressing, `FlatHashTable`).
* `JobStart/StageSubmitted."Properties"`: a `java.util.Properties`. On JDK 9+
  its entries live in a `ConcurrentHashMap`, iterated bin by bin.
* `EnvironmentUpdate` maps: `Seq[(String, String)].toMap`, a Scala 2.12
  immutable `HashMap` (a hash trie, 5 bits per level) once it has more than 4
  entries, insertion order up to 4.

`tests/test_ordering.py` checks all three rules against every reference log
(34 multi-stage jobs, 178 property sets, 36 environment maps). Within a
ConcurrentHashMap bin the order is insertion order, which we take from the
caller.
"""

from __future__ import annotations

from collections.abc import Iterable

_M32 = 0xFFFFFFFF


def _s32(x: int) -> int:
    x &= _M32
    return x - (1 << 32) if x >> 31 else x


def java_string_hash(s: str) -> int:
    """`String.hashCode` (UTF-16 code units), as an unsigned 32-bit int."""
    h = 0
    data = s.encode("utf-16-be")
    for i in range(0, len(data), 2):
        h = (31 * h + (data[i] << 8 | data[i + 1])) & _M32
    return h


# --------------------------------------------------------------------------- Scala mutable.HashSet[Int]


def _byteswap32(v: int) -> int:
    hc = (v * 0x9E3775CD) & _M32
    hc = int.from_bytes(hc.to_bytes(4, "big"), "little")
    return (hc * 0x9E3775CD) & _M32


def _flat_index(elem: int, size: int) -> int:
    ones = size - 1
    bits = ones.bit_count()
    rot = bits % 32  # seedvalue = tableSizeSeed = bitCount(table.length - 1)
    imp = _byteswap32(elem & _M32)
    imp = ((imp >> rot) | (imp << (32 - rot))) & _M32 if rot else imp
    return (imp >> (32 - bits)) & ones


def scala_int_hashset_order(insertion_order: Iterable[int]) -> list[int]:
    """Iteration order of a Scala 2.12 `mutable.HashSet[Int]` filled in `insertion_order`."""
    size, count = 32, 0
    table: list[int | None] = [None] * size

    def add(t: list[int | None], x: int) -> bool:
        h = _flat_index(x, len(t))
        while t[h] is not None:
            if t[h] == x:
                return False
            h = (h + 1) % len(t)
        t[h] = x
        return True

    for x in insertion_order:
        if add(table, x):
            count += 1
            if count >= size * 450 // 1000:  # FlatHashTable.growTable at load factor 0.45
                old, size = table, size * 2
                table = [None] * size
                for y in old:
                    if y is not None:
                        add(table, y)
    return [x for x in table if x is not None]


# --------------------------------------------------------------------------- java.util.Properties (JDK 17)


def java_properties_order(keys: Iterable[str]) -> list[str]:
    """Iteration order of a JDK 9+ `Properties` (ConcurrentHashMap) holding `keys` (given in insertion order).

    Bins are visited in index order; the table starts at 16 and doubles once the
    size reaches 0.75 of it (matches every reference log).
    """
    keys = list(keys)
    n = 16
    while len(keys) >= n * 3 // 4:
        n *= 2

    def bin_of(k: str) -> int:
        h = java_string_hash(k)
        return ((h ^ (h >> 16)) & 0x7FFFFFFF) & (n - 1)

    return sorted(keys, key=bin_of)  # stable: insertion order within a bin


def java_properties(d: dict[str, str]) -> dict[str, str]:
    return {k: d[k] for k in java_properties_order(d)}


# --------------------------------------------------------------------------- Scala immutable.HashMap


def _scala_improve(hcode: int) -> int:
    h = _s32(hcode)
    h = _s32(h + ~_s32(h << 9))
    h = _s32(h ^ ((h & _M32) >> 14))
    h = _s32(h + _s32(h << 4))
    h = _s32(h ^ ((h & _M32) >> 10))
    return h & _M32


def scala_map_order(keys: Iterable[str]) -> list[str]:
    """Iteration order of `Seq(...).toMap` with String keys: Map1..Map4 keep insertion order, then a hash trie."""
    keys = list(dict.fromkeys(keys))
    if len(keys) <= 4:
        return keys

    def trie_path(k: str) -> tuple[int, ...]:
        h = _scala_improve(java_string_hash(k))
        return tuple((h >> (5 * level)) & 31 for level in range(7))

    return sorted(keys, key=trie_path)


def scala_map(d: dict[str, str]) -> dict[str, str]:
    return {k: d[k] for k in scala_map_order(d)}
