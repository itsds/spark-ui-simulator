"""Dataclass <-> JSON mapping for event-log types.

Every event-log type is a dataclass whose fields carry their JSON key
(`jf("Stage ID")`). Field order is JSON key order, so declaring the dataclass in
Spark's `JsonProtocol` order is enough to produce identical bytes.

Encoding rules:
* `omit_none=True`: the key is left out when the value is `None` (Scala
  `Option` fields written with `foreach`). Otherwise `None` becomes `null`.
* Types are taken from the annotations: nested dataclasses, `list[T]`,
  `dict[str, T]`, `T | None` and plain JSON scalars. Unions of dataclasses
  dispatch on a discriminator key (`TaskEndReason` on "Reason", `JobResult` on
  "Result") through a `decode` classmethod on the base class.

Decoding is **strict**: an unknown key or a missing required key raises
`DecodeError`. Combined with the byte-identical round-trip test over every
reference log, this proves that every field Spark wrote is modeled.
"""

from __future__ import annotations

import dataclasses
import functools
import types
import typing
from typing import Any, ClassVar, Union, get_args, get_origin, get_type_hints


class DecodeError(ValueError):
    pass


def jf(json_name: str, *, omit_none: bool = False, default: Any = dataclasses.MISSING,
       default_factory: Any = dataclasses.MISSING):
    """Declare a dataclass field stored under `json_name`."""
    md = {"json": json_name, "omit_none": omit_none}
    if default_factory is not dataclasses.MISSING:
        return dataclasses.field(default_factory=default_factory, metadata=md)
    if default is dataclasses.MISSING and omit_none:
        default = None
    return dataclasses.field(default=default, metadata=md)


class JsonObject:
    """Mixin for dataclasses that map onto one JSON object."""

    # Extra leading keys written before the fields (e.g. {"Event": ...}, {"Reason": ...}).
    def _json_prefix(self) -> dict:
        return {}

    def to_json(self) -> dict:
        out = self._json_prefix()
        for f, name, omit_none in _fields(type(self)):
            v = getattr(self, f)
            if v is None and omit_none:
                continue
            out[name] = encode(v)
        return out

    @classmethod
    def from_json(cls, d: dict, _skip: tuple[str, ...] = ()):
        if not isinstance(d, dict):
            raise DecodeError(f"{cls.__name__}: expected object, got {type(d).__name__}")
        hints = _hints(cls)
        kwargs = {}
        seen = set(_skip)
        for f, name, omit_none in _fields(cls):
            if name in d:
                kwargs[f] = decode(hints[f], d[name], f"{cls.__name__}.{name}")
                seen.add(name)
            elif not omit_none and not _has_default(cls, f):
                raise DecodeError(f"{cls.__name__}: missing key {name!r}")
        extra = [k for k in d if k not in seen]
        if extra:
            raise DecodeError(f"{cls.__name__}: unknown keys {extra}")
        return cls(**kwargs)


@functools.cache
def _fields(cls) -> tuple[tuple[str, str, bool], ...]:
    out = []
    for f in dataclasses.fields(cls):
        name = f.metadata.get("json")
        if name is not None:
            out.append((f.name, name, f.metadata.get("omit_none", False)))
    return tuple(out)


@functools.cache
def _hints(cls) -> dict:
    return get_type_hints(cls)


@functools.cache
def _has_default(cls, fname: str) -> bool:
    f = {x.name: x for x in dataclasses.fields(cls)}[fname]
    return f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING


def encode(v):
    if isinstance(v, JsonObject):
        return v.to_json()
    if isinstance(v, (list, tuple)):
        return [encode(x) for x in v]
    if isinstance(v, dict):
        return {k: encode(x) for k, x in v.items()}
    return v


_SCALARS = {int: (int,), str: (str,), bool: (bool,), float: (float,), type(None): (type(None),)}


def decode(tp, v, where: str = ""):
    origin = get_origin(tp)
    if tp is Any:
        return v
    if origin in (Union, types.UnionType):
        args = get_args(tp)
        if v is None and type(None) in args:
            return None
        non_none = [a for a in args if a is not type(None)]
        # Scalar unions (e.g. int | str for accumulator values): keep the JSON type as is.
        if all(a in _SCALARS for a in non_none):
            if isinstance(v, bool) and bool not in non_none:
                raise DecodeError(f"{where}: unexpected bool")
            if any(isinstance(v, a) and not (a is int and isinstance(v, bool)) for a in non_none):
                return v
            raise DecodeError(f"{where}: {v!r} is not {tp}")
        if len(non_none) == 1:
            return decode(non_none[0], v, where)
        raise DecodeError(f"{where}: unsupported union {tp}")
    if origin is list:
        (item,) = get_args(tp)
        if not isinstance(v, list):
            raise DecodeError(f"{where}: expected list")
        return [decode(item, x, where) for x in v]
    if origin is dict:
        _, val = get_args(tp)
        if not isinstance(v, dict):
            raise DecodeError(f"{where}: expected object")
        return {k: decode(val, x, where) for k, x in v.items()}
    if isinstance(tp, type) and issubclass(tp, JsonObject):
        dec = getattr(tp, "decode", None)
        return dec(v) if dec is not None else tp.from_json(v)
    if tp in _SCALARS:
        if tp is int and (isinstance(v, bool) or not isinstance(v, int)):
            raise DecodeError(f"{where}: {v!r} is not int")
        if tp is float and not isinstance(v, float):
            raise DecodeError(f"{where}: {v!r} is not float")
        if tp is not int and tp is not float and not isinstance(v, tp):
            raise DecodeError(f"{where}: {v!r} is not {tp.__name__}")
        return v
    raise DecodeError(f"{where}: unsupported type {tp}")


__all__ = ["ClassVar", "DecodeError", "JsonObject", "decode", "encode", "jf", "typing"]
