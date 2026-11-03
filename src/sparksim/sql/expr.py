"""Attributes and expressions, printed exactly as Catalyst's `toString` prints them in plans.

Only what the physical plans of our apps need: column references with expression
ids (`customer_id#1L`), literals, null checks, comparisons, boolean and
arithmetic operators, casts, function calls, aliases and aggregate functions.
Each class's `__str__` follows the Catalyst class of the same name:

    AttributeReference   name#id + "L" for bigint             customer_id#1L
    BinaryOperator       (left op right)                      (dt#7 >= 2026-10-03)
    Not                  NOT child                            NOT (status#5 = RETURNED)
    IsNotNull            isnotnull(child)
    Cast                 cast(child as type)                  cast(quantity#3 as double)
    Alias                child AS name#id
    AggregateExpression  partial_ / merge_ / "" + func        partial_sum(amount#4)

Literals print raw (no quotes): Literal.toString of a UTF8String or a date is
the value itself.

UnsafeRow sizes (for shuffle `data size` and memory) follow
`UnsafeRow.calculateBitSetWidthInBytes` + 8 bytes per field + variable-length
data rounded up to 8 bytes.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field

FIXED_TYPES = {"bigint", "int", "double", "float", "smallint", "tinyint", "boolean", "date", "timestamp"}


class Expr:
    dtype: str = "string"
    nullable: bool = True

    def children(self) -> tuple[Expr, ...]:
        return ()

    def refs(self) -> list[Attr]:
        out: list[Attr] = []
        for c in self.children():
            for a in c.refs():
                if a not in out:
                    out.append(a)
        return out

    @property
    def foldable(self) -> bool:
        return bool(self.children()) and all(c.foldable for c in self.children())

    # PySpark-like builders (methods, not ==/!= overloads, so lists and sets of expressions stay sane)
    def eq(self, other) -> BinaryOp:
        return EqualTo(self, lit(other))

    def ne(self, other) -> Not:
        return Not(EqualTo(self, lit(other)))

    def ge(self, other) -> BinaryOp:
        return BinaryOp(self, ">=", lit(other), "boolean", "GreaterThanOrEqual")

    def gt(self, other) -> BinaryOp:
        return BinaryOp(self, ">", lit(other), "boolean", "GreaterThan")

    def le(self, other) -> BinaryOp:
        return BinaryOp(self, "<=", lit(other), "boolean", "LessThanOrEqual")

    def lt(self, other) -> BinaryOp:
        return BinaryOp(self, "<", lit(other), "boolean", "LessThan")

    def __and__(self, other):
        return And(self, other)

    def __mul__(self, other):
        o = lit(other)
        return BinaryOp(self, "*", o, _numeric(self.dtype, o.dtype), "")

    def __add__(self, other):
        o = lit(other)
        return BinaryOp(self, "+", o, _numeric(self.dtype, o.dtype), "")

    def cast(self, dtype: str) -> Cast:
        return Cast(self, dtype)

    def alias(self, name: str) -> Alias:
        return Alias(self, name)


def _numeric(a: str, b: str) -> str:
    order = ["tinyint", "smallint", "int", "bigint", "float", "double"]
    return max(a, b, key=lambda t: order.index(t) if t in order else len(order))


@dataclass(eq=False)
class Attr(Expr):
    """`AttributeReference`. Equality is by expression id, as in Catalyst."""

    name: str
    dtype: str
    expr_id: int = -1
    nullable: bool = True
    avg_bytes: float = 0.0  # strings / binary: average UTF-8 length of a value

    def __str__(self) -> str:
        return f"{self.name}#{self.expr_id}{'L' if self.dtype == 'bigint' else ''}"

    def refs(self) -> list[Attr]:
        return [self]

    def __eq__(self, other) -> bool:
        return isinstance(other, Attr) and other.expr_id == self.expr_id

    def __hash__(self):
        return hash(("attr", self.expr_id))

    def with_nullable(self, nullable: bool) -> Attr:
        return Attr(self.name, self.dtype, self.expr_id, nullable, self.avg_bytes)

    @property
    def foldable(self) -> bool:
        return False


@dataclass(eq=False)
class Lit(Expr):
    value: object
    dtype: str = "string"
    nullable: bool = False

    def __str__(self) -> str:
        if self.value is None:
            return "null"
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        return str(self.value)

    @property
    def foldable(self) -> bool:
        return True


def lit(v) -> Expr:
    if isinstance(v, Expr):
        return v
    if isinstance(v, bool):
        return Lit(v, "boolean")
    if isinstance(v, int):
        return Lit(v, "int")
    if isinstance(v, float):
        return Lit(v, "double")
    return Lit(v, "string")


@dataclass(eq=False)
class IsNotNull(Expr):
    child: Expr
    dtype: str = "boolean"
    nullable: bool = False

    def children(self):
        return (self.child,)

    def __str__(self) -> str:
        return f"isnotnull({self.child})"


@dataclass(eq=False)
class Not(Expr):
    child: Expr
    dtype: str = "boolean"

    def children(self):
        return (self.child,)

    def __str__(self) -> str:
        return f"NOT {self.child}"


@dataclass(eq=False)
class BinaryOp(Expr):
    left: Expr
    op: str
    right: Expr
    dtype: str = "boolean"
    source_filter: str = ""  # data source Filter class name when the predicate can be pushed down

    def children(self):
        return (self.left, self.right)

    def __str__(self) -> str:
        return f"({self.left} {self.op} {self.right})"


def EqualTo(a: Expr, b: Expr) -> BinaryOp:  # noqa: N802
    return BinaryOp(a, "=", b, "boolean", "EqualTo")


def And(a: Expr, b: Expr) -> BinaryOp:  # noqa: N802
    return BinaryOp(a, "AND", b, "boolean")


@dataclass(eq=False)
class Cast(Expr):
    child: Expr
    dtype: str = "double"

    def children(self):
        return (self.child,)

    def __str__(self) -> str:
        return f"cast({self.child} as {self.dtype})"


@dataclass(eq=False)
class Func(Expr):
    """A scalar function call printed as `name(arg, arg, ...)` (Expression.toString with flatArguments)."""

    name: str
    args: tuple = ()
    dtype: str = "string"
    avg_bytes: float = 0.0  # average size of a string result

    def children(self):
        return tuple(a for a in self.args if isinstance(a, Expr))

    def __str__(self) -> str:
        return f"{self.name}({', '.join(str(a) for a in self.args)})"


@dataclass(eq=False)
class Alias(Expr):
    child: Expr
    name: str
    expr_id: int = -1

    def children(self):
        return (self.child,)

    @property
    def dtype(self) -> str:  # type: ignore[override]
        return self.child.dtype

    def __str__(self) -> str:
        return f"{self.child} AS {self.name}#{self.expr_id}{'L' if self.dtype == 'bigint' else ''}"

    def to_attr(self) -> Attr:
        avg = getattr(self.child, "avg_bytes", 0.0)
        return Attr(self.name, self.dtype, self.expr_id, self.child.nullable, avg)


# --------------------------------------------------------------------------- predicates


def conjuncts(e: Expr | None) -> list[Expr]:
    if e is None:
        return []
    if isinstance(e, BinaryOp) and e.op == "AND":
        return conjuncts(e.left) + conjuncts(e.right)
    return [e]


def and_all(es: list[Expr]) -> Expr | None:
    out = None
    for e in es:
        out = e if out is None else And(out, e)
    return out


def source_filter(e: Expr) -> str | None:
    """`DataSourceStrategy.translateFilter`: the data source Filter pushed to Parquet, or None."""
    if isinstance(e, IsNotNull) and isinstance(e.child, Attr):
        return f"IsNotNull({e.child.name})"
    if isinstance(e, Attr) and e.dtype == "boolean":  # a boolean column used as a predicate
        return f"EqualTo({e.name},true)"
    if isinstance(e, Not):
        inner = source_filter(e.child)
        return f"Not({inner})" if inner else None
    if isinstance(e, BinaryOp) and e.source_filter:
        if isinstance(e.left, Attr) and isinstance(e.right, Lit):
            return f"{e.source_filter}({e.left.name},{e.right})"
    return None


def null_intolerant_refs(e: Expr) -> list[Attr]:
    """Attributes a predicate rejects nulls of (InferFiltersFromConstraints adds isnotnull for them)."""
    if isinstance(e, Not):
        return null_intolerant_refs(e.child)
    if isinstance(e, Attr) and e.dtype == "boolean":
        return [e]
    if isinstance(e, BinaryOp) and e.source_filter:
        return [a for a in (e.left, e.right) if isinstance(a, Attr)]
    return []


# --------------------------------------------------------------------------- aggregates


@dataclass(eq=False)
class AggFunc:
    """An aggregate function: sum / count / max / min / avg, optionally distinct."""

    name: str  # sum | count | max | min | avg
    child: Expr | None  # None: count(1)
    distinct: bool = False

    @property
    def dtype(self) -> str:
        if self.name == "count":
            return "bigint"
        if self.name == "avg":
            return "double"
        if self.name == "sum":
            return "bigint" if self.child is not None and self.child.dtype in ("int", "bigint", "smallint",
                                                                                 "tinyint") else "double"
        return self.child.dtype if self.child is not None else "bigint"

    def arg_str(self) -> str:
        return "1" if self.child is None else str(self.child)

    def __str__(self) -> str:  # AggregateFunction.toString (no distinct, used for result attribute names)
        return f"{self.name}({self.arg_str()})"

    def agg_string(self, mode: str) -> str:
        prefix = {"partial": "partial_", "merge": "merge_", "final": ""}[mode]
        return f"{prefix}{self.name}({'distinct ' if self.distinct else ''}{self.arg_str()})"

    def buffer_names(self) -> list[tuple[str, str]]:
        """aggBufferAttributes (name, type)."""
        if self.name == "avg":
            return [("sum", "double"), ("count", "bigint")]
        if self.name == "count":
            return [("count", "bigint")]
        return [(self.name, self.dtype)]


@dataclass(eq=False)
class AggExpr:
    """`AggregateExpression` with its result attribute (`sum(x)#85`) and its buffer attributes."""

    func: AggFunc
    result_id: int
    buffer: list[Attr] = field(default_factory=list)

    @property
    def result_attr(self) -> Attr:
        return Attr(str(self.func), self.func.dtype, self.result_id, self.func.name != "count")


# --------------------------------------------------------------------------- types and sizes


def catalog_string(attrs: Iterable[Attr]) -> str:
    """`StructType.catalogString` of the read schema."""
    return "struct<" + ",".join(f"{a.name}:{a.dtype}" for a in attrs) + ">"


def unsafe_row_bytes(attrs: Iterable[Attr]) -> int:
    """Average `UnsafeRow.getSizeInBytes` of rows with these columns."""
    attrs = list(attrs)
    n = len(attrs)
    size = 8 * ((n + 63) // 64) + 8 * n
    for a in attrs:
        if a.dtype not in FIXED_TYPES:
            size += math.ceil((a.avg_bytes + 3.5) / 8) * 8 if a.avg_bytes else 8
    return size


def sort_order(a: Attr) -> str:
    return f"{a} ASC NULLS FIRST"


def bracket(items: Iterable) -> str:
    return "[" + ", ".join(str(i) for i in items) + "]"


def field_string(name: str, values: list) -> str:
    """`ExplainUtils.generateFieldString`."""
    if not values:
        return f"{name}: []"
    return f"{name} [{len(values)}]: {bracket(values)}"


class ExprIds:
    """`NamedExpression.newExprId`: one counter per JVM, so ids grow over the whole application."""

    def __init__(self, start: int = 0):
        self._next = start

    def next(self) -> int:
        n = self._next
        self._next += 1
        return n

    def skip(self, n: int) -> None:
        self._next += n

    def attr(self, name: str, dtype: str, avg_bytes: float = 0.0, nullable: bool = True) -> Attr:
        return Attr(name, dtype, self.next(), nullable, avg_bytes)

    def alias(self, child: Expr, name: str) -> Alias:
        return Alias(child, name, self.next())
