"""`explain("formatted")`: the `physicalPlanDescription` of SQLExecutionStart / AQE updates.

A port of `ExplainUtils.processPlan` and `TreeNode.generateTreeString` (Spark 3.5.9):

1. operator ids: post-order (`foreachUp`), one id per node *object* (an identity map), skipping
   WholeStageCodegen and InputAdapter; an AdaptiveSparkPlan numbers its current plan, then its
   initial plan if that is a different tree, then itself; a query stage numbers its plan first;
2. codegen ids: a pre-order walk sets `[codegen id : n]` on every node below a WholeStageCodegen
   until an InputAdapter;
3. the tree: `+- ` / `:- ` connectors, `* ` before nodes inside a codegen stage, query stages with
   `Statistics(...)` once materialized, `== Current Plan ==` / `== Final Plan ==` and
   `== Initial Plan ==` headers under AdaptiveSparkPlan;
4. one block per operator id (post-order, each id once).

Checked line for line against every plan description in `ref01-daily-etl-aqe`.
"""

from __future__ import annotations

from .nodes import AdaptiveSparkPlan, InputAdapter, QueryStage, SparkPlan, WholeStageCodegen


class _Explain:
    def __init__(self) -> None:
        self.op_ids: dict[int, int] = {}  # id(node) -> operator id
        self.nodes: dict[int, SparkPlan] = {}
        self.codegen: dict[int, int] = {}
        self.counter = 0

    # ------------------------------------------------------------------ ids

    def _set(self, n: SparkPlan) -> None:
        if id(n) not in self.op_ids:
            self.counter += 1
            self.op_ids[id(n)] = self.counter
            self.nodes[id(n)] = n

    def gen_ids(self, plan: SparkPlan) -> None:
        for n in _post_order(plan):
            if isinstance(n, (WholeStageCodegen, InputAdapter)):
                continue
            if isinstance(n, AdaptiveSparkPlan):
                self.gen_ids(n.current)
                if n.current is not n.initial:
                    self.gen_ids(n.initial)
                self._set(n)
            elif isinstance(n, QueryStage):
                self.gen_ids(n.plan)
                self._set(n)
            else:
                self._set(n)

    def gen_codegen_ids(self, plan: SparkPlan) -> None:
        current = -1

        def tag(n: SparkPlan, inner: list[SparkPlan]) -> None:
            if current != -1:
                self.codegen[id(n)] = current
            for c in inner:
                self.gen_codegen_ids(c)

        for n in plan.walk():
            if isinstance(n, WholeStageCodegen):
                current = n.codegen_id
            elif isinstance(n, InputAdapter):
                current = -1
            elif isinstance(n, AdaptiveSparkPlan):
                tag(n, [n.current])
            elif isinstance(n, QueryStage):
                tag(n, [n.plan])
            else:
                tag(n, [])

    def collect(self, plan: SparkPlan, out: list[SparkPlan], seen: set[int]) -> None:
        def add(n: SparkPlan) -> None:
            i = self.op_ids.get(id(n))
            if i is not None and i not in seen:
                seen.add(i)
                out.append(n)

        for n in _post_order(plan):
            if isinstance(n, (WholeStageCodegen, InputAdapter)):
                continue
            if isinstance(n, AdaptiveSparkPlan):
                self.collect(n.current, out, seen)
                if n.current is not n.initial:
                    self.collect(n.initial, out, seen)
                add(n)
            elif isinstance(n, QueryStage):
                self.collect(n.plan, out, seen)
                add(n)
            else:
                add(n)

    # ------------------------------------------------------------------ tree string

    def tree(self, n: SparkPlan, depth: int, last: list[bool], prefix: str, indent: int, out: list[str]) -> None:
        if isinstance(n, WholeStageCodegen):
            self.tree(n.children[0], depth, last, "* ", indent, out)
            return
        if isinstance(n, InputAdapter):
            self.tree(n.children[0], depth, last, "", indent, out)
            return
        out.append("   " * indent)
        if depth > 0:
            for is_last in last[:-1]:
                out.append("   " if is_last else ":  ")
            out.append("+- " if last[-1] else ":- ")
        out.append(prefix + n.node_id_string(self.op_ids.get(id(n), "unknown")) + "\n")
        if isinstance(n, AdaptiveSparkPlan):
            if n.current is n.initial:
                self.tree(n.current, depth + 1, [*last, True], "", indent, out)
            else:
                header = "Final Plan" if n.final else "Current Plan"
                for h, plan in ((header, n.current), ("Initial Plan", n.initial)):
                    out.append("   " * depth + f"+- == {h} ==\n")
                    self.tree(plan, 0, [], "", depth + 1, out)
            return
        if isinstance(n, QueryStage):
            self.tree(n.plan, depth + 1, [*last, True], "", indent, out)
            return
        kids = n.children
        for i, c in enumerate(kids):
            self.tree(c, depth + 1, [*last, i == len(kids) - 1], prefix, indent, out)

    def block(self, n: SparkPlan) -> str:
        cg = self.codegen.get(id(n))
        head = f"({self.op_ids[id(n)]}) {n.node_name}" + (f" [codegen id : {cg}]" if cg is not None else "")
        return "\n" + head + "\n" + "".join(f + "\n" for f in n.verbose_fields())


def _post_order(plan: SparkPlan):
    for c in plan.children:
        yield from _post_order(c)
    yield plan


def explain_formatted(plan: SparkPlan) -> str:
    """`QueryExecution.explainString(ExplainMode.fromString("formatted"))` of the executed plan."""
    ex = _Explain()
    ex.gen_ids(plan)
    ex.gen_codegen_ids(plan)
    tree: list[str] = []
    ex.tree(plan, 0, [], "", 0, tree)
    ops: list[SparkPlan] = []
    ex.collect(plan, ops, set())
    return "== Physical Plan ==\n" + "".join(tree) + "\n" + "".join(ex.block(n) for n in ops) + "\n"
