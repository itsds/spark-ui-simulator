"""What a workload is: a PySpark job's code and the data it reads, separate from how a scenario runs it.

A scenario (`scenarios/<id>/scenario.yaml`, `sparksim.scenario`) picks a workload by name, says how and
where it is submitted (cluster, executors, `--conf`), and describes each run: its date and other
parameters, how its input data differs from the workload's usual data, and the faults injected into it.

A workload module defines `WORKLOAD = Workload(...)`:

* `params`: the job's parameters and their defaults (e.g. the date it processes);
* `datasets(params)`: the input tables as they usually are, by name (`InputDataset`: files, sizes,
  key distributions);
* `queries(params, datasets)`: the DataFrame actions, written like the PySpark script, taking their key
  spaces from the datasets so that a skewed key in the data shows up in the shuffles;
* `dist_resources(params)`: what the script's spark-submit ships from HDFS with `--jars` / `--files`
  (path, bytes), at those params.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from ..engine.runtime import InputDataset
from ..sql.dataframe import SqlQuery

Params = Mapping[str, object]


@dataclass(frozen=True)
class Workload:
    name: str
    script: str  # the PySpark script (spark-submit primary resource)
    description: str
    datasets: Callable[[Params], dict[str, InputDataset]]
    queries: Callable[[Params, Mapping[str, InputDataset]], tuple[SqlQuery, ...]]
    params: Mapping[str, object] = field(default_factory=dict)  # defaults
    dist_resources: Callable[[Params], tuple[tuple[str, int], ...]] = lambda p: ()

    def submit_conf(self, params: Params) -> list[tuple[str, str]]:
        """The `spark.yarn.dist.jars` / `spark.yarn.dist.files` spark-submit records for the dist resources."""
        res = self.dist_resources(params)
        jars = [path for path, _ in res if path.endswith(".jar")]
        files = [path for path, _ in res if not path.endswith(".jar")]
        return ([("spark.yarn.dist.jars", ",".join(jars))] if jars else []) + (
            [("spark.yarn.dist.files", ",".join(files))] if files else [])

    def resolve(self, params: Params) -> dict[str, object]:
        unknown = set(params) - set(self.params)
        if unknown:
            raise ValueError(f"workload {self.name}: unknown params {sorted(unknown)} "
                             f"(it takes {sorted(self.params)})")
        return {**self.params, **params}
