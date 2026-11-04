"""Scenarios: `scenarios/<id>/scenario.yaml` → one simulated application per run.

    sc = load_scenario("01-join-skew")
    sc.roles                      # ("last_good", "incident")
    spec = sc.spec("incident")    # engine.runtime.AppSpec: workload + job + run data + faults
    sc.generate("event-logs", "yarn-logs")   # every run's event log (+ container logs)

An incident scenario runs the same job twice (ADR-006): the last good run on the usual data, and the
incident run with its faults injected. The schema is in `schema.py`, the fault mechanisms in `faults.py`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from functools import cached_property
from pathlib import Path

import yaml

from ..apps import workload as get_workload
from ..engine.runtime import AppRun, AppSpec
from ..model.cluster import ClusterSpec, ExecutorSizing
from .faults import RunSetup, apply_fault
from .schema import RUN_ROLES, ScenarioModel, size_mb, spark_value

MiB = 1024 * 1024


def scenarios_dir() -> Path:
    """`$SPARKSIM_SCENARIOS`, else the repository's `scenarios/` (the package runs from the source tree)."""
    env = os.environ.get("SPARKSIM_SCENARIOS")
    return Path(env) if env else Path(__file__).resolve().parents[3] / "scenarios"


def scenario_ids(root: Path | None = None) -> list[str]:
    root = root or scenarios_dir()
    return sorted(p.parent.name for p in root.glob("*/scenario.yaml"))


def load_scenario(ident: str | Path) -> Scenario:
    """By id (`01-join-skew`), by directory, or by the path of a scenario.yaml."""
    p = Path(ident)
    if p.name == "scenario.yaml":
        path = p
    elif p.is_dir():
        path = p / "scenario.yaml"
    else:
        path = scenarios_dir() / str(ident) / "scenario.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"no scenario.yaml for {ident!r} (looked at {path})")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw.get("id") != path.parent.name:
        raise ValueError(f"{path}: id {raw.get('id')!r} does not match its directory {path.parent.name!r}")
    return scenario_from_dict(raw, path.parent)


def load_raw(ident: str) -> dict:
    """A scenario.yaml as plain data (tests derive variants of a scenario from it)."""
    return yaml.safe_load((scenarios_dir() / ident / "scenario.yaml").read_text(encoding="utf-8"))


def scenario_from_dict(raw: dict, directory: Path | None = None) -> Scenario:
    """Validate a scenario (schema, then its workload, datasets, key columns) and return it."""
    model = ScenarioModel.model_validate(raw)
    sc = Scenario(model, directory or scenarios_dir() / model.id)
    sc.validate()
    return sc


@dataclass(frozen=True)
class Scenario:
    model: ScenarioModel
    dir: Path

    @property
    def id(self) -> str:
        return self.model.id

    @property
    def roles(self) -> tuple[str, ...]:
        return tuple(r for r in RUN_ROLES if r in self.model.runs)

    @cached_property
    def workload(self):
        return get_workload(self.model.job.workload)

    def app_id(self, role: str) -> str:
        return f"application_{self.model.job.cluster.cluster_timestamp}_{self.model.runs[role].app_seq:04d}"

    # ------------------------------------------------------------------ AppSpec

    def setup(self, role: str) -> RunSetup:
        """The run's simulation inputs: the workload's data with the run's overrides, then its faults."""
        job, run = self.model.job, self.model.runs[role]
        wl = self.workload
        params = wl.resolve(run.params)
        datasets = wl.datasets(params)
        for name, d in run.data.items():
            if name not in datasets:
                raise ValueError(f"run {role}: data for unknown dataset {name!r} (the workload has {sorted(datasets)})")
            ch = {}
            if d.files is not None:
                ch["files"] = d.files
            if d.mean_file_size is not None:
                ch["mean_file_bytes"] = size_mb(d.mean_file_size) * MiB
            if d.file_size_sigma is not None:
                ch["file_size_sigma"] = d.file_size_sigma
            datasets[name] = replace(datasets[name], **ch)
        setup = RunSetup(datasets=datasets, conf=[(k, spark_value(v)) for k, v in job.conf.items()],
                         sizing=_sizing(job.executors), cluster=_cluster(job.cluster))
        if job.executors.memory_overhead is not None and "spark.executor.memoryOverhead" not in job.conf:
            setup.conf.append(("spark.executor.memoryOverhead", spark_value(job.executors.memory_overhead)))
        for f in run.faults:
            apply_fault(setup, f)
        return setup

    def spec(self, role: str) -> AppSpec:
        job, run = self.model.job, self.model.runs[role]
        wl = self.workload
        params = wl.resolve(run.params)
        s = self.setup(role)
        return AppSpec(
            seed=run.seed, app_name=job.app_name, user=job.user, script=wl.script,
            started_at=run.started_at.isoformat(timespec="milliseconds"), app_seq=run.app_seq,
            cluster=s.cluster, sizing=s.sizing, inputs=tuple(s.datasets.values()),
            queries=wl.queries(params, s.datasets), conf=tuple(s.conf), queue=job.queue,
            native_memory_per_task=s.native_per_task, executor_kills=tuple(s.executor_kills),
        )

    def validate(self) -> None:
        """Everything the schema can't see: the workload, its params, datasets and key columns."""
        for role in self.roles:
            self.spec(role)

    # ------------------------------------------------------------------ outputs

    def build(self, role: str):
        from ..engine.emit import build_events

        run = AppRun(self.spec(role)).run()
        return run, build_events(run)

    def generate(self, out_dir: str | Path, logs_dir: str | Path | None = None,
                 roles: tuple[str, ...] | None = None) -> list[Path]:
        """Write each run's event log (and its YARN container logs under `logs_dir`)."""
        from ..eventlog import write_event_log
        from ..yarnlogs import write_container_logs

        paths = []
        for role in roles or self.roles:
            run, events = self.build(role)
            if logs_dir is not None:
                write_container_logs(run, logs_dir)
            paths.append(write_event_log(Path(out_dir) / run.yapp.event_log_name, events))
        return paths


def _sizing(e) -> ExecutorSizing:
    return ExecutorSizing(instances=e.instances, cores=e.cores, memory_mb=size_mb(e.memory),
                          overhead_mb=None if e.memory_overhead is None else size_mb(e.memory_overhead),
                          driver_memory_mb=size_mb(e.driver_memory), driver_cores=e.driver_cores)


def _cluster(c) -> ClusterSpec:
    return ClusterSpec(
        cluster_timestamp=c.cluster_timestamp, racks=c.racks, hosts_per_rack=c.hosts_per_rack,
        nm_vcores=c.nm_vcores, nm_memory_mb=size_mb(c.nm_memory), min_allocation_mb=size_mb(c.min_allocation),
        max_allocation_mb=size_mb(c.max_allocation), domain=c.domain, host_prefix=c.host_prefix,
        rm_host=c.rm_host, rm_standby_hosts=tuple(c.rm_standby_hosts), rm_cluster_id=c.rm_cluster_id,
        rm_epoch=c.rm_epoch, occupancy=tuple(c.occupancy),
    )
