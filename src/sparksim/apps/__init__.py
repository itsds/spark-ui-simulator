"""Synthetic Spark applications: the Phase 2 tiny app and the workloads scenarios run."""

from __future__ import annotations

import importlib

# workload name -> module defining WORKLOAD
WORKLOAD_MODULES = {
    "clickstream_enrichment": "sparksim.apps.daily_enrichment",
}


def workload(name: str):
    try:
        mod = WORKLOAD_MODULES[name]
    except KeyError:
        raise ValueError(f"unknown workload {name!r} (known: {sorted(WORKLOAD_MODULES)})") from None
    return importlib.import_module(mod).WORKLOAD
