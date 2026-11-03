"""Shared event-log value types (Spark 3.5 `JsonProtocol` field order).

Each class mirrors one `JsonProtocol.*ToJson` writer. Field order is JSON key
order. Checked byte for byte against `reference/event-logs/` by
`tests/test_eventlog_roundtrip.py`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .codec import DecodeError, JsonObject, jf

INTERNAL_METRICS_PREFIX = "internal.metrics."


# --------------------------------------------------------------------------- storage


@dataclass(frozen=True)
class StorageLevel(JsonObject):
    use_disk: bool = jf("Use Disk", default=False)
    use_memory: bool = jf("Use Memory", default=False)
    use_off_heap: bool = jf("Use Off Heap", default=False)
    deserialized: bool = jf("Deserialized", default=False)
    replication: int = jf("Replication", default=1)


StorageLevel.NONE = StorageLevel()
StorageLevel.MEMORY_ONLY = StorageLevel(use_memory=True, deserialized=True)
StorageLevel.MEMORY_AND_DISK = StorageLevel(use_disk=True, use_memory=True, deserialized=True)
StorageLevel.MEMORY_AND_DISK_SER = StorageLevel(use_disk=True, use_memory=True)
StorageLevel.DISK_ONLY = StorageLevel(use_disk=True)


@dataclass(frozen=True)
class BlockManagerId(JsonObject):
    executor_id: str = jf("Executor ID")
    host: str = jf("Host")
    port: int = jf("Port")


# --------------------------------------------------------------------------- accumulators


@dataclass
class AccumulableInfo(JsonObject):
    """One accumulator in Task Info / Stage Info / failure reasons.

    `update` and `value` keep their JSON type: Spark writes numbers for
    `internal.metrics.*` and strings for every other accumulator (SQL metrics
    included), and strings for nameless updates inside failure reasons.
    """

    id: int = jf("ID")
    name: str | None = jf("Name", omit_none=True)
    update: int | str | None = jf("Update", omit_none=True)
    value: int | str | None = jf("Value", omit_none=True)
    internal: bool = jf("Internal", default=False)
    count_failed_values: bool = jf("Count Failed Values", default=False)
    metadata: str | None = jf("Metadata", omit_none=True)

    @property
    def is_task_metric(self) -> bool:
        return self.name is not None and self.name.startswith(INTERNAL_METRICS_PREFIX)


def accum_value(name: str | None, v: int) -> int | str:
    """JSON representation of an accumulator value, as `JsonProtocol.accumValueToJson`."""
    if name is not None and name.startswith(INTERNAL_METRICS_PREFIX):
        return int(v)
    return str(v)


# --------------------------------------------------------------------------- RDDs and stages


def rdd_scope(scope_id: int | str, name: str, parent: str | None = None) -> str:
    """`RDDOperationScope.toJson` (Jackson, property order id, name, parent)."""
    d = {"id": str(scope_id), "name": name}
    if parent is not None:
        d["parent"] = json.loads(parent)
    return json.dumps(d, separators=(",", ":"), ensure_ascii=False)


@dataclass
class RDDInfo(JsonObject):
    rdd_id: int = jf("RDD ID")
    name: str = jf("Name")
    scope: str | None = jf("Scope", omit_none=True)  # JSON text, see rdd_scope()
    callsite: str = jf("Callsite", default="")
    parent_ids: list[int] = jf("Parent IDs", default_factory=list)
    storage_level: StorageLevel = jf("Storage Level", default_factory=StorageLevel)
    barrier: bool = jf("Barrier", default=False)
    deterministic_level: str = jf("DeterministicLevel", default="DETERMINATE")  # DETERMINATE|UNORDERED|INDETERMINATE
    num_partitions: int = jf("Number of Partitions", default=0)
    num_cached_partitions: int = jf("Number of Cached Partitions", default=0)
    memory_size: int = jf("Memory Size", default=0)
    disk_size: int = jf("Disk Size", default=0)


@dataclass
class StageInfo(JsonObject):
    stage_id: int = jf("Stage ID")
    attempt_id: int = jf("Stage Attempt ID")
    name: str = jf("Stage Name")
    num_tasks: int = jf("Number of Tasks")
    rdd_infos: list[RDDInfo] = jf("RDD Info", default_factory=list)
    parent_ids: list[int] = jf("Parent IDs", default_factory=list)
    details: str = jf("Details", default="")
    submission_time: int | None = jf("Submission Time", omit_none=True)
    completion_time: int | None = jf("Completion Time", omit_none=True)
    failure_reason: str | None = jf("Failure Reason", omit_none=True)
    accumulables: list[AccumulableInfo] = jf("Accumulables", default_factory=list)
    resource_profile_id: int = jf("Resource Profile Id", default=0)
    shuffle_push_enabled: bool = jf("Shuffle Push Enabled", default=False)
    shuffle_push_mergers_count: int = jf("Shuffle Push Mergers Count", default=0)


# --------------------------------------------------------------------------- tasks


@dataclass
class TaskInfo(JsonObject):
    task_id: int = jf("Task ID")
    index: int = jf("Index")
    attempt: int = jf("Attempt")
    partition_id: int = jf("Partition ID")
    launch_time: int = jf("Launch Time")
    executor_id: str = jf("Executor ID")
    host: str = jf("Host")
    locality: str = jf("Locality")  # PROCESS_LOCAL | NODE_LOCAL | NO_PREF | RACK_LOCAL | ANY
    speculative: bool = jf("Speculative", default=False)
    getting_result_time: int = jf("Getting Result Time", default=0)
    finish_time: int = jf("Finish Time", default=0)
    failed: bool = jf("Failed", default=False)
    killed: bool = jf("Killed", default=False)
    accumulables: list[AccumulableInfo] = jf("Accumulables", default_factory=list)


@dataclass
class StackTraceElement(JsonObject):
    declaring_class: str = jf("Declaring Class")
    method_name: str = jf("Method Name")
    file_name: str | None = jf("File Name")  # null for native frames
    line_number: int = jf("Line Number")


@dataclass
class ExceptionInfo(JsonObject):
    """`JsonProtocol.exceptionToJson` (job failures)."""

    message: str = jf("Message")
    stack_trace: list[StackTraceElement] = jf("Stack Trace", default_factory=list)


# --------------------------------------------------------------------------- executors and resources


@dataclass
class ExecutorInfo(JsonObject):
    host: str = jf("Host")
    total_cores: int = jf("Total Cores")
    log_urls: dict[str, str] = jf("Log Urls", default_factory=dict)
    attributes: dict[str, str] = jf("Attributes", default_factory=dict)
    resources: dict[str, dict] = jf("Resources", default_factory=dict)
    resource_profile_id: int = jf("Resource Profile Id", default=0)
    registration_time: int | None = jf("Registration Time", omit_none=True)
    request_time: int | None = jf("Request Time", omit_none=True)


@dataclass
class ExecutorResourceRequest(JsonObject):
    resource_name: str = jf("Resource Name")
    amount: int = jf("Amount")
    discovery_script: str = jf("Discovery Script", default="")
    vendor: str = jf("Vendor", default="")


@dataclass
class TaskResourceRequest(JsonObject):
    resource_name: str = jf("Resource Name")
    amount: float = jf("Amount")


@dataclass
class BlockUpdatedInfo(JsonObject):
    block_manager_id: BlockManagerId = jf("Block Manager ID")
    block_id: str = jf("Block ID")
    storage_level: StorageLevel = jf("Storage Level")
    memory_size: int = jf("Memory Size")
    disk_size: int = jf("Disk Size")


# --------------------------------------------------------------------------- SQL plan (Jackson-serialized case classes)


@dataclass
class SQLPlanMetric(JsonObject):
    name: str = jf("name")
    accumulator_id: int = jf("accumulatorId")
    metric_type: str = jf("metricType")  # sum | size | timing | nsTiming | average


@dataclass
class SparkPlanInfo(JsonObject):
    node_name: str = jf("nodeName")
    simple_string: str = jf("simpleString")
    children: list[SparkPlanInfo] = jf("children", default_factory=list)
    metadata: dict[str, str] = jf("metadata", default_factory=dict)
    metrics: list[SQLPlanMetric] = jf("metrics", default_factory=list)

    def walk(self):
        yield self
        for c in self.children:
            yield from c.walk()


__all__ = [
    "INTERNAL_METRICS_PREFIX", "AccumulableInfo", "BlockManagerId", "BlockUpdatedInfo", "DecodeError",
    "ExceptionInfo", "ExecutorInfo", "ExecutorResourceRequest", "RDDInfo", "SQLPlanMetric", "SparkPlanInfo",
    "StackTraceElement", "StageInfo", "StorageLevel", "TaskInfo", "TaskResourceRequest", "accum_value",
    "field", "rdd_scope",
]
