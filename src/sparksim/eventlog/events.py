"""Every Spark 3.5 listener event we read or write, in `JsonProtocol` key order.

Core events follow `org.apache.spark.util.JsonProtocol`. SQL events
(`org.apache.spark.sql.execution.ui.*`) are Scala case classes that Jackson
serializes in constructor order. The events marked *unverified* appear in no
reference log yet. They follow the Spark 3.5 source and will be checked when a
reference log contains them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from .codec import DecodeError, JsonObject, jf
from .common import (
    BlockManagerId,
    BlockUpdatedInfo,
    ExecutorInfo,
    ExecutorResourceRequest,
    SparkPlanInfo,
    SQLPlanMetric,
    StageInfo,
    TaskInfo,
    TaskResourceRequest,
)
from .metrics import ExecutorMetrics, TaskMetrics
from .reasons import JobResult, TaskEndReason

SQL_UI = "org.apache.spark.sql.execution.ui."


@dataclass
class Event(JsonObject):
    EVENT: ClassVar[str] = ""
    _registry: ClassVar[dict[str, type[Event]]] = {}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if cls.EVENT:
            Event._registry[cls.EVENT] = cls

    def _json_prefix(self) -> dict:
        return {"Event": self.EVENT}

    @classmethod
    def decode(cls, d: dict) -> Event:
        name = d.get("Event")
        sub = Event._registry.get(name)
        if sub is None:
            raise DecodeError(f"unsupported event {name!r}")
        return sub.from_json(d, _skip=("Event",))

    @property
    def short_name(self) -> str:
        return self.EVENT.rsplit(".", 1)[-1]


# --------------------------------------------------------------------------- application / environment


@dataclass
class LogStart(Event):
    EVENT: ClassVar[str] = "SparkListenerLogStart"
    spark_version: str = jf("Spark Version", default="3.5.9")


@dataclass
class ResourceProfileAdded(Event):
    EVENT: ClassVar[str] = "SparkListenerResourceProfileAdded"
    resource_profile_id: int = jf("Resource Profile Id", default=0)
    executor_resources: dict[str, ExecutorResourceRequest] = jf("Executor Resource Requests", default_factory=dict)
    task_resources: dict[str, TaskResourceRequest] = jf("Task Resource Requests", default_factory=dict)


@dataclass
class EnvironmentUpdate(Event):
    EVENT: ClassVar[str] = "SparkListenerEnvironmentUpdate"
    jvm_information: dict[str, str] = jf("JVM Information", default_factory=dict)
    spark_properties: dict[str, str] = jf("Spark Properties", default_factory=dict)
    hadoop_properties: dict[str, str] = jf("Hadoop Properties", default_factory=dict)
    system_properties: dict[str, str] = jf("System Properties", default_factory=dict)
    metrics_properties: dict[str, str] = jf("Metrics Properties", default_factory=dict)
    classpath_entries: dict[str, str] = jf("Classpath Entries", default_factory=dict)


@dataclass
class ApplicationStart(Event):
    EVENT: ClassVar[str] = "SparkListenerApplicationStart"
    app_name: str = jf("App Name", default="")
    app_id: str | None = jf("App ID", omit_none=True)
    timestamp: int = jf("Timestamp", default=0)
    user: str = jf("User", default="")
    app_attempt_id: str | None = jf("App Attempt ID", omit_none=True)
    driver_logs: dict[str, str] | None = jf("Driver Logs", omit_none=True)
    driver_attributes: dict[str, str] | None = jf("Driver Attributes", omit_none=True)


@dataclass
class ApplicationEnd(Event):
    EVENT: ClassVar[str] = "SparkListenerApplicationEnd"
    timestamp: int = jf("Timestamp", default=0)


# --------------------------------------------------------------------------- executors / block managers


@dataclass
class BlockManagerAdded(Event):
    EVENT: ClassVar[str] = "SparkListenerBlockManagerAdded"
    block_manager_id: BlockManagerId = jf("Block Manager ID")
    max_memory: int = jf("Maximum Memory", default=0)
    timestamp: int = jf("Timestamp", default=0)
    max_on_heap_memory: int | None = jf("Maximum Onheap Memory", omit_none=True)
    max_off_heap_memory: int | None = jf("Maximum Offheap Memory", omit_none=True)


@dataclass
class BlockManagerRemoved(Event):
    EVENT: ClassVar[str] = "SparkListenerBlockManagerRemoved"
    block_manager_id: BlockManagerId = jf("Block Manager ID")
    timestamp: int = jf("Timestamp", default=0)


@dataclass
class ExecutorAdded(Event):
    EVENT: ClassVar[str] = "SparkListenerExecutorAdded"
    timestamp: int = jf("Timestamp", default=0)
    executor_id: str = jf("Executor ID", default="")
    executor_info: ExecutorInfo = jf("Executor Info", default=None)


@dataclass
class ExecutorRemoved(Event):
    EVENT: ClassVar[str] = "SparkListenerExecutorRemoved"
    timestamp: int = jf("Timestamp", default=0)
    executor_id: str = jf("Executor ID", default="")
    removed_reason: str = jf("Removed Reason", default="")


@dataclass
class BlockUpdated(Event):
    EVENT: ClassVar[str] = "SparkListenerBlockUpdated"
    block_updated_info: BlockUpdatedInfo = jf("Block Updated Info")


@dataclass
class UnpersistRDD(Event):
    EVENT: ClassVar[str] = "SparkListenerUnpersistRDD"
    rdd_id: int = jf("RDD ID", default=0)


@dataclass
class StageExecutorMetrics(Event):
    """*Unverified* (needs spark.eventLog.logStageExecutorMetrics=true)."""

    EVENT: ClassVar[str] = "SparkListenerStageExecutorMetrics"
    executor_id: str = jf("Executor ID", default="")
    stage_id: int = jf("Stage ID", default=0)
    stage_attempt_id: int = jf("Stage Attempt ID", default=0)
    executor_metrics: ExecutorMetrics = jf("Executor Metrics", default_factory=ExecutorMetrics)


# --------------------------------------------------------------------------- jobs / stages / tasks


@dataclass
class JobStart(Event):
    EVENT: ClassVar[str] = "SparkListenerJobStart"
    job_id: int = jf("Job ID", default=0)
    submission_time: int = jf("Submission Time", default=0)
    stage_infos: list[StageInfo] = jf("Stage Infos", default_factory=list)
    stage_ids: list[int] = jf("Stage IDs", default_factory=list)
    properties: dict[str, str] = jf("Properties", default_factory=dict)


@dataclass
class JobEnd(Event):
    EVENT: ClassVar[str] = "SparkListenerJobEnd"
    job_id: int = jf("Job ID", default=0)
    completion_time: int = jf("Completion Time", default=0)
    job_result: JobResult = jf("Job Result", default=None)


@dataclass
class StageSubmitted(Event):
    EVENT: ClassVar[str] = "SparkListenerStageSubmitted"
    stage_info: StageInfo = jf("Stage Info")
    properties: dict[str, str] | None = jf("Properties", omit_none=True)


@dataclass
class StageCompleted(Event):
    EVENT: ClassVar[str] = "SparkListenerStageCompleted"
    stage_info: StageInfo = jf("Stage Info")


@dataclass
class TaskStart(Event):
    EVENT: ClassVar[str] = "SparkListenerTaskStart"
    stage_id: int = jf("Stage ID", default=0)
    stage_attempt_id: int = jf("Stage Attempt ID", default=0)
    task_info: TaskInfo = jf("Task Info", default=None)


@dataclass
class TaskGettingResult(Event):
    EVENT: ClassVar[str] = "SparkListenerTaskGettingResult"
    task_info: TaskInfo = jf("Task Info")


@dataclass
class TaskEnd(Event):
    EVENT: ClassVar[str] = "SparkListenerTaskEnd"
    stage_id: int = jf("Stage ID", default=0)
    stage_attempt_id: int = jf("Stage Attempt ID", default=0)
    task_type: str = jf("Task Type", default="ResultTask")  # ShuffleMapTask | ResultTask
    reason: TaskEndReason = jf("Task End Reason", default=None)
    task_info: TaskInfo = jf("Task Info", default=None)
    task_executor_metrics: ExecutorMetrics = jf("Task Executor Metrics", default_factory=ExecutorMetrics)
    task_metrics: TaskMetrics | None = jf("Task Metrics", omit_none=True)  # absent for FetchFailed / ExecutorLost


# --------------------------------------------------------------------------- SQL (Jackson case classes)


@dataclass
class SQLExecutionStart(Event):
    EVENT: ClassVar[str] = SQL_UI + "SparkListenerSQLExecutionStart"
    execution_id: int = jf("executionId", default=0)
    root_execution_id: int = jf("rootExecutionId", default=0)
    description: str = jf("description", default="")
    details: str = jf("details", default="")
    physical_plan_description: str = jf("physicalPlanDescription", default="")
    spark_plan_info: SparkPlanInfo = jf("sparkPlanInfo", default=None)
    time: int = jf("time", default=0)
    modified_configs: dict[str, str] = jf("modifiedConfigs", default_factory=dict)
    job_tags: list[str] = jf("jobTags", default_factory=list)


@dataclass
class SQLExecutionEnd(Event):
    EVENT: ClassVar[str] = SQL_UI + "SparkListenerSQLExecutionEnd"
    execution_id: int = jf("executionId", default=0)
    time: int = jf("time", default=0)
    error_message: str | None = jf("errorMessage", default="")


@dataclass
class SQLAdaptiveExecutionUpdate(Event):
    EVENT: ClassVar[str] = SQL_UI + "SparkListenerSQLAdaptiveExecutionUpdate"
    execution_id: int = jf("executionId", default=0)
    physical_plan_description: str = jf("physicalPlanDescription", default="")
    spark_plan_info: SparkPlanInfo = jf("sparkPlanInfo", default=None)


@dataclass
class SQLAdaptiveSQLMetricUpdates(Event):
    """*Unverified* (AQE adds metrics for nodes it creates; not seen in our reference logs)."""

    EVENT: ClassVar[str] = SQL_UI + "SparkListenerSQLAdaptiveSQLMetricUpdates"
    execution_id: int = jf("executionId", default=0)
    sql_plan_metrics: list[SQLPlanMetric] = jf("sqlPlanMetrics", default_factory=list)


@dataclass
class DriverAccumUpdates(Event):
    EVENT: ClassVar[str] = SQL_UI + "SparkListenerDriverAccumUpdates"
    execution_id: int = jf("executionId", default=0)
    accum_updates: list[list[int]] = jf("accumUpdates", default_factory=list)  # [[accumulatorId, value], ...]


EVENT_TYPES: dict[str, type[Event]] = dict(Event._registry)

__all__ = [
    "EVENT_TYPES", "SQL_UI", "ApplicationEnd", "ApplicationStart", "BlockManagerAdded", "BlockManagerRemoved",
    "BlockUpdated", "DriverAccumUpdates", "EnvironmentUpdate", "Event", "ExecutorAdded", "ExecutorRemoved",
    "JobEnd", "JobStart", "LogStart", "ResourceProfileAdded", "SQLAdaptiveExecutionUpdate",
    "SQLAdaptiveSQLMetricUpdates", "SQLExecutionEnd", "SQLExecutionStart", "StageCompleted",
    "StageExecutorMetrics", "StageSubmitted", "TaskEnd", "TaskGettingResult", "TaskStart", "UnpersistRDD", "field",
]
