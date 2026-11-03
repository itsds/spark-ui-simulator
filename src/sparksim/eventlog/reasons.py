"""Task end reasons and job results (`JsonProtocol.taskEndReasonToJson` / `jobResultToJson`)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from .codec import DecodeError, JsonObject, jf
from .common import AccumulableInfo, BlockManagerId, ExceptionInfo, StackTraceElement

# --------------------------------------------------------------------------- task end reasons


@dataclass
class TaskEndReason(JsonObject):
    REASON: ClassVar[str] = ""
    _registry: ClassVar[dict[str, type[TaskEndReason]]] = {}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        TaskEndReason._registry[cls.REASON] = cls

    def _json_prefix(self) -> dict:
        return {"Reason": self.REASON}

    @classmethod
    def decode(cls, d: dict) -> TaskEndReason:
        try:
            sub = TaskEndReason._registry[d["Reason"]]
        except KeyError as e:
            raise DecodeError(f"unknown Task End Reason {d.get('Reason')!r}") from e
        return sub.from_json(d, _skip=("Reason",))

    @property
    def successful(self) -> bool:
        return isinstance(self, Success)

    @property
    def updates_accumulators(self) -> bool:
        """DAGScheduler.handleTaskCompletion merges accumulator updates only for these outcomes."""
        return isinstance(self, (Success, ExceptionFailure, TaskKilled))


@dataclass
class Success(TaskEndReason):
    REASON: ClassVar[str] = "Success"


@dataclass
class Resubmitted(TaskEndReason):
    REASON: ClassVar[str] = "Resubmitted"


@dataclass
class TaskResultLost(TaskEndReason):
    REASON: ClassVar[str] = "TaskResultLost"


@dataclass
class UnknownReason(TaskEndReason):
    REASON: ClassVar[str] = "UnknownReason"


@dataclass
class FetchFailed(TaskEndReason):
    REASON: ClassVar[str] = "FetchFailed"
    block_manager_address: BlockManagerId | None = jf("Block Manager Address", omit_none=True)
    shuffle_id: int = jf("Shuffle ID", default=0)
    map_id: int = jf("Map ID", default=0)
    map_index: int = jf("Map Index", default=0)
    reduce_id: int = jf("Reduce ID", default=0)
    message: str = jf("Message", default="")


@dataclass
class ExceptionFailure(TaskEndReason):
    REASON: ClassVar[str] = "ExceptionFailure"
    class_name: str = jf("Class Name", default="")
    description: str = jf("Description", default="")
    stack_trace: list[StackTraceElement] = jf("Stack Trace", default_factory=list)
    full_stack_trace: str = jf("Full Stack Trace", default="")
    # Nameless updates (ID, Update as string). JsonProtocol sorts them by ID.
    accum_updates: list[AccumulableInfo] = jf("Accumulator Updates", default_factory=list)

    def to_json(self) -> dict:
        d = super().to_json()
        d["Accumulator Updates"] = [a.to_json() for a in sorted(self.accum_updates, key=lambda a: a.id)]
        return d


@dataclass
class TaskCommitDenied(TaskEndReason):
    REASON: ClassVar[str] = "TaskCommitDenied"
    job_id: int = jf("Job ID", default=0)
    partition_id: int = jf("Partition ID", default=0)
    attempt_number: int = jf("Attempt Number", default=0)


@dataclass
class ExecutorLostFailure(TaskEndReason):
    REASON: ClassVar[str] = "ExecutorLostFailure"
    executor_id: str = jf("Executor ID", default="")
    exit_caused_by_app: bool = jf("Exit Caused By App", default=True)
    loss_reason: str | None = jf("Loss Reason", omit_none=True)


@dataclass
class TaskKilled(TaskEndReason):
    REASON: ClassVar[str] = "TaskKilled"
    kill_reason: str = jf("Kill Reason", default="")
    accum_updates: list[AccumulableInfo] = jf("Accumulator Updates", default_factory=list)  # not sorted by Spark


# --------------------------------------------------------------------------- job results


@dataclass
class JobResult(JsonObject):
    RESULT: ClassVar[str] = ""
    _registry: ClassVar[dict[str, type[JobResult]]] = {}

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        JobResult._registry[cls.RESULT] = cls

    def _json_prefix(self) -> dict:
        return {"Result": self.RESULT}

    @classmethod
    def decode(cls, d: dict) -> JobResult:
        try:
            sub = JobResult._registry[d["Result"]]
        except KeyError as e:
            raise DecodeError(f"unknown Job Result {d.get('Result')!r}") from e
        return sub.from_json(d, _skip=("Result",))


@dataclass
class JobSucceeded(JobResult):
    RESULT: ClassVar[str] = "JobSucceeded"


@dataclass
class JobFailed(JobResult):
    RESULT: ClassVar[str] = "JobFailed"
    exception: ExceptionInfo = jf("Exception", default_factory=lambda: ExceptionInfo(message=""))


__all__ = [
    "ExceptionFailure", "ExecutorLostFailure", "FetchFailed", "JobFailed", "JobResult", "JobSucceeded",
    "Resubmitted", "Success", "TaskCommitDenied", "TaskEndReason", "TaskKilled", "TaskResultLost",
    "UnknownReason", "field",
]
