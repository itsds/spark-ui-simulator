"""Call sites (stage names, job `Details`) of PySpark DataFrame actions, copied from the reference logs.

Actions that go straight through Py4J (reads and writes) are named after the
Java reflection frame: `parquet at NativeMethodAccessorImpl.java:0`
(EVENT_LOG_FORMAT.md §7). Broadcast jobs run on the broadcast-exchange thread
pool. Source: `ref01-daily-etl-aqe` jobs 3 and 4 (Spark 3.5.9, JDK 17).
"""

from __future__ import annotations

_PY4J_TAIL = [
    "java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
    "java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)",
    "java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)",
    "java.base/java.lang.reflect.Method.invoke(Method.java:569)",
    "py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)",
    "py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)",
    "py4j.Gateway.invoke(Gateway.java:282)",
    "py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)",
    "py4j.commands.CallCommand.execute(CallCommand.java:79)",
    "py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)",
    "py4j.ClientServerConnection.run(ClientServerConnection.java:106)",
    "java.base/java.lang.Thread.run(Thread.java:840)",
]

# DataFrameWriter methods → the Scala frame Py4J calls into.
_WRITER_FRAMES = {
    "parquet": "org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:802)",
}


def py4j_callsite(action: str) -> tuple[str, str]:
    """(short form, long form) for a DataFrame action invoked from Python through Py4J."""
    frame = _WRITER_FRAMES[action]
    return f"{action} at NativeMethodAccessorImpl.java:0", "\n".join([frame, *_PY4J_TAIL])


BROADCAST_SHORT = "$anonfun$withThreadLocalCaptured$1 at FutureTask.java:264"
BROADCAST_LONG = "\n".join([
    "org.apache.spark.sql.execution.SQLExecution$.$anonfun$withThreadLocalCaptured$1(SQLExecution.scala:219)",
    "java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)",
    "java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
    "java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)",
    "java.base/java.lang.Thread.run(Thread.java:840)",
])


# The SparkException of a failed job, as JobEnd's `JobFailed` carries it (`JsonProtocol.exceptionToJson`) and
# the PySpark driver prints it under its Py4JJavaError (ref10, Spark 3.5.9). Created on the DAGScheduler's event
# loop: abortStage after a task set failed, handleJobCancellation for a job AQE cancelled. A job submitted with
# `runJob` (result stages) also gets its caller's frames appended, racing with the event-log writer (ref10/ref11
# show both); AQE's map-stage jobs (`submitMapStage`) keep these frames only.
_DAG = "org.apache.spark.scheduler.DAGScheduler"
_LOOP = "org.apache.spark.scheduler.DAGSchedulerEventProcessLoop"
_EVENT_LOOP_TAIL = [
    (_LOOP, "onReceive", "DAGScheduler.scala", 3036),
    (_LOOP, "onReceive", "DAGScheduler.scala", 3025),
    ("org.apache.spark.util.EventLoop$$anon$1", "run", "EventLoop.scala", 49),
]
ABORT_STAGE_STACK = [
    (_DAG, "failJobAndIndependentStages", "DAGScheduler.scala", 2898),
    (_DAG, "$anonfun$abortStage$2", "DAGScheduler.scala", 2834),
    (_DAG, "$anonfun$abortStage$2$adapted", "DAGScheduler.scala", 2833),
    ("scala.collection.mutable.ResizableArray", "foreach", "ResizableArray.scala", 62),
    ("scala.collection.mutable.ResizableArray", "foreach$", "ResizableArray.scala", 55),
    ("scala.collection.mutable.ArrayBuffer", "foreach", "ArrayBuffer.scala", 49),
    (_DAG, "abortStage", "DAGScheduler.scala", 2833),
    (_DAG, "$anonfun$handleTaskSetFailed$1", "DAGScheduler.scala", 1253),
    (_DAG, "$anonfun$handleTaskSetFailed$1$adapted", "DAGScheduler.scala", 1253),
    ("scala.Option", "foreach", "Option.scala", 407),
    (_DAG, "handleTaskSetFailed", "DAGScheduler.scala", 1253),
    (_LOOP, "doOnReceive", "DAGScheduler.scala", 3102),
    *_EVENT_LOOP_TAIL,
]
CANCEL_JOB_STACK = [
    (_DAG, "failJobAndIndependentStages", "DAGScheduler.scala", 2898),
    (_DAG, "handleJobCancellation", "DAGScheduler.scala", 2773),
    (_LOOP, "doOnReceive", "DAGScheduler.scala", 3055),
    *_EVENT_LOOP_TAIL,
]


def java_frame(cls: str, method: str, file: str, line: int) -> str:
    """`StackTraceElement.toString` for the JDK 17 frames above (Spark's own classes: no module prefix)."""
    return f"{cls}.{method}({file}:{line})"
