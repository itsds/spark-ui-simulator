"""What a shuffle fetch from a dead executor looks like: FetchFailed messages and the executor's error lines.

Checked against real Spark 3.5.9 runs without the external shuffle service: `reference/event-logs/ref12-
fetch-failure` (a write stage after a sort-merge join, the executor preempted by YARN) and `ref05-failures`
(an RDD job, the executor killed while blocks were in flight), plus the line numbers of the 3.5.9 bytecode
(`ShuffleBlockFetcherIterator.initialize` sends its first requests at line 715, `next()` the later ones at 1067).

Three ways a reduce task meets the dead executor (`variant`):

* `init`: its first batch of requests (up to spark.reducer.maxSizeInFlight) includes one to the dead
  executor: `createAndStart` asks the driver, which knows it is dead: ExecutorDeadException, no retry. The
  cause's frames run up the RDD chain from the ShuffledRowRDD (ref12).
* `next`: the same for a request sent later, while the task consumes its blocks (`next()` refills the
  window): the cause's frames are the iterator's, "... N more" (Java's frames in common).
* `retry`: the request was in flight when the executor died: Connection reset, RetryingBlockTransferor retries
  after spark.shuffle.io.retryWait (5 s) and then gets ExecutorDeadException (ref05).

The task thread throws FetchFailedException where its operator pulls rows: the Sort below the join
(`sort_addToSorter_0` of its codegen stage), the join (`smj_findNextJoinRows_0`), the write's Sort if it is in
the join's codegen stage, then FileFormatWriter.executeTask.
"""

from __future__ import annotations

from ..sql import nodes as sqln

SBFI = "org.apache.spark.storage.ShuffleBlockFetcherIterator"
GEN = "org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage"
WSCG_ITER = ("org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator"
             "$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)")
BUFFERED = "org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)"

# ShuffleBlockFetcherIterator.next down to the operator's input iterator (ref12)
_READ_CHAIN = [
    "org.apache.spark.util.CompletionIterator.next(CompletionIterator.scala:29)",
    "scala.collection.Iterator$$anon$11.nextCur(Iterator.scala:486)",
    "scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:492)",
    "scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)",
    "org.apache.spark.util.CompletionIterator.hasNext(CompletionIterator.scala:31)",
    "org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)",
    "scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)",
]
_WRITE_TASK = [
    "org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:385)",
    "org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)",
    "org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)",
    "org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)",
]
_RDD_COMPUTE = {
    "MapPartitionsRDD": "org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)",
    "ZippedPartitionsRDD2": "org.apache.spark.rdd.ZippedPartitionsRDD2.compute(ZippedPartitionsRDD.scala:89)",
    "ShuffledRowRDD": "org.apache.spark.sql.execution.ShuffledRowRDD.compute(ShuffledRowRDD.scala:232)",
}
_RDD_ITER = ["org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)",
             "org.apache.spark.rdd.RDD.iterator(RDD.scala:331)"]
_RESULT_TASK = [
    "org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)",
    "org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)",
    "org.apache.spark.scheduler.Task.run(Task.scala:141)",
    "org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:621)",
    "org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)",
    "org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)",
    "org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)",
    "org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:624)",
    "java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
    "java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)",
    "java.base/java.lang.Thread.run(Thread.java:840)",
]
_SEND = [
    "org.apache.spark.network.netty.NettyBlockTransferService$$anon$2.createAndStart(NettyBlockTransferService."
    "scala:141)",
    "org.apache.spark.network.shuffle.RetryingBlockTransferor.transferAllOutstanding(RetryingBlockTransferor.java:"
    "173)",
    "org.apache.spark.network.shuffle.RetryingBlockTransferor.start(RetryingBlockTransferor.java:152)",
    "org.apache.spark.network.netty.NettyBlockTransferService.fetchBlocks(NettyBlockTransferService.scala:151)",
    f"{SBFI}.sendRequest(ShuffleBlockFetcherIterator.scala:374)",
    f"{SBFI}.send$1(ShuffleBlockFetcherIterator.scala:1209)",
    f"{SBFI}.fetchUpToMaxBytes(ShuffleBlockFetcherIterator.scala:1201)",
]
_RETRY = [
    "org.apache.spark.network.netty.NettyBlockTransferService$$anon$2.createAndStart(NettyBlockTransferService."
    "scala:141)",
    "org.apache.spark.network.shuffle.RetryingBlockTransferor.transferAllOutstanding(RetryingBlockTransferor.java:"
    "173)",
    "org.apache.spark.network.shuffle.RetryingBlockTransferor.lambda$initiateRetry$0(RetryingBlockTransferor.java:"
    "206)",
    "java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)",
    "java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)",
    "java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
    "java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)",
    "io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)",
    "java.base/java.lang.Thread.run(Thread.java:840)",
]
# the netty event loop that sees the connection reset (ref05)
CONNECTION_RESET = [
    "java.base/sun.nio.ch.SocketChannelImpl.throwConnectionReset(SocketChannelImpl.java:394)",
    "java.base/sun.nio.ch.SocketChannelImpl.read(SocketChannelImpl.java:426)",
    "io.netty.buffer.PooledByteBuf.setBytes(PooledByteBuf.java:254)",
    "io.netty.buffer.AbstractByteBuf.writeBytes(AbstractByteBuf.java:1132)",
    "io.netty.channel.socket.nio.NioSocketChannel.doReadBytes(NioSocketChannel.java:357)",
    "io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:151)",
    "io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:788)",
    "io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:724)",
    "io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:650)",
    "io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:562)",
    "io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997)",
    "io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)",
    "io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)",
    "java.base/java.lang.Thread.run(Thread.java:840)",
]


def executor_dead(executor_id: str) -> str:
    return (f"org.apache.spark.ExecutorDeadException: [INTERNAL_ERROR_NETWORK] The relative remote executor(Id: "
            f"{executor_id}), which maintains the block data to fetch is dead.")


def _codegen_of(plan, node) -> int | None:
    """The codegen id of the WholeStageCodegen that contains `node` (None: not in one)."""
    def walk(n, cg):
        if n is node:
            return cg
        if isinstance(n, sqln.InputAdapter):
            cg = None
        if isinstance(n, sqln.WholeStageCodegen):
            cg = n.codegen_id
        for c in n.children:
            r = walk(c, cg)
            if r is not None:
                return r
        return None
    return walk(plan, None)


def _join_sides(plan):
    """(the SortMergeJoin, its Sort children, the ops above the join in its codegen stage, outermost first)."""
    above: list = []

    def find(n, path):
        if isinstance(n, sqln.SortMergeJoin):
            return n, path
        for c in n.children:
            r = find(c, path + [n])
            if r is not None:
                return r
        return None

    found = find(plan, [])
    if found is None:
        return None
    smj, path = found
    for n in reversed(path):  # walk up from the join while in its codegen stage
        if isinstance(n, (sqln.WholeStageCodegen, sqln.InputAdapter)):
            break
        above.append(n)
    sorts = []
    for c in smj.children:
        n = c
        while not isinstance(n, sqln.Sort):
            n = n.children[0]
        sorts.append(n)
    return smj, sorts, list(reversed(above))


def _write_files(plan):
    return next((n for n in plan.walk() if isinstance(n, sqln.WriteFiles)), plan)


def join_side(plan, exchange: str) -> int:
    """Which side of the stage's sort-merge join reads shuffle `exchange` (0: streamed/left, 1: buffered)."""
    plan = _write_files(plan)
    sides = _join_sides(plan)
    if sides is None or not isinstance(plan, sqln.WriteFiles):
        raise NotImplementedError("FetchFailed outside a write stage after a sort-merge join has no real "
                                  "reference yet (its operators' frames are unknown)")
    for i, srt in enumerate(sides[1]):
        if any(isinstance(n, sqln.QueryStage) and getattr(n, "name", None) == exchange for n in srt.walk()):
            return i
    raise ValueError(f"no side of the join reads {exchange}")


def task_frames(plan, source: int) -> list[str]:
    """The task thread's frames when shuffle source `source` of a write stage after a sort-merge join fails."""
    plan = _write_files(plan)
    sides = _join_sides(plan)
    if sides is None or not isinstance(plan, sqln.WriteFiles):
        raise NotImplementedError("FetchFailed outside a write stage after a sort-merge join has no real "
                                  "reference yet (its operators' frames are unknown)")
    smj, sorts, above = sides
    k = _codegen_of(plan, sorts[source])
    j = _codegen_of(plan, smj)
    out = [f"{SBFI}.next(ShuffleBlockFetcherIterator.scala:971)", f"{SBFI}.next(ShuffleBlockFetcherIterator.scala:86)"]
    out += _READ_CHAIN
    out += [f"{GEN}{k}.sort_addToSorter_0$(Unknown Source)", f"{GEN}{k}.processNext(Unknown Source)", BUFFERED,
            WSCG_ITER, f"{GEN}{j}.smj_findNextJoinRows_0$(Unknown Source)"]
    if any(isinstance(n, sqln.Sort) for n in above):  # the write's Sort, in the join's codegen stage
        out.append(f"{GEN}{j}.sort_addToSorter_0$(Unknown Source)")
    out += [f"{GEN}{j}.processNext(Unknown Source)", BUFFERED, WSCG_ITER]
    out += _WRITE_TASK
    out += [_RDD_COMPUTE["MapPartitionsRDD"], *_RDD_ITER]  # the WriteFiles RDD
    return out + _RESULT_TASK


def _rdd_chain(rdds, source: int) -> list[str]:
    """RDD.compute frames from the source's ShuffledRowRDD up to (not including) the stage's last RDD."""
    by_id = {r.rdd_id: r for r in rdds}
    child_of: dict[int, int] = {}
    for r in rdds:
        for p in r.parent_ids:
            child_of[p] = r.rdd_id
    root = rdds[0].rdd_id
    zipped = next(r for r in rdds if r.name == "ZippedPartitionsRDD2")
    side = zipped.parent_ids[source]
    rid = side
    while by_id[rid].name != "ShuffledRowRDD":
        rid = by_id[rid].parent_ids[0]
    out = []
    while rid != root:
        out += [_RDD_COMPUTE[by_id[rid].name], *_RDD_ITER]
        rid = child_of[rid]
    return out


def cause_frames(stage, source: int, variant: str) -> tuple[list[str], list[str]]:
    """(the ExecutorDeadException's frames as Java prints them in the message, all of its frames)."""
    outer = task_frames(stage.plan_root, source)
    if variant == "retry":
        return list(_RETRY), list(_RETRY)
    if variant == "next":
        full = _SEND + [f"{SBFI}.next(ShuffleBlockFetcherIterator.scala:1067)"] + outer[1:]
    else:  # init: the iterator's constructor, called from ShuffledRowRDD.compute
        full = _SEND + [f"{SBFI}.initialize(ShuffleBlockFetcherIterator.scala:715)",
                        f"{SBFI}.<init>(ShuffleBlockFetcherIterator.scala:194)",
                        "org.apache.spark.shuffle.BlockStoreShuffleReader.read(BlockStoreShuffleReader.scala:73)"]
        full += _rdd_chain(stage.rdds, source) + outer[-(len(_RESULT_TASK) + 3):]
    m, n = len(full) - 1, len(outer) - 1
    while m >= 0 and n >= 0 and full[m] == outer[n]:
        m, n = m - 1, n - 1
    return full[:m + 1] + [f"... {len(full) - 1 - m} more"], full


def fetch_failed_message(stage, source: int, executor_id: str, variant: str) -> str:
    """`FetchFailed.message` (Utils.exceptionString of the FetchFailedException), also the stage's failure
    reason when it is the first."""
    outer = ["org.apache.spark.errors.SparkCoreErrors$.fetchFailedError(SparkCoreErrors.scala:437)",
             f"{SBFI}.throwFetchFailedException(ShuffleBlockFetcherIterator.scala:1239)",
             *task_frames(stage.plan_root, source)]
    shown, _ = cause_frames(stage, source, variant)
    lines = ["org.apache.spark.shuffle.FetchFailedException", *(f"\tat {f}" for f in outer),
             f"Caused by: {executor_dead(executor_id)}"]
    lines += [f"\t{f}" if f.startswith("...") else f"\tat {f}" for f in shown]
    return "\n".join(lines) + "\n"


def executor_dead_stack(stage, source: int, executor_id: str, variant: str) -> str:
    """The exception as an executor's ERROR line prints it (every frame)."""
    _, full = cause_frames(stage, source, variant)
    return "\n".join([executor_dead(executor_id), *(f"\tat {f}" for f in full)])
