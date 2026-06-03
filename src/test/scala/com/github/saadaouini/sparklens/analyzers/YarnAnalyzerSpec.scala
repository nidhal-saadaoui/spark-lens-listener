package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{ExecutorData, SparkAppModel, TaskData, TaskMetrics, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YarnAnalyzerSpec extends AnyFlatSpec with Matchers {

  private val yarnProps = Map("spark.master" -> "yarn")

  // Executor with explicit YARN add/remove times
  private def exec(id: String, addedMs: Long, removedMs: Option[Long] = None,
                   reason: Option[String] = None, host: String = "node-01"): (String, ExecutorData) =
    id -> ExecutorData(executorId = id, host = host, totalCores = 4,
                       addedTimeMs = addedMs, removedTimeMs = removedMs,
                       removalReason = reason)

  // Failed task on a specific host
  private def failedTask(id: Long, host: String): TaskData = TaskData(
    taskId = id, index = id.toInt, attempt = 0, executorId = "0", host = host,
    status = "FAILED", launchTimeMs = 0L, finishTimeMs = 1000L,
    failed = true, killed = false, speculative = false,
    errorMessage = Some("error"), metrics = TaskMetrics(),
  )

  // App starting at t=0 with executor added at t=X
  private def appWithQueueWait(waitMs: Long, props: Map[String, String] = yarnProps): SparkAppModel = {
    val e = exec("0", addedMs = waitMs, removedMs = None)
    app(executors = Map(e), props = props).copy(startTimeMs = 0L)
  }

  // ── Guard: skip when not on YARN ─────────────────────────────────────────

  "YarnAnalyzer" should "produce no YARN-specific issues when not on YARN" in {
    // No YARN markers in props — checks 1, 2, 5 should not fire
    // (checks 3 and 4 are cluster-agnostic)
    val a = appWithQueueWait(60000L, props = Map.empty)
    YarnAnalyzer.analyze(a).filter(i =>
      i.id == "yarn-queue-wait" || i.id == "yarn-vmem-oom-kill" ||
      i.id == "yarn-pyspark-overhead-risk"
    ) shouldBe empty
  }

  // ── Check 1: Queue wait time ─────────────────────────────────────────────

  it should "flag Info when queue wait exceeds 30s" in {
    val a = appWithQueueWait(waitMs = 45000L)
    val issues = YarnAnalyzer.analyze(a)
    val i = issues.filter(_.id == "yarn-queue-wait")
    i should not be empty
    i.head.metrics("queue_wait_ms").toLong shouldBe 45000L
    i.head.estimatedImpact.flatMap(_.savedTimeMs) shouldBe Some(45000L)
  }

  it should "flag Warning when queue wait exceeds 2 minutes" in {
    val a = appWithQueueWait(waitMs = 150000L)
    YarnAnalyzer.analyze(a).filter(_.id == "yarn-queue-wait").head.severity shouldBe Warning
  }

  it should "not flag queue wait below 30s threshold" in {
    val a = appWithQueueWait(waitMs = 10000L)
    YarnAnalyzer.analyze(a).filter(_.id == "yarn-queue-wait") shouldBe empty
  }

  it should "not flag queue wait when no executors exist" in {
    val a = app(props = yarnProps).copy(startTimeMs = 0L)
    YarnAnalyzer.analyze(a).filter(_.id == "yarn-queue-wait") shouldBe empty
  }

  // ── Check 2: Virtual memory OOM ──────────────────────────────────────────

  it should "flag Warning when YARN kills for virtual memory" in {
    val execs = Map(exec("0", 0L, Some(60000L),
      Some("Container killed by YARN for exceeding virtual memory limits. 12 GB of 5 GB virtual memory used.")))
    val issues = YarnAnalyzer.analyze(app(executors = execs, props = yarnProps))
    val i = issues.filter(_.id == "yarn-vmem-oom-kill")
    i should not be empty
    i.head.severity shouldBe Warning
    i.head.configFix.get should include("vmem-check-enabled=false")
  }

  it should "not flag vmem for physical memory kills (exit 137)" in {
    // Physical OOM is handled by DynamicAllocationAnalyzer, not YarnAnalyzer
    val execs = Map(exec("0", 0L, Some(60000L),
      Some("Container exited with a non-zero exit code 137.")))
    YarnAnalyzer.analyze(app(executors = execs, props = yarnProps))
      .filter(_.id == "yarn-vmem-oom-kill") shouldBe empty
  }

  it should "not flag vmem for a generic non-YARN virtual memory message" in {
    // No YARN markers in props OR in the reason string — isYarn() returns false
    val execs = Map(exec("0", 0L, Some(60000L),
      Some("Process exceeded virtual memory limit and was terminated by the OS.")))
    YarnAnalyzer.analyze(app(executors = execs, props = Map.empty))
      .filter(_.id == "yarn-vmem-oom-kill") shouldBe empty
  }

  // ── Check 3: Hot-node failure pattern ────────────────────────────────────

  it should "flag Warning when one host accounts for >= 50% of failures" in {
    val badTasks  = (0L until 8L).map(i => failedTask(i, "bad-node"))
    val goodTasks = (8L until 10L).map(i => failedTask(i, "good-node"))
    val s = stage(stageId = 0, tasks = badTasks ++ goodTasks)
      .copy(numTasks = 10, hasExactAggregates = true, exactTaskCount = 10, exactFailedCount = 10)
    val issues = YarnAnalyzer.analyze(app(stages = Map(0 -> s), props = yarnProps))
    val i = issues.filter(_.id == "yarn-hot-node-failure")
    i should not be empty
    i.head.metrics("hot_node") shouldBe "bad-node"
    i.head.metrics("node_failed_tasks").toInt shouldBe 8
    i.head.configFix.get should include("excludeOnFailure.enabled=true")
  }

  it should "not flag hot-node when failures are spread across 3 nodes (~33% each)" in {
    // 3 failures per node, 9 total — max = 33% < 50% threshold
    val tasks = (0L until 3L).map(i => failedTask(i, "node-a")) ++
                (3L until 6L).map(i => failedTask(i, "node-b")) ++
                (6L until 9L).map(i => failedTask(i, "node-c"))
    val s = stage(stageId = 0, tasks = tasks).copy(numTasks = 9)
    YarnAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id == "yarn-hot-node-failure") shouldBe empty
  }

  it should "not flag hot-node when total failures are below minimum (5)" in {
    val tasks = (0L until 3L).map(i => failedTask(i, "bad-node"))
    val s = stage(stageId = 0, tasks = tasks).copy(numTasks = 3)
    YarnAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id == "yarn-hot-node-failure") shouldBe empty
  }

  // ── Check 4: Shuffle disk full ────────────────────────────────────────────

  it should "flag Warning when tasks fail with disk-full errors" in {
    val diskFullTask = TaskData(
      taskId = 0, index = 0, attempt = 0, executorId = "0", host = "node-01",
      status = "FAILED", launchTimeMs = 0L, finishTimeMs = 1000L,
      failed = true, killed = false, speculative = false,
      errorMessage = Some("java.io.IOException: No space left on device"),
      metrics = TaskMetrics(),
    )
    val s = stage(stageId = 0, tasks = Seq(diskFullTask)).copy(numTasks = 1)
    val issues = YarnAnalyzer.analyze(app(stages = Map(0 -> s), props = yarnProps))
    val i = issues.filter(_.id == "yarn-shuffle-disk-full")
    i should not be empty
    i.head.severity shouldBe Warning
    i.head.configFix.get should include("spark.local.dir")
  }

  it should "detect ENOSPC and DiskSpaceException patterns" in {
    val tasks = Seq(
      TaskData(taskId = 1, index = 1, attempt = 0, executorId = "0", host = "n1",
               status = "FAILED", launchTimeMs = 0L, finishTimeMs = 1000L,
               failed = true, killed = false, speculative = false,
               errorMessage = Some("DiskSpaceException: disk quota exceeded"),
               metrics = TaskMetrics()),
    )
    YarnAnalyzer.analyze(app(stages = Map(0 -> stage(stageId = 0, tasks = tasks))))
      .filter(_.id == "yarn-shuffle-disk-full") should not be empty
  }

  it should "not flag disk full for unrelated task failures" in {
    val tasks = Seq(failedTask(0, "node-01"))
    val s = stage(stageId = 0, tasks = tasks)
    YarnAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id == "yarn-shuffle-disk-full") shouldBe empty
  }

  // ── Check 5: PySpark overhead risk ───────────────────────────────────────

  it should "flag Warning when Python UDFs are present and overhead < 20% on YARN" in {
    val sqlWithUdf = sqlExec(id = 0L, plan = "BatchEvalPython", jobIds = Nil)
    val issues = YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlWithUdf),
      props    = yarnProps + ("spark.executor.memory" -> "4g"),
    ))
    val i = issues.filter(_.id == "yarn-pyspark-overhead-risk")
    i should not be empty
    i.head.severity shouldBe Warning
    i.head.configFix.get should include("memoryOverheadFactor=0.4")
  }

  it should "detect Python UDF via planTree node" in {
    val tree = planNode("BatchEvalPython", simpleString = "BatchEvalPython[...]")
    val sqlWithUdf = sqlExec(id = 0L, planTree = Some(tree))
    YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlWithUdf),
      props    = yarnProps,
    )).filter(_.id == "yarn-pyspark-overhead-risk") should not be empty
  }

  it should "not flag PySpark risk when overhead is explicitly set high enough" in {
    val sqlWithUdf = sqlExec(id = 0L, plan = "BatchEvalPython")
    YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlWithUdf),
      props    = yarnProps + ("spark.executor.memoryOverhead" -> "4g"),
    )).filter(_.id == "yarn-pyspark-overhead-risk") shouldBe empty
  }

  it should "not flag PySpark risk when overhead factor is already >= 0.2" in {
    val sqlWithUdf = sqlExec(id = 0L, plan = "BatchEvalPython")
    YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlWithUdf),
      props    = yarnProps + ("spark.executor.memoryOverheadFactor" -> "0.3"),
    )).filter(_.id == "yarn-pyspark-overhead-risk") shouldBe empty
  }

  it should "not flag PySpark risk when no Python UDFs are present" in {
    val sqlNoUdf = sqlExec(id = 0L, plan = "SortMergeJoin")
    YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlNoUdf),
      props    = yarnProps,
    )).filter(_.id == "yarn-pyspark-overhead-risk") shouldBe empty
  }

  it should "not flag PySpark risk when not running on YARN" in {
    val sqlWithUdf = sqlExec(id = 0L, plan = "BatchEvalPython")
    YarnAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlWithUdf),
      props    = Map.empty,   // no YARN markers
    )).filter(_.id == "yarn-pyspark-overhead-risk") shouldBe empty
  }
}
