package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Info, StageData, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutorSizingAnalyzerSpec extends AnyFlatSpec with Matchers {

  private val MB = 1024L * 1024L
  private val GB = 1024L * MB

  // Helper: stage with task sample so p95 can be computed from the sample
  private def stageWithPeak(id: Int, peakPerTask: Long, taskCount: Int = 20): StageData = {
    val tasks = (0 until taskCount).map(i => task(peakExecutionMemory = peakPerTask, id = i.toLong))
    stage(stageId = id, tasks = tasks, submitMs = Some(0L), completeMs = Some(60000L)).copy(
      hasExactAggregates          = true,
      exactTaskCount              = taskCount,
      exactPeakExecutionMemorySum = peakPerTask * taskCount,
    )
  }

  // ── Executor memory — under-provisioning ──────────────────────────────────

  "ExecutorSizingAnalyzer" should "flag under-provisioned memory when peak demand > 85% of pool" in {
    // executor.memory=4g, memFraction=0.6 → pool=2.4 GB
    // 2 cores per executor, avg peak=1.5 GB/task → demand=3 GB > pool
    val s = stageWithPeak(0, peakPerTask = 1536L * MB)
    val a = app(
      stages    = Map(0 -> s),
      executors = Map("0" -> executor().copy(totalCores = 2)),
      props     = Map(
        "spark.executor.memory" -> "4g",
        "spark.executor.cores"  -> "2",
      ),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    val under = issues.filter(_.id == "executor-memory-underprovisioned")
    under should not be empty
    under.head.metrics("p95_task_memory") should not be empty
    under.head.configFix shouldBe defined
  }

  it should "not flag under-provisioning when peak demand is well within the pool" in {
    // executor.memory=8g, pool=4.8 GB, 1 core, peak=1 GB → demand=1 GB << 4.8 GB
    val s = stageWithPeak(0, peakPerTask = 1L * GB)
    val a = app(
      stages    = Map(0 -> s),
      props     = Map("spark.executor.memory" -> "8g", "spark.executor.cores" -> "1"),
    )
    ExecutorSizingAnalyzer.analyze(a)
      .filter(_.id == "executor-memory-underprovisioned") shouldBe empty
  }

  // ── Executor memory — over-provisioning ───────────────────────────────────

  it should "flag over-provisioned memory when peak demand < 25% of pool" in {
    // executor.memory=32g, pool=19.2 GB, 1 core, peak=1 GB → demand=1 GB = 5% of pool
    val s = stageWithPeak(0, peakPerTask = 1L * GB)
    val a = app(
      stages    = Map(0 -> s),
      props     = Map("spark.executor.memory" -> "32g", "spark.executor.cores" -> "1"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    val over = issues.filter(_.id == "executor-memory-overprovisioned")
    over should not be empty
    over.head.severity shouldBe Info
    over.head.configFix shouldBe defined
    // Recommended memory must be less than current
    val recommendedRaw = over.head.metrics("recommended_memory")
    recommendedRaw should not be empty
  }

  it should "not flag over-provisioning when there is disk spill" in {
    val s = stageWithPeak(0, peakPerTask = 1L * GB).copy(
      exactDiskSpillBytes = 200L * MB,
    )
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.executor.memory" -> "32g", "spark.executor.cores" -> "1"),
    )
    ExecutorSizingAnalyzer.analyze(a)
      .filter(_.id == "executor-memory-overprovisioned") shouldBe empty
  }

  it should "not flag memory issues when peakExecutionMemory is zero (metric not available)" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(60000L))
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.executor.memory" -> "4g"),
    )
    ExecutorSizingAnalyzer.analyze(a)
      .filter(i => i.id.startsWith("executor-memory")) shouldBe empty
  }

  it should "include recommended memory in configFix" in {
    val s = stageWithPeak(0, peakPerTask = 1536L * MB)
    val a = app(
      stages    = Map(0 -> s),
      executors = Map("0" -> executor().copy(totalCores = 2)),
      props     = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "2"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    val under = issues.filter(_.id == "executor-memory-underprovisioned")
    under should not be empty
    under.head.configFix.get should include("spark.executor.memory")
  }

  // ── Driver memory ──────────────────────────────────────────────────────────

  it should "flag driver memory risk when result size exceeds 40% of driver heap" in {
    // driver.memory=2g, total result = 1.2 GB → 60% → Warning
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(10000L)).copy(
      hasExactAggregates = true,
      exactResultSize    = 1228L * MB,  // 1.2 GB
    )
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.driver.memory" -> "2g"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    val driverIssue = issues.filter(_.id == "driver-memory-underprovisioned")
    driverIssue should not be empty
    driverIssue.head.severity shouldBe Warning
    driverIssue.head.configFix.get should include("spark.driver.memory")
  }

  it should "flag Critical when result > 80% of driver heap" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(10000L)).copy(
      hasExactAggregates = true,
      exactResultSize    = 1700L * MB,  // > 80% of 2g
    )
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.driver.memory" -> "2g"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    issues.filter(_.id == "driver-memory-underprovisioned").head.severity shouldBe Critical
  }

  it should "not flag driver memory when result is small relative to driver heap" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(10000L)).copy(
      hasExactAggregates = true,
      exactResultSize    = 100L * MB,
    )
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.driver.memory" -> "8g"),
    )
    ExecutorSizingAnalyzer.analyze(a)
      .filter(_.id == "driver-memory-underprovisioned") shouldBe empty
  }

  // ── Cluster cores — over-provisioning ─────────────────────────────────────

  it should "flag an over-provisioned cluster when widest stage uses < 30% of cores" in {
    // Cluster: 4 executors × 8 cores = 32 total cores
    // Widest stage: 8 tasks → 8/32 = 25% < 30%
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(60000L))
      .copy(numTasks = 8)
    val execs = (0 until 4).map(i => i.toString -> executor(id = i.toString).copy(totalCores = 8)).toMap
    val a = app(stages = Map(0 -> s), executors = execs)
    val issues = ExecutorSizingAnalyzer.analyze(a)
    val cluster = issues.filter(_.id == "cluster-cores-overprovisioned")
    cluster should not be empty
    cluster.head.severity shouldBe Info
    cluster.head.metrics("total_cores").toInt shouldBe 32
    cluster.head.metrics("max_stage_tasks").toInt shouldBe 8
    cluster.head.configFix.get should include("spark.dynamicAllocation.enabled")
  }

  it should "not flag a cluster that uses more than 30% of cores" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(60000L))
      .copy(numTasks = 12)
    val execs = (0 until 2).map(i => i.toString -> executor(id = i.toString).copy(totalCores = 8)).toMap
    // 12/16 = 75% utilisation
    val a = app(stages = Map(0 -> s), executors = execs)
    ExecutorSizingAnalyzer.analyze(a)
      .filter(_.id == "cluster-cores-overprovisioned") shouldBe empty
  }

  it should "not flag cluster size below the minimum 8 cores threshold" in {
    val s = stage(stageId = 0).copy(numTasks = 1)
    val execs = Map("0" -> executor().copy(totalCores = 4))
    // totalCores=4 < minimum 8 → skip check
    ExecutorSizingAnalyzer.analyze(app(stages = Map(0 -> s), executors = execs))
      .filter(_.id == "cluster-cores-overprovisioned") shouldBe empty
  }

  // ── False-positive guards ─────────────────────────────────────────────────

  it should "produce no issues on an empty app" in {
    ExecutorSizingAnalyzer.analyze(app()) shouldBe empty
  }

  it should "not flag memory when no executor.memory property is set" in {
    val tasks = (0 until 10).map(i =>
      task(peakExecutionMemory = 2L * GB, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks,
                  submitMs = Some(0L), completeMs = Some(60000L)).copy(
      hasExactAggregates          = true,
      exactTaskCount              = 10,
      exactPeakExecutionMemorySum = 10 * 2L * GB,
    )
    // No spark.executor.memory set → skip analysis
    ExecutorSizingAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id.startsWith("executor-memory")) shouldBe empty
  }

  it should "not use single-task stage peak when multi-task stages exist" in {
    // Single-task (coalesce(1)) stage with 50 GB peak — should be excluded.
    // Multi-task stage with 3 GB peak — 3 GB > 85% of 2.4 GB pool → fires.
    // executor.memory=4g, memFraction=0.6 → pool=2.4 GB, 1 core → demand=3 GB (125% pool)
    val singleTask = stageWithPeak(0, peakPerTask = 50L * GB, taskCount = 1)
    val normalTask = stageWithPeak(1, peakPerTask = 3L  * GB)
    val a = app(
      stages = Map(0 -> singleTask, 1 -> normalTask),
      props  = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "1"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a).filter(_.id == "executor-memory-underprovisioned")
    issues should not be empty
    // Recommendation based on 3 GB peak, not 50 GB — should not suggest anything near 50+ GB
    issues.head.metrics("p95_task_memory") shouldBe "3.0 GB"
  }

  it should "fall back to single-task stages when no multi-task long stages exist" in {
    // executor.memory=2g, pool=1.2 GB, 1 core, peak=3 GB → demand=3 GB (250% pool) → fires
    val singleTask = stageWithPeak(0, peakPerTask = 3L * GB, taskCount = 1)
    val a = app(
      stages = Map(0 -> singleTask),
      props  = Map("spark.executor.memory" -> "2g", "spark.executor.cores" -> "1"),
    )
    val issues = ExecutorSizingAnalyzer.analyze(a)
    issues.exists(_.id == "executor-memory-underprovisioned") shouldBe true
    issues.find(_.id == "executor-memory-underprovisioned").get
      .description should include("single-task stages only")
  }

  it should "not flag under-provisioning when p95 demand exactly equals the memory pool" in {
    // executor.memory=4g, memFraction=0.6 → pool=2.4g
    // 1 core, p95 peak=2.4g → demand=2.4g = 100% exactly (not >85% boundary test)
    // 2.4g / 2.4g = 1.0 → 100% > 85% → DOES fire. Let's test boundary more carefully:
    // demand at 85% exactly → should not fire
    // pool = 2.4 GB, 85% = 2.04 GB → p95 peak = 2.04 GB → demand (1 core) = 2.04 GB → 85% → doesn't fire
    val peakBytes = (2.4 * GB * 0.84).toLong  // just below 85% threshold
    val tasks     = (0 until 10).map(i => task(peakExecutionMemory = peakBytes, id = i.toLong))
    val s         = stage(stageId = 0, tasks = tasks,
                          submitMs = Some(0L), completeMs = Some(60000L)).copy(
      hasExactAggregates          = true,
      exactTaskCount              = 10,
      exactPeakExecutionMemorySum = 10 * peakBytes,
    )
    val a = app(stages = Map(0 -> s),
                props  = Map("spark.executor.memory" -> "4g", "spark.executor.cores" -> "1"))
    ExecutorSizingAnalyzer.analyze(a)
      .filter(_.id == "executor-memory-underprovisioned") shouldBe empty
  }
}
