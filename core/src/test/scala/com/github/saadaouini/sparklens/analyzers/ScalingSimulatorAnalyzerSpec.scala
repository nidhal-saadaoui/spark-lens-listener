package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScalingSimulatorAnalyzerSpec extends AnyFlatSpec with Matchers {

  import AnalyzerFixtures._

  // Build a stage with exact aggregates — the simulator uses totalExecutorRunTimeMs / taskCount
  // to derive avg task duration, so both must be set via exact aggregate fields.
  private def timedStage(
    stageId:        Int,
    numTasks:       Int,
    taskDurationMs: Long,           // avg task duration (executor run time per task)
    submitMs:       Long = 0L,
    durationMs:     Long = 0L,      // wall-clock duration used for durationMs() on StageData
    parentIds:      Seq[Int] = Nil,
  ) = {
    val wallClock = if (durationMs > 0) durationMs
                    else math.ceil(numTasks.toDouble / 16).toLong * taskDurationMs  // 4 exec × 4 cores
    stage(
      stageId   = stageId,
      submitMs  = Some(submitMs),
      completeMs = Some(submitMs + wallClock),
    ).copy(
      numTasks               = numTasks,
      exactTaskCount         = numTasks,
      exactExecutorRunTimeMs = numTasks.toLong * taskDurationMs,
      hasExactAggregates     = true,
      parentIds              = parentIds,
    )
  }

  private def executors(n: Int) = (0 until n).map { i =>
    s"$i" -> executor(id = s"$i").copy(totalCores = 4, addedTimeMs = 0L)
  }.toMap

  // ── Basic simulation ───────────────────────────────────────────────────────

  "ScalingSimulatorAnalyzer" should "produce an issue for a well-formed multi-stage app" in {
    val s0 = timedStage(0, numTasks = 40, taskDurationMs = 1000L, submitMs = 0L, durationMs = 10000L)
    val s1 = timedStage(1, numTasks = 40, taskDurationMs = 1000L, submitMs = 10000L, durationMs = 10000L, parentIds = Seq(0))
    val s2 = timedStage(2, numTasks = 40, taskDurationMs = 1000L, submitMs = 20000L, durationMs = 10000L, parentIds = Seq(1))
    val a  = app(
      stages    = Map(0 -> s0, 1 -> s1, 2 -> s2),
      executors = executors(4),
    ).copy(endTimeMs = Some(30000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues should not be empty
    issues.head.category shouldBe "scaling"
    issues.head.id shouldBe "scaling-estimate-0"
  }

  it should "project lower runtime at 2× executors than at 1×" in {
    val s0 = timedStage(0, numTasks = 80, taskDurationMs = 1000L, submitMs = 0L, durationMs = 20000L)
    val s1 = timedStage(1, numTasks = 80, taskDurationMs = 1000L, submitMs = 20000L, durationMs = 20000L, parentIds = Seq(0))
    val s2 = timedStage(2, numTasks = 80, taskDurationMs = 1000L, submitMs = 40000L, durationMs = 20000L, parentIds = Seq(1))
    val a  = app(
      stages    = Map(0 -> s0, 1 -> s1, 2 -> s2),
      executors = executors(4),
    ).copy(endTimeMs = Some(60000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues should not be empty
    val metrics = issues.head.metrics
    // The 2× entry should show a negative pct change (faster than baseline)
    val twoXKey = metrics.keys.find(_.contains("2×"))
    twoXKey shouldBe defined
    metrics(twoXKey.get) should include ("-")
  }

  it should "include actual executor count in metrics" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(stages = Map(0->s0,1->s1,2->s2), executors = executors(6))
      .copy(endTimeMs = Some(30000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues.head.metrics.keys should contain ("actual  (6 exec)")
  }

  // ── Dynamic allocation ceiling detection ──────────────────────────────────

  it should "emit Warning severity when DA ceiling is hit" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(
      stages    = Map(0->s0, 1->s1, 2->s2),
      executors = executors(4),
      props     = Map(
        "spark.dynamicAllocation.enabled"      -> "true",
        "spark.dynamicAllocation.maxExecutors" -> "4",
      ),
    ).copy(endTimeMs = Some(30000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues should not be empty
    issues.head.severity shouldBe Warning
  }

  it should "include configFix with raised maxExecutors when DA ceiling is hit" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(
      stages    = Map(0->s0, 1->s1, 2->s2),
      executors = executors(4),
      props     = Map(
        "spark.dynamicAllocation.enabled"      -> "true",
        "spark.dynamicAllocation.maxExecutors" -> "4",
      ),
    ).copy(endTimeMs = Some(30000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues.head.configFix shouldBe defined
    issues.head.configFix.get should include ("spark.dynamicAllocation.maxExecutors=8")
  }

  it should "emit Info severity when DA is not enabled" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(stages = Map(0->s0,1->s1,2->s2), executors = executors(4))
      .copy(endTimeMs = Some(30000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues should not be empty
    issues.head.severity should not be Warning
  }

  // ── Insufficient data ─────────────────────────────────────────────────────

  it should "produce no issues when fewer than 3 stages have timing data" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val a  = app(stages = Map(0->s0, 1->s1), executors = executors(4))
      .copy(endTimeMs = Some(20000L))
    ScalingSimulatorAnalyzer.analyze(a) shouldBe empty
  }

  it should "produce no issues when app duration is below 10 seconds" in {
    val s0 = timedStage(0, 10, 100L, 0L, 1000L)
    val s1 = timedStage(1, 10, 100L, 1000L, 1000L, Seq(0))
    val s2 = timedStage(2, 10, 100L, 2000L, 1000L, Seq(1))
    val a  = app(stages = Map(0->s0, 1->s1, 2->s2), executors = executors(4))
      .copy(endTimeMs = Some(3000L))
    ScalingSimulatorAnalyzer.analyze(a) shouldBe empty
  }

  it should "produce no issues when there are no executors" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(stages = Map(0->s0,1->s1,2->s2), executors = Map.empty)
      .copy(endTimeMs = Some(30000L))
    ScalingSimulatorAnalyzer.analyze(a) shouldBe empty
  }

  // ── Scale projection sanity check ─────────────────────────────────────────

  it should "include estimatedImpact with savedTimeMs for a job that benefits from scaling" in {
    val s0 = timedStage(0, 80, 1000L, 0L, 20000L)
    val s1 = timedStage(1, 80, 1000L, 20000L, 20000L, Seq(0))
    val s2 = timedStage(2, 80, 1000L, 40000L, 20000L, Seq(1))
    val a  = app(stages = Map(0->s0,1->s1,2->s2), executors = executors(4))
      .copy(endTimeMs = Some(60000L))
    val issues = ScalingSimulatorAnalyzer.analyze(a)
    issues.head.estimatedImpact shouldBe defined
    issues.head.estimatedImpact.get.savedTimeMs shouldBe defined
    issues.head.estimatedImpact.get.savedTimeMs.get should be > 0L
  }

  it should "include both serial_floor_pct and model_confidence in metrics" in {
    val s0 = timedStage(0, 40, 1000L, 0L, 10000L)
    val s1 = timedStage(1, 40, 1000L, 10000L, 10000L, Seq(0))
    val s2 = timedStage(2, 40, 1000L, 20000L, 10000L, Seq(1))
    val a  = app(stages = Map(0->s0,1->s1,2->s2), executors = executors(4))
      .copy(endTimeMs = Some(30000L))
    val metrics = ScalingSimulatorAnalyzer.analyze(a).head.metrics
    metrics.keys should contain ("serial_floor_pct")
    metrics.keys should contain ("model_confidence")
  }
}
