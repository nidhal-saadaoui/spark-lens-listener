package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, ExecutorData, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DynamicAllocationAnalyzerSpec extends AnyFlatSpec with Matchers {

  private val dynProps = Map("spark.dynamicAllocation.enabled" -> "true")

  // Executor helpers
  private def exec(id: String, addedMs: Long, removedMs: Option[Long] = None,
                   reason: Option[String] = None): (String, ExecutorData) =
    id -> ExecutorData(executorId = id, host = s"host-$id", totalCores = 4,
                       addedTimeMs = addedMs, removedTimeMs = removedMs,
                       removalReason = reason)

  // ── Guard: no-op when dynamic allocation is off ──────────────────────────

  "DynamicAllocationAnalyzer" should "produce no issues when dynamic allocation is disabled" in {
    DynamicAllocationAnalyzer.analyze(app()) shouldBe empty
  }

  // ── Check 1: no shuffle protection ───────────────────────────────────────

  it should "flag Critical when dynalloc is on and neither ESS nor shuffle tracking is enabled" in {
    val issues = DynamicAllocationAnalyzer.analyze(app(props = dynProps))
    val i = issues.filter(_.id == "dynalloc-no-shuffle-protection")
    i should not be empty
    i.head.severity shouldBe Critical
    i.head.configFix.get should include("shuffleTracking.enabled=true")
  }

  it should "not flag shuffle protection when shuffle tracking is enabled" in {
    val p = dynProps + ("spark.dynamicAllocation.shuffleTracking.enabled" -> "true")
    DynamicAllocationAnalyzer.analyze(app(props = p))
      .filter(_.id == "dynalloc-no-shuffle-protection") shouldBe empty
  }

  it should "not flag shuffle protection when ESS is enabled" in {
    val p = dynProps + ("spark.shuffle.service.enabled" -> "true")
    DynamicAllocationAnalyzer.analyze(app(props = p))
      .filter(_.id == "dynalloc-no-shuffle-protection") shouldBe empty
  }

  // ── Check 2: executor churn ───────────────────────────────────────────────

  it should "flag Warning when more than 25% of removed executors lived < 30s" in {
    // 3 short-lived (10s) out of 4 removed = 75% churn
    val execs = Map(
      exec("0", addedMs = 0L,     removedMs = Some(10000L),  reason = Some("idle")),
      exec("1", addedMs = 0L,     removedMs = Some(10000L),  reason = Some("idle")),
      exec("2", addedMs = 0L,     removedMs = Some(10000L),  reason = Some("idle")),
      exec("3", addedMs = 0L,     removedMs = Some(120000L), reason = Some("idle")),
    )
    val issues = DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
    val i = issues.filter(_.id == "dynalloc-executor-churn")
    i should not be empty
    i.head.severity shouldBe Warning
    i.head.metrics("churned_executors") shouldBe "3"
  }

  it should "not flag churn when fewer than 25% of executors are short-lived" in {
    // 1 short-lived out of 5 removed = 20% < 25% threshold
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(10000L),  reason = Some("idle")),
      exec("1", addedMs = 0L, removedMs = Some(200000L), reason = Some("idle")),
      exec("2", addedMs = 0L, removedMs = Some(200000L), reason = Some("idle")),
      exec("3", addedMs = 0L, removedMs = Some(200000L), reason = Some("idle")),
      exec("4", addedMs = 0L, removedMs = Some(200000L), reason = Some("idle")),
    )
    DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
      .filter(_.id == "dynalloc-executor-churn") shouldBe empty
  }

  it should "not flag churn when fewer than minRemovedExecs executors were removed" in {
    // Only 2 removed — below default minRemovedExecs=3
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(5000L),  reason = Some("idle")),
      exec("1", addedMs = 0L, removedMs = Some(5000L),  reason = Some("idle")),
    )
    DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
      .filter(_.id == "dynalloc-executor-churn") shouldBe empty
  }

  // ── Check 3: YARN OOM kill ────────────────────────────────────────────────

  it should "flag Warning when executors are killed by YARN memory enforcer (exit 137)" in {
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L),
           reason = Some("Container exited with a non-zero exit code 137.")),
      exec("1", addedMs = 0L, removedMs = Some(60000L),
           reason = Some("Container exited with a non-zero exit code 137.")),
    )
    val issues = DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
    val i = issues.filter(_.id == "dynalloc-yarn-oom-kill")
    i should not be empty
    i.head.severity shouldBe Warning
    i.head.metrics("yarn_oom_killed") shouldBe "2"
    i.head.configFix.get should include("memoryOverheadFactor")
  }

  it should "flag Warning when YARN explicitly reports exceeding memory limits" in {
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L),
           reason = Some("Container killed by YARN for exceeding memory limits. 12 GB of 10 GB physical memory used.")),
    )
    val issues = DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
    issues.filter(_.id == "dynalloc-yarn-oom-kill") should not be empty
  }

  it should "not flag YARN OOM for general preemption reasons" in {
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L),
           reason = Some("Container preempted by YARN resource manager.")),
    )
    DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
      .filter(_.id == "dynalloc-yarn-oom-kill") shouldBe empty
  }

  it should "not flag YARN OOM when executors exit cleanly" in {
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L), reason = Some("Executor idle")),
    )
    DynamicAllocationAnalyzer.analyze(app(executors = execs, props = dynProps))
      .filter(_.id == "dynalloc-yarn-oom-kill") shouldBe empty
  }

  // ── Check 4: maxExecutors ceiling ────────────────────────────────────────

  it should "flag Info when peak executor count hits the maxExecutors ceiling" in {
    val p = dynProps + ("spark.dynamicAllocation.maxExecutors" -> "3")
    // 3 executors added at the same time → peak = 3 = ceiling
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L)),
      exec("1", addedMs = 0L, removedMs = Some(60000L)),
      exec("2", addedMs = 0L, removedMs = Some(60000L)),
    )
    val issues = DynamicAllocationAnalyzer.analyze(app(executors = execs, props = p))
    val i = issues.filter(_.id == "dynalloc-maxexecutors-ceiling")
    i should not be empty
    i.head.metrics("peak_concurrent_executors") shouldBe "3"
    i.head.metrics("configured_ceiling")        shouldBe "3"
  }

  it should "not flag ceiling when peak is below maxExecutors" in {
    val p = dynProps + ("spark.dynamicAllocation.maxExecutors" -> "10")
    val execs = Map(
      exec("0", addedMs = 0L, removedMs = Some(60000L)),
      exec("1", addedMs = 0L, removedMs = Some(60000L)),
    )
    DynamicAllocationAnalyzer.analyze(app(executors = execs, props = p))
      .filter(_.id == "dynalloc-maxexecutors-ceiling") shouldBe empty
  }

  it should "not flag ceiling when maxExecutors is not configured" in {
    // No maxExecutors property → property absent → no check
    DynamicAllocationAnalyzer.analyze(app(props = dynProps))
      .filter(_.id == "dynalloc-maxexecutors-ceiling") shouldBe empty
  }

  // ── Check 5: scale-up lag after idle gap ─────────────────────────────────

  it should "flag Info when high scheduler delay follows an idle gap between jobs" in {
    // Job 0 completes at t=10s; job 1 starts at t=80s → 70s gap (> default 60s)
    val j0 = job(jobId = 0, stageIds = Seq(0), submissionTimeMs = 0L)
      .copy(completionTimeMs = Some(10000L))
    val j1 = job(jobId = 1, stageIds = Seq(1), submissionTimeMs = 80000L)
      .copy(completionTimeMs = Some(120000L))

    // Stage 1 tasks all have 8s launch delay from stage submission
    val delayedTasks = (0 until 10).map(i =>
      AnalyzerFixtures.task(id = i.toLong, durationMs = 5000L).copy(
        launchTimeMs = 80000L + 8000L,   // 8s after stage submit
        finishTimeMs = 80000L + 8000L + 5000L,
      ))
    val s0 = stage(stageId = 0, submitMs = Some(0L),     completeMs = Some(10000L))
    val s1 = stage(stageId = 1, tasks = delayedTasks,
                   submitMs = Some(80000L), completeMs = Some(120000L))

    val a = app(
      stages    = Map(0 -> s0, 1 -> s1),
      jobs      = Map(0 -> j0, 1 -> j1),
      props     = dynProps,
    )
    val issues = DynamicAllocationAnalyzer.analyze(a)
    val i = issues.filter(_.id == "dynalloc-scaleup-lag-1")
    i should not be empty
    i.head.metrics("idle_gap_ms").toLong shouldBe 70000L
    i.head.configFix.get should include("minExecutors")
  }

  it should "not flag scale-up lag when the idle gap is below the threshold" in {
    // Gap = 30s < default 60s
    val j0 = job(jobId = 0, stageIds = Seq(0), submissionTimeMs = 0L)
      .copy(completionTimeMs = Some(10000L))
    val j1 = job(jobId = 1, stageIds = Seq(1), submissionTimeMs = 40000L)
      .copy(completionTimeMs = Some(80000L))
    val delayedTasks = (0 until 10).map(i =>
      AnalyzerFixtures.task(id = i.toLong, durationMs = 5000L).copy(
        launchTimeMs = 40000L + 8000L,
        finishTimeMs = 40000L + 8000L + 5000L,
      ))
    val s0 = stage(stageId = 0, submitMs = Some(0L),     completeMs = Some(10000L))
    val s1 = stage(stageId = 1, tasks = delayedTasks,
                   submitMs = Some(40000L), completeMs = Some(80000L))
    DynamicAllocationAnalyzer.analyze(
      app(stages = Map(0 -> s0, 1 -> s1), jobs = Map(0 -> j0, 1 -> j1), props = dynProps))
      .filter(_.id.startsWith("dynalloc-scaleup-lag")) shouldBe empty
  }

  it should "not flag scale-up lag when task launch delay is below threshold" in {
    // Big gap but tasks launch immediately (no scale-up lag)
    val j0 = job(jobId = 0, stageIds = Seq(0), submissionTimeMs = 0L)
      .copy(completionTimeMs = Some(10000L))
    val j1 = job(jobId = 1, stageIds = Seq(1), submissionTimeMs = 120000L)
      .copy(completionTimeMs = Some(160000L))
    val fastTasks = (0 until 10).map(i =>
      AnalyzerFixtures.task(id = i.toLong, durationMs = 5000L).copy(
        launchTimeMs = 120000L + 100L,   // only 100ms delay
        finishTimeMs = 120000L + 100L + 5000L,
      ))
    val s0 = stage(stageId = 0, submitMs = Some(0L),      completeMs = Some(10000L))
    val s1 = stage(stageId = 1, tasks = fastTasks,
                   submitMs = Some(120000L), completeMs = Some(160000L))
    DynamicAllocationAnalyzer.analyze(
      app(stages = Map(0 -> s0, 1 -> s1), jobs = Map(0 -> j0, 1 -> j1), props = dynProps))
      .filter(_.id.startsWith("dynalloc-scaleup-lag")) shouldBe empty
  }
}
