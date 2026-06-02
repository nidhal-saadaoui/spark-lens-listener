package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{TaskData, TaskMetrics, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchedulerDelayAnalyzerSpec extends AnyFlatSpec with Matchers {

  // Build a task with an explicit launchTimeMs (relative to stage submit = 0)
  private def taskWithLaunch(launchMs: Long, id: Long = 0L): TaskData =
    TaskData(
      taskId       = id,
      index        = id.toInt,
      attempt      = 0,
      executorId   = "0",
      host         = "host1",
      status       = "SUCCESS",
      launchTimeMs = launchMs,
      finishTimeMs = launchMs + 5000L,
      failed       = false,
      killed       = false,
      speculative  = false,
      errorMessage = None,
      metrics      = TaskMetrics(),
    )

  // Stage with tasks that all have a 3s launch delay
  private def delayedStage(id: Int, delayMs: Long, count: Int = 10) = {
    val tasks = (0 until count).map(i => taskWithLaunch(launchMs = delayMs, id = i.toLong))
    stage(stageId = id, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
  }

  "SchedulerDelayAnalyzer" should "flag stage when median launch delay exceeds 2s" in {
    val s = delayedStage(0, delayMs = 4000L)
    val issues = SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.exists(_.id == "scheduler-delay-0") shouldBe true
    issues.head.metrics("median_launch_delay_ms").toLong shouldBe 4000L
  }

  it should "not flag when median delay is below threshold" in {
    val s = delayedStage(0, delayMs = 500L)
    SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "scheduler-delay-0") shouldBe false
  }

  it should "not flag when task sample is too small (< 5)" in {
    val tasks = (0 until 3).map(i => taskWithLaunch(launchMs = 5000L, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(10000L))
    SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "scheduler-delay-0") shouldBe false
  }

  it should "emit Warning when median delay >= 5s" in {
    val s = delayedStage(0, delayMs = 6000L)
    val issues = SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.filter(_.id == "scheduler-delay-0").head.severity shouldBe Warning
  }

  it should "ignore failed and killed tasks" in {
    val successTasks = (0 until 5).map(i => taskWithLaunch(launchMs = 4000L, id = i.toLong))
    val failedTask   = TaskData(
      taskId = 99, index = 99, attempt = 0, executorId = "0", host = "host1",
      status = "FAILED", launchTimeMs = 50000L, finishTimeMs = 51000L,
      failed = true, killed = false, speculative = false, errorMessage = Some("OOM"),
      metrics = TaskMetrics(),
    )
    val s = stage(stageId = 0, tasks = successTasks :+ failedTask,
                  submitMs = Some(0L), completeMs = Some(30000L))
    val issues = SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
    // Should fire based on success tasks only (delay=4s), not the outlier failed task
    issues.exists(_.id == "scheduler-delay-0") shouldBe true
    issues.head.metrics("median_launch_delay_ms").toLong shouldBe 4000L
  }

  it should "include configFix for locality.wait" in {
    val s = delayedStage(0, delayMs = 4000L)
    val issues = SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.filter(_.id == "scheduler-delay-0").head.configFix shouldBe defined
    issues.head.configFix.get should include("spark.locality.wait")
  }

  // ── False-positive guards ─────────────────────────────────────────────────

  it should "produce no issues on an empty app" in {
    SchedulerDelayAnalyzer.analyze(app()) shouldBe empty
  }

  it should "not flag when tasks launch before stage submission (clock skew in fixtures)" in {
    // Tasks with launchTimeMs=0 on a stage submitted at 10s look like negative delay → skip
    val tasks = (0 until 10).map(i => taskWithLaunch(launchMs = 0L, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(10000L), completeMs = Some(30000L))
    SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "scheduler-delay-0") shouldBe false
  }

  it should "not flag a stage with no task sample" in {
    val s = stage(stageId = 0, tasks = Nil,
                  submitMs = Some(0L), completeMs = Some(30000L)).copy(
      hasExactAggregates = true,
      exactTaskCount     = 100,
    )
    SchedulerDelayAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "scheduler-delay-0") shouldBe false
  }
}
