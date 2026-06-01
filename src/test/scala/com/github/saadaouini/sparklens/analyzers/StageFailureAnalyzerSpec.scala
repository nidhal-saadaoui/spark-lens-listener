package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StageFailureAnalyzerSpec extends AnyFlatSpec with Matchers {

  "StageFailureAnalyzer" should "return no issues for an empty app" in {
    StageFailureAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when all stages are first attempt with no failures" in {
    val tasks = (0 until 10).map(i => task(id = i))
    val s0 = stage(stageId = 0, attemptId = 0, tasks = tasks)
    StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0))) shouldBe empty
  }

  it should "flag a stage that was retried (attemptId > 0)" in {
    val s0 = stage(stageId = 0, attemptId = 1, failReason = Some("OOM on executor"))
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id == "stage-retry-0") shouldBe true
    issues.find(_.id == "stage-retry-0").get.severity shouldBe Warning
  }

  it should "include the failure reason in the retried stage description" in {
    val s0 = stage(stageId = 0, attemptId = 1, failReason = Some("OOM on executor"))
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.find(_.id == "stage-retry-0").get.description should include("OOM on executor")
  }

  it should "not flag a stage with attemptId == 0" in {
    val s0 = stage(stageId = 0, attemptId = 0)
    StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0))).exists(_.id == "stage-retry-0") shouldBe false
  }

  it should "flag high task failure rate at exactly 5% (5 of 100)" in {
    val tasks = (0 until 5).map(i => task(id = i, failed = true)) ++
                (5 until 100).map(i => task(id = i))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id.startsWith("task-failure")) shouldBe true
  }

  it should "not flag task failure rate below 5% (1 of 25 = 4%)" in {
    val tasks = Seq(task(id = 0, failed = true)) ++ (1 until 25).map(i => task(id = i))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.exists(_.id.startsWith("task-failure")) shouldBe false
  }

  it should "not flag task failures when fewer than 5 tasks" in {
    val tasks = (0 until 4).map(i => task(id = i, failed = true))
    val s0 = stage(stageId = 0, tasks = tasks)
    StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0))).exists(_.id.startsWith("task-failure")) shouldBe false
  }

  it should "include the sample error message in the description" in {
    val tasks = (0 until 10).map(i =>
      task(id = i, failed = i < 2, errorMsg = if (i < 2) Some("OutOfMemoryError GC overhead") else None))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.find(_.id.startsWith("task-failure")).get.description should include("OutOfMemoryError")
  }

  it should "populate failed_tasks and total_tasks in metrics" in {
    // 10 failed out of 100 total = 10% → above 5% threshold
    val tasks = (0 until 10).map(i => task(id = i, failed = true)) ++
                (10 until 100).map(i => task(id = i))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> s0)))
    val m = issues.find(_.id.startsWith("task-failure")).get.metrics
    m("failed_tasks") shouldBe "10"
    m("total_tasks")  shouldBe "100"
  }

  it should "flag each stage independently" in {
    val bad = (0 until 100).map(i => task(id = i, failed = i < 10))
    val stages = Map(
      0 -> stage(stageId = 0, tasks = bad),
      1 -> stage(stageId = 1, tasks = bad),
    )
    val issues = StageFailureAnalyzer.analyze(app(stages = stages))
    issues.exists(_.id == "task-failure-0") shouldBe true
    issues.exists(_.id == "task-failure-1") shouldBe true
  }

  it should "not fire when failure rate is below a custom failedTaskRateWarn" in {
    // 7% failure rate — above default 5% but below custom 20%
    val tasks = (0 until 100).map(i => task(id = i, failed = i < 7))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.stageFailure.failedTaskRateWarn" -> "0.20"))
    StageFailureAnalyzer.analyze(a).exists(_.id.startsWith("task-failure")) shouldBe false
  }

  it should "attach estimatedImpact to task-failure issue" in {
    val tasks = (0 until 20).map(i => task(id = i, failed = i < 2, executorRunTimeMs = 5000L))
    val issues = StageFailureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    val failIssues = issues.filter(_.id.startsWith("task-failure"))
    failIssues should not be empty
    val imp = failIssues.head.estimatedImpact
    imp shouldBe defined
    imp.get.savedTimeMs.exists(_ > 0) shouldBe true
    imp.get.confidence shouldBe "medium"
  }
}
