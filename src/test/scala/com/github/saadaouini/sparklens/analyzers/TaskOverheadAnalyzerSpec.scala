package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TaskOverheadAnalyzerSpec extends AnyFlatSpec with Matchers {

  "TaskOverheadAnalyzer" should "return no issues for an empty app" in {
    TaskOverheadAnalyzer.analyze(app()) shouldBe empty
  }

  it should "flag a stage where deserialize time exceeds 30% of executor run time" in {
    // 100 tasks: 1000 ms run, 400 ms deserialize → 40% > 30% threshold, stage > 5 s
    val tasks = (0 until 100).map(i =>
      task(id = i, executorRunTimeMs = 1000L, executorDeserializeTimeMs = 400L))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
    val issues = TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
    val overhead = issues.filter(_.id.startsWith("task-overhead"))
    overhead should not be empty
    overhead.head.severity shouldBe Warning
    overhead.head.metrics("task_count").toInt shouldBe 100
  }

  it should "not flag a stage below the deserialize ratio threshold" in {
    val tasks = (0 until 100).map(i =>
      task(id = i, executorRunTimeMs = 1000L, executorDeserializeTimeMs = 100L))  // 10% < 30%
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
    TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id.startsWith("task-overhead")) shouldBe empty
  }

  it should "not flag a stage shorter than minStageSec (5 s default)" in {
    // High ratio but stage total executor time = 100 tasks × 200ms = 20s in sample,
    // but stage wall-clock is only 3s → minDuration check uses totalExecutorRunTimeMs
    val tasks = (0 until 10).map(i =>
      task(id = i, executorRunTimeMs = 200L, executorDeserializeTimeMs = 100L))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(3000L))
    // totalExecutorRunTimeMs from tasks = 10 × 200 = 2000ms < 5000ms threshold
    TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
      .filter(_.id.startsWith("task-overhead")) shouldBe empty
  }

  it should "use exact aggregates when hasExactAggregates is true" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(30000L))
      .copy(
        hasExactAggregates             = true,
        exactTaskCount                 = 200,
        exactExecutorRunTimeMs         = 200 * 1000L,
        exactExecutorDeserializeTimeMs = 200 * 500L,  // 50% ratio
      )
    val issues = TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.filter(_.id.startsWith("task-overhead")) should not be empty
    issues.head.metrics("task_count").toInt shouldBe 200
  }

  it should "respect a custom deserializeRatioWarn threshold" in {
    val tasks = (0 until 100).map(i =>
      task(id = i, executorRunTimeMs = 1000L, executorDeserializeTimeMs = 350L))  // 35%
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
    val a = app(
      stages = Map(0 -> s),
      props  = Map("spark.sparklens.overhead.deserializeRatioWarn" -> "0.5"),
    )
    TaskOverheadAnalyzer.analyze(a).filter(_.id.startsWith("task-overhead")) shouldBe empty
  }

  it should "include deserialize_ratio in metrics" in {
    val tasks = (0 until 100).map(i =>
      task(id = i, executorRunTimeMs = 1000L, executorDeserializeTimeMs = 400L))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
    val issues = TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
    val m = issues.filter(_.id.startsWith("task-overhead")).head.metrics
    m("deserialize_ratio").toDouble shouldBe (0.4 +- 0.01)
  }

  it should "include estimatedImpact with the deserialize time as savedTimeMs" in {
    val tasks = (0 until 100).map(i =>
      task(id = i, executorRunTimeMs = 1000L, executorDeserializeTimeMs = 400L))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(30000L))
    val issues = TaskOverheadAnalyzer.analyze(app(stages = Map(0 -> s)))
    val imp = issues.filter(_.id.startsWith("task-overhead")).head.estimatedImpact
    imp shouldBe defined
    imp.get.savedTimeMs shouldBe Some(100 * 400L)  // 100 tasks × 400ms
    imp.get.confidence shouldBe "medium"
  }
}
