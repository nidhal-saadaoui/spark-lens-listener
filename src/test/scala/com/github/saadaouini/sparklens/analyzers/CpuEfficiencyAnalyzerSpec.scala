package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Info
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CpuEfficiencyAnalyzerSpec extends AnyFlatSpec with Matchers {

  // CpuEfficiencyAnalyzer: Info when cpuFraction < 20%
  // Requires total executorRunTime >= 30000ms and cpuTimeNs > 0

  "CpuEfficiencyAnalyzer" should "return no issues for an empty app" in {
    CpuEfficiencyAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when total run time is below 30 seconds" in {
    // 5 tasks × 5s = 25s total < 30s minimum
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 5000L, cpuNs = 100000000L))
    CpuEfficiencyAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when cpu time is zero" in {
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, cpuNs = 0L))
    CpuEfficiencyAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when cpu fraction is at or above 20%" in {
    // cpu_fraction = (500ms_cpu / 1000ms_run) = 50%
    val tasks = (0 until 5).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 5000L * 1000000L)) // 5s cpu / 10s run = 50%
    CpuEfficiencyAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "flag Info when cpu fraction is below 20%" in {
    // cpuFraction = 1s_cpu / 10s_run = 10% < 20%
    val tasks = (0 until 5).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 1000L * 1000000L)) // 1s cpu / 10s run = 10%
    val issues = CpuEfficiencyAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Info
    issues.head.id shouldBe "cpu-0"
  }

  it should "report cpu_fraction and run_time_ms in metrics" in {
    val tasks = (0 until 5).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 1000L * 1000000L))
    val issues = CpuEfficiencyAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    val m = issues.head.metrics
    m("run_time_ms").toLong shouldBe 50000L  // 5 tasks × 10s
    m("cpu_time_ms").toLong shouldBe 5000L   // 5 tasks × 1s
    m("cpu_fraction").toDouble shouldBe (0.1 +- 0.01)
  }

  it should "produce one issue per affected stage" in {
    val tasks = (0 until 5).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 1000L * 1000000L))
    val stages = Map(
      0 -> stage(stageId = 0, tasks = tasks),
      1 -> stage(stageId = 1, tasks = tasks),
    )
    val issues = CpuEfficiencyAnalyzer.analyze(app(stages = stages))
    issues should have size 2
    issues.map(_.id).toSet shouldBe Set("cpu-0", "cpu-1")
  }

  it should "flag the stage ID in affectedStages" in {
    val tasks = (0 until 5).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 1000L * 1000000L))
    val issues = CpuEfficiencyAnalyzer.analyze(app(stages = Map(4 -> stage(stageId = 4, tasks = tasks))))
    issues.head.affectedStages shouldBe Seq(4)
  }

  it should "not fire when cpu fraction is above a custom lowFraction" in {
    // cpuFraction ~10% — below default 20% but above custom 5%... not an issue at custom threshold 5%
    // Here we set threshold to 5% so 10% is above it → no issue
    val tasks = (0 until 6).map(i =>
      task(id = i, executorRunTimeMs = 10000L, cpuNs = 1000L * 1000000L))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.cpu.lowFraction" -> "0.05"))
    CpuEfficiencyAnalyzer.analyze(a) shouldBe empty
  }
}
