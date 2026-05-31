package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Critical
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemoryPressureAnalyzerSpec extends AnyFlatSpec with Matchers {

  // MemoryPressureAnalyzer fires Critical when BOTH gcFraction >= 10% AND diskSpill >= 100MB
  // and total executor run time >= 10000ms

  "MemoryPressureAnalyzer" should "return no issues for an empty app" in {
    MemoryPressureAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when executor run time is below 10 seconds" in {
    val tasks = Seq(task(executorRunTimeMs = 1000L, gcMs = 200L, diskSpill = 500 * MB))
    MemoryPressureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when GC fraction is below 10% even with spill" in {
    // gcFraction = 500/10000 = 5% < 10%
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 500L, diskSpill = 200 * MB))
    MemoryPressureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when spill is below 100MB even with high GC" in {
    // gcFraction = 2000/10000 = 20%, but total spill = 5 × 10MB = 50MB < 100MB
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 2000L, diskSpill = 10 * MB))
    MemoryPressureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "flag Critical when both GC >= 10% and spill >= 100MB" in {
    // gcFraction = 1500/10000 = 15%, spill = 5 * 50MB = 250MB
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 1500L, diskSpill = 50 * MB))
    val issues = MemoryPressureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
    issues.head.id shouldBe "memory-pressure-0"
  }

  it should "populate gc_fraction and spill_bytes in metrics" in {
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 2000L, diskSpill = 50 * MB))
    val issues = MemoryPressureAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    val m = issues.head.metrics
    m("gc_fraction").toDouble shouldBe (0.2 +- 0.01)
    m("spill_bytes").toLong shouldBe (5 * 50 * MB)
  }

  it should "produce one issue per affected stage" in {
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 2000L, diskSpill = 50 * MB))
    val stages = Map(
      0 -> stage(stageId = 0, tasks = tasks),
      1 -> stage(stageId = 1, tasks = tasks),
    )
    val issues = MemoryPressureAnalyzer.analyze(app(stages = stages))
    issues should have size 2
    issues.map(_.id).toSet shouldBe Set("memory-pressure-0", "memory-pressure-1")
  }

  it should "include the stage in affectedStages" in {
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 2000L, diskSpill = 50 * MB))
    val issues = MemoryPressureAnalyzer.analyze(app(stages = Map(2 -> stage(stageId = 2, tasks = tasks))))
    issues.head.affectedStages shouldBe Seq(2)
  }

  it should "not fire when spill is below a custom spillMb threshold" in {
    // 5 tasks × 30MB = 150MB total spill + 20% GC — fires at default 100MB threshold
    // with custom 200MB threshold → 150MB < 200MB → no issue
    val tasks = (0 until 5).map(i => task(id = i, executorRunTimeMs = 10000L, gcMs = 2000L, diskSpill = 30 * MB))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.memoryPressure.spillMb" -> "200"))
    MemoryPressureAnalyzer.analyze(a) shouldBe empty
  }
}
