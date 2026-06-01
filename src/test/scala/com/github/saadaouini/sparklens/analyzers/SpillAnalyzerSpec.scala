package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpillAnalyzerSpec extends AnyFlatSpec with Matchers {

  "SpillAnalyzer" should "return no issues when no spill" in {
    val tasks = (1 to 5).map(_ => task())
    SpillAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when disk spill is below 100 MB" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 10L * MB))
    SpillAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect warning when disk spill is between 100 MB and 1 GB" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 30L * MB))  // 5 * 30 MB = 150 MB
    val issues = SpillAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "detect critical when disk spill exceeds 1 GB" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 300L * MB))  // 5 * 300 MB = 1.5 GB
    val issues = SpillAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
  }

  it should "report both disk and memory spill in metrics" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 30L * MB, memSpill = 10L * MB))
    val issues = SpillAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues.head.metrics("disk_bytes").toLong should be > 0L
    issues.head.metrics("memory_bytes").toLong should be > 0L
  }

  it should "fire when disk spill exceeds a custom warnDiskMb threshold" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 10L * MB))  // 50 MB total, above custom 20 MB
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.spill.warnDiskMb" -> "20"))
    SpillAnalyzer.analyze(a) should not be empty
  }

  it should "not fire when disk spill is below a custom warnDiskMb threshold" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 30L * MB))  // 150 MB total, below custom 200 MB
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.spill.warnDiskMb" -> "200"))
    SpillAnalyzer.analyze(a) shouldBe empty
  }

  it should "attach estimatedImpact with savedBytes and savedTimeMs" in {
    val tasks = (1 to 5).map(_ => task(diskSpill = 500L * MB))
    val s     = stage(stageId = 0, tasks = tasks).copy(hasExactAggregates = true, exactDiskSpillBytes = 500L * MB)
    val issues = SpillAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues should not be empty
    val imp = issues.head.estimatedImpact
    imp shouldBe defined
    imp.get.savedBytes.exists(_ > 0) shouldBe true
    imp.get.savedTimeMs.exists(_ > 0) shouldBe true
    imp.get.confidence shouldBe "medium"
  }
}
