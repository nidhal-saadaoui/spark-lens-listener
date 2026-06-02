package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Info
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IoClassifierAnalyzerSpec extends AnyFlatSpec with Matchers {

  "IoClassifierAnalyzer" should "return no issues for an empty app" in {
    IoClassifierAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues for a short stage" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(5000L))
             .copy(exactInputBytes = 500L * 1024L * 1024L, hasExactAggregates = true)
    IoClassifierAnalyzer.analyze(app(stages = Map(0 -> s))) shouldBe empty
  }

  it should "flag an I/O-bound stage when throughput exceeds the floor" in {
    // 1 executor with 1 core, 20 s stage, reading 200 MB → 10 MB/s >> 3 MB/s floor
    val exec = executor(id = "0").copy(totalCores = 1, addedTimeMs = 0L, removedTimeMs = None)
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(20000L))
             .copy(exactInputBytes = 200L * 1024L * 1024L, hasExactAggregates = true)
    val issues = IoClassifierAnalyzer.analyze(app(
      stages    = Map(0 -> s),
      executors = Map("0" -> exec),
    ))
    val ioBound = issues.filter(_.id.startsWith("io-bound"))
    ioBound should not be empty
    ioBound.head.severity shouldBe Info
    ioBound.head.metrics("cores") shouldBe "1"
  }

  it should "flag a compute-bound stage when throughput is below the ceiling" in {
    // 1 executor with 4 cores, 30 s stage, only 1 MB I/O → 0.008 MB/s/core << 1 MB/s ceiling
    val exec = executor(id = "0").copy(totalCores = 4, addedTimeMs = 0L, removedTimeMs = None)
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(30000L))
             .copy(exactInputBytes = 1L * 1024L * 1024L, hasExactAggregates = true)
    val issues = IoClassifierAnalyzer.analyze(app(
      stages    = Map(0 -> s),
      executors = Map("0" -> exec),
    ))
    val computeBound = issues.filter(_.id.startsWith("compute-bound"))
    computeBound should not be empty
    computeBound.head.severity shouldBe Info
  }

  it should "floor cores to 1 when no executors are present (local mode guard)" in {
    // No executors in model but stage has large I/O — should not crash
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(20000L))
             .copy(exactInputBytes = 200L * 1024L * 1024L, hasExactAggregates = true)
    val issues = IoClassifierAnalyzer.analyze(app(stages = Map(0 -> s)))
    // should produce an issue (cores floored to 1)
    issues should not be empty
  }

  it should "not flag when stage has trivially small I/O (below minIoBytes)" in {
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(30000L))
             .copy(exactInputBytes = 100L, hasExactAggregates = true)
    IoClassifierAnalyzer.analyze(app(stages = Map(0 -> s))) shouldBe empty
  }

  it should "respect custom thresholds" in {
    val exec = executor(id = "0").copy(totalCores = 1, addedTimeMs = 0L, removedTimeMs = None)
    val s = stage(stageId = 0, submitMs = Some(0L), completeMs = Some(20000L))
             .copy(exactInputBytes = 200L * 1024L * 1024L, hasExactAggregates = true)
    // Set ioFloorMbps to 100 so 10 MB/s/core is below the custom floor → not I/O bound
    val a = app(
      stages    = Map(0 -> s),
      executors = Map("0" -> exec),
      props     = Map("spark.sparklens.io.ioFloorMbps" -> "100"),
    )
    IoClassifierAnalyzer.analyze(a).filter(_.id.startsWith("io-bound")) shouldBe empty
  }
}
