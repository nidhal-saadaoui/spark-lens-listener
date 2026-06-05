package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionImbalanceAnalyzerSpec extends AnyFlatSpec with Matchers {

  // 15 small + 5 large tasks so p50 = smallBytes and p95 = largeBytes (clear ratio)
  private def imbalancedStage(id: Int, smallBytes: Long, largeBytes: Long) = {
    val nSmall = 15
    val nLarge = 5
    val total  = nSmall + nLarge
    val smallTasks = (0 until nSmall).map(i =>
      task(inputBytes = smallBytes, id = i.toLong))
    val largeTasks = (nSmall until total).map(i =>
      task(inputBytes = largeBytes, id = i.toLong))
    stage(stageId = id, tasks = smallTasks ++ largeTasks,
          submitMs = Some(0L), completeMs = Some(60000L)).copy(
      hasExactAggregates = true,
      exactTaskCount     = total,
      exactInputBytes    = smallBytes * nSmall + largeBytes * nLarge,
    )
  }

  "PartitionImbalanceAnalyzer" should "flag stage with p95/p50 ratio > 3x" in {
    // p50 = 10 MB, p95 = 100 MB → ratio 10×
    val s = imbalancedStage(0, smallBytes = 10L * MB, largeBytes = 100L * MB)
    val a = app(stages = Map(0 -> s))
    val issues = PartitionImbalanceAnalyzer.analyze(a)
    issues.exists(_.id == "partition-imbalance-0") shouldBe true
    issues.head.metrics("p95_p50_ratio") should not be empty
  }

  it should "not flag when p95/p50 ratio is below threshold" in {
    // All tasks same size → ratio = 1.0
    val tasks = (0 until 20).map(i => task(inputBytes = 50L * MB, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks,
                  submitMs = Some(0L), completeMs = Some(30000L)).copy(
      hasExactAggregates = true,
      exactTaskCount     = 20,
      exactInputBytes    = 20 * 50L * MB,
    )
    PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "partition-imbalance-0") shouldBe false
  }

  it should "not flag when total input is below 100 MB floor" in {
    // 20 tasks × 1 MB = 20 MB total — below default floor
    val s = imbalancedStage(0, smallBytes = 1L * MB, largeBytes = 10L * MB)
    PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "partition-imbalance-0") shouldBe false
  }

  it should "not flag when task sample is too small (< 10)" in {
    val tasks = (0 until 5).map(i => task(inputBytes = 100L * MB, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks,
                  submitMs = Some(0L), completeMs = Some(10000L)).copy(
      hasExactAggregates = true,
      exactTaskCount     = 5,
      exactInputBytes    = 5 * 100L * MB,
    )
    PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id == "partition-imbalance-0") shouldBe false
  }

  it should "emit Warning when ratio >= 5x" in {
    val s = imbalancedStage(0, smallBytes = 10L * MB, largeBytes = 100L * MB)
    val issues = PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.filter(_.id == "partition-imbalance-0").head.severity shouldBe Warning
  }

  it should "include configFix for maxPartitionBytes" in {
    val s = imbalancedStage(0, smallBytes = 10L * MB, largeBytes = 100L * MB)
    val issues = PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
    issues.filter(_.id == "partition-imbalance-0").head.configFix shouldBe defined
  }

  it should "not flag stages with no input data" in {
    val tasks = (0 until 20).map(i => task(id = i.toLong))  // no inputBytes
    val s = stage(stageId = 0, tasks = tasks,
                  submitMs = Some(0L), completeMs = Some(30000L)).copy(
      hasExactAggregates = true,
      exactTaskCount     = 20,
    )
    PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id.startsWith("partition-imbalance")) shouldBe false
  }

  it should "produce no issues on an empty app" in {
    PartitionImbalanceAnalyzer.analyze(app()) shouldBe empty
  }

  it should "not flag shuffle-only stages (no inputBytesRead)" in {
    // A shuffle reduce stage has shuffle reads, not input reads
    val tasks = (0 until 20).map(i =>
      task(remoteShuffleBytes = 50L * MB, id = i.toLong))
    val s = stage(stageId = 0, tasks = tasks,
                  submitMs = Some(0L), completeMs = Some(30000L)).copy(
      hasExactAggregates    = true,
      exactTaskCount        = 20,
      exactShuffleRemoteBytes = 20 * 50L * MB,
    )
    PartitionImbalanceAnalyzer.analyze(app(stages = Map(0 -> s)))
      .exists(_.id.startsWith("partition-imbalance")) shouldBe false
  }
}
