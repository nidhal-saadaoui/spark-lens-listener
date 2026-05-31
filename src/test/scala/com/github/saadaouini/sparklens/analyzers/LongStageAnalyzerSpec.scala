package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.StageData
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LongStageAnalyzerSpec extends AnyFlatSpec with Matchers {

  private def stageWithDuration(id: Int, durationMs: Long): (Int, StageData) =
    id -> stage(stageId = id).copy(
      submissionTimeMs = Some(0L),
      completionTimeMs = Some(durationMs),
    )

  "LongStageAnalyzer" should "return no issues for an empty app" in {
    LongStageAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when a job has fewer than 3 stages" in {
    val stages = Map(stageWithDuration(0, 10000L), stageWithDuration(1, 100000L))
    val j = job(jobId = 0, stageIds = Seq(0, 1))
    LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j))) shouldBe empty
  }

  it should "return no issues when no stage is a clear outlier" in {
    // 3 stages all similar duration — ratio < 5
    val stages = Map(
      stageWithDuration(0, 30000L),
      stageWithDuration(1, 35000L),
      stageWithDuration(2, 40000L),
    )
    val j = job(jobId = 0, stageIds = Seq(0, 1, 2))
    LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j))) shouldBe empty
  }

  it should "flag a stage that is much longer than the median" in {
    // Stage 2: 300s vs median ~30s → ratio 10× > default 5×
    val stages = Map(
      stageWithDuration(0, 30000L),
      stageWithDuration(1, 32000L),
      stageWithDuration(2, 300000L),
    )
    val j = job(jobId = 0, stageIds = Seq(0, 1, 2))
    val issues = LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j)))
    issues should have size 1
    issues.head.affectedStages shouldBe Seq(2)
    issues.head.title should include("× job median")
  }

  it should "not flag a long stage that is below the minimum duration" in {
    // Stage 2 is 10× median but only 3s total — below default 30s minimum
    val stages = Map(
      stageWithDuration(0, 300L),
      stageWithDuration(1, 320L),
      stageWithDuration(2, 3000L),
    )
    val j = job(jobId = 0, stageIds = Seq(0, 1, 2))
    LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j))) shouldBe empty
  }

  it should "include duration and ratio in metrics" in {
    val stages = Map(
      stageWithDuration(0, 30000L),
      stageWithDuration(1, 32000L),
      stageWithDuration(2, 300000L),
    )
    val j = job(jobId = 0, stageIds = Seq(0, 1, 2))
    val issues = LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j)))
    issues.head.metrics("duration_ms").toLong shouldBe 300000L
    issues.head.metrics("median_ms").toLong   shouldBe 32000L  // median of [30000,32000,300000] = sorted(1)
  }

  it should "emit at most one issue per stage even if the stage appears in multiple jobs" in {
    val stages = Map(
      stageWithDuration(0, 30000L),
      stageWithDuration(1, 32000L),
      stageWithDuration(2, 300000L),
    )
    val j0 = job(jobId = 0, stageIds = Seq(0, 1, 2))
    val j1 = job(jobId = 1, stageIds = Seq(0, 1, 2))
    val issues = LongStageAnalyzer.analyze(app(stages = stages, jobs = Map(0 -> j0, 1 -> j1)))
    issues.count(_.affectedStages.contains(2)) shouldBe 1
  }

  it should "not flag when ratio is below a custom outlierRatio" in {
    // 10× ratio — above default 5× but below custom 15×
    val stages = Map(
      stageWithDuration(0, 30000L),
      stageWithDuration(1, 32000L),
      stageWithDuration(2, 300000L),
    )
    val j = job(jobId = 0, stageIds = Seq(0, 1, 2))
    val a = app(stages = stages, jobs = Map(0 -> j),
      props = Map("spark.sparklens.longStage.outlierRatio" -> "15.0"))
    LongStageAnalyzer.analyze(a) shouldBe empty
  }
}
