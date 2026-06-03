package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JobTimelineAnalyzerSpec extends AnyFlatSpec with Matchers {

  "JobTimelineAnalyzer" should "return no issues for an empty app" in {
    JobTimelineAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when jobs have no submission time (submissionTimeMs = 0)" in {
    val j0 = job(jobId = 0).copy(submissionTimeMs = 0L,      completionTimeMs = Some(1000L))
    val j1 = job(jobId = 1).copy(submissionTimeMs = 0L,      completionTimeMs = Some(2000L))
    JobTimelineAnalyzer.analyze(app(jobs = Map(0 -> j0, 1 -> j1))) shouldBe empty
  }

  it should "flag a gap greater than the default threshold (60 s)" in {
    val j0 = job(jobId = 0).copy(submissionTimeMs = 1L,      completionTimeMs = Some(1000L))
    val j1 = job(jobId = 1).copy(submissionTimeMs = 70000L,  completionTimeMs = Some(72000L))
    val issues = JobTimelineAnalyzer.analyze(app(jobs = Map(0 -> j0, 1 -> j1)))
    val gaps = issues.filter(_.id.startsWith("timeline-gap"))
    gaps should have size 1
    gaps.head.severity shouldBe Warning
    gaps.head.affectedJobs.sorted shouldBe Seq(0, 1)
  }

  it should "not flag a gap smaller than the threshold" in {
    val j0 = job(jobId = 0).copy(submissionTimeMs = 0L,     completionTimeMs = Some(1000L))
    val j1 = job(jobId = 1).copy(submissionTimeMs = 30000L, completionTimeMs = Some(32000L))
    JobTimelineAnalyzer.analyze(app(jobs = Map(0 -> j0, 1 -> j1)))
      .filter(_.id.startsWith("timeline-gap")) shouldBe empty
  }

  it should "respect a custom gapWarnMs threshold" in {
    val j0 = job(jobId = 0).copy(submissionTimeMs = 1L,     completionTimeMs = Some(1000L))
    val j1 = job(jobId = 1).copy(submissionTimeMs = 5000L,  completionTimeMs = Some(6000L))
    val a  = app(
      jobs  = Map(0 -> j0, 1 -> j1),
      props = Map("spark.sparklens.timeline.gapWarnMs" -> "3000"),
    )
    val gaps = JobTimelineAnalyzer.analyze(a).filter(_.id.startsWith("timeline-gap"))
    gaps should not be empty
  }

  it should "flag fragmentation when >70% of jobs are short and total jobs >= 50" in {
    // 55 jobs all under 2 s → 100% short > 70% threshold, 55 >= 50 minJobs
    val jobs = (0 until 55).map { i =>
      i -> job(jobId = i).copy(submissionTimeMs = i * 3000L, completionTimeMs = Some(i * 3000L + 500L))
    }.toMap
    val issues = JobTimelineAnalyzer.analyze(app(jobs = jobs))
    val frag = issues.filter(_.id == "timeline-fragmentation")
    frag should have size 1
    frag.head.severity shouldBe Info
    frag.head.metrics("small_jobs").toInt shouldBe 55
  }

  it should "not flag fragmentation when fewer than 50 total jobs" in {
    val jobs = (0 until 25).map { i =>
      i -> job(jobId = i).copy(submissionTimeMs = i * 3000L, completionTimeMs = Some(i * 3000L + 500L))
    }.toMap
    JobTimelineAnalyzer.analyze(app(jobs = jobs))
      .filter(_.id == "timeline-fragmentation") shouldBe empty
  }

  it should "not flag fragmentation when short jobs are below 70% fraction threshold" in {
    // 60 jobs: 40 short, 20 long → 40/60 ≈ 66.7% < 70%
    val short = (0 until 40).map(i => i -> job(jobId = i).copy(
      submissionTimeMs = i * 5000L, completionTimeMs = Some(i * 5000L + 500L))).toMap
    val long  = (40 until 60).map(i => i -> job(jobId = i).copy(
      submissionTimeMs = i * 5000L, completionTimeMs = Some(i * 5000L + 30000L))).toMap
    JobTimelineAnalyzer.analyze(app(jobs = short ++ long))
      .filter(_.id == "timeline-fragmentation") shouldBe empty
  }

  it should "include estimatedImpact with high confidence on gap issues" in {
    val j0 = job(jobId = 0).copy(submissionTimeMs = 1L,     completionTimeMs = Some(1000L))
    val j1 = job(jobId = 1).copy(submissionTimeMs = 90000L, completionTimeMs = Some(92000L))
    val issues = JobTimelineAnalyzer.analyze(app(jobs = Map(0 -> j0, 1 -> j1)))
    val gap = issues.filter(_.id.startsWith("timeline-gap")).head
    gap.estimatedImpact shouldBe defined
    gap.estimatedImpact.get.confidence shouldBe "high"
    gap.estimatedImpact.get.savedTimeMs shouldBe Some(89000L)
  }

  // ── Intra-job stage gap ────────────────────────────────────────────────────

  it should "flag a large driver idle gap between consecutive stages in the same job" in {
    // Stage 0 completes at t=10000, stage 1 submitted at t=30000 → 20s gap > 10s default
    val s0 = stage(stageId = 0, submitMs = Some(0L),   completeMs = Some(10000L))
    val s1 = stage(stageId = 1, submitMs = Some(30000L), completeMs = Some(35000L))
    val j0 = job(jobId = 0, stageIds = Seq(0, 1))
    val issues = JobTimelineAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0),
    ))
    val stageGap = issues.filter(_.id.startsWith("driver-stage-gap"))
    stageGap should not be empty
    stageGap.head.estimatedImpact.flatMap(_.savedTimeMs) shouldBe Some(20000L)
  }

  it should "not flag stage gaps below the threshold" in {
    // 5s gap with default 10s threshold — should not fire
    val s0 = stage(stageId = 0, submitMs = Some(0L),    completeMs = Some(10000L))
    val s1 = stage(stageId = 1, submitMs = Some(15000L), completeMs = Some(20000L))
    val j0 = job(jobId = 0, stageIds = Seq(0, 1))
    JobTimelineAnalyzer.analyze(app(stages = Map(0 -> s0, 1 -> s1), jobs = Map(0 -> j0)))
      .filter(_.id.startsWith("driver-stage-gap")) shouldBe empty
  }

  it should "not fire stage gap when stages have no timing data" in {
    val s0 = stage(stageId = 0).copy(submissionTimeMs = None, completionTimeMs = None)
    val s1 = stage(stageId = 1).copy(submissionTimeMs = None, completionTimeMs = None)
    val j0 = job(jobId = 0, stageIds = Seq(0, 1))
    JobTimelineAnalyzer.analyze(app(stages = Map(0 -> s0, 1 -> s1), jobs = Map(0 -> j0)))
      .filter(_.id.startsWith("driver-stage-gap")) shouldBe empty
  }

  it should "respect custom spark.sparklens.timeline.stageGapWarnMs threshold" in {
    // 8s gap — below default 10s but above custom 5s
    val s0 = stage(stageId = 0, submitMs = Some(0L),    completeMs = Some(10000L))
    val s1 = stage(stageId = 1, submitMs = Some(18000L), completeMs = Some(25000L))
    val j0 = job(jobId = 0, stageIds = Seq(0, 1))
    val issues = JobTimelineAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0),
      props  = Map("spark.sparklens.timeline.stageGapWarnMs" -> "5000"),
    ))
    issues.filter(_.id.startsWith("driver-stage-gap")) should not be empty
  }
}
