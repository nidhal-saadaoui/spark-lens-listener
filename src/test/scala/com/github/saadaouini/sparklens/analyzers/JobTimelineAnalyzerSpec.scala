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

  it should "flag fragmentation when >50% of jobs are short and total jobs >= 20" in {
    // 25 jobs all under 2 s
    val jobs = (0 until 25).map { i =>
      i -> job(jobId = i).copy(submissionTimeMs = i * 3000L, completionTimeMs = Some(i * 3000L + 500L))
    }.toMap
    val issues = JobTimelineAnalyzer.analyze(app(jobs = jobs))
    val frag = issues.filter(_.id == "timeline-fragmentation")
    frag should have size 1
    frag.head.severity shouldBe Info
    frag.head.metrics("small_jobs").toInt shouldBe 25
  }

  it should "not flag fragmentation when fewer than minJobs total jobs" in {
    val jobs = (0 until 5).map { i =>
      i -> job(jobId = i).copy(submissionTimeMs = i * 3000L, completionTimeMs = Some(i * 3000L + 500L))
    }.toMap
    JobTimelineAnalyzer.analyze(app(jobs = jobs))
      .filter(_.id == "timeline-fragmentation") shouldBe empty
  }

  it should "not flag fragmentation when short jobs are below the fraction threshold" in {
    // 21 jobs: 10 short, 11 long → 10/21 ≈ 47.6% < 50%
    val short = (0 until 10).map(i => i -> job(jobId = i).copy(
      submissionTimeMs = i * 5000L, completionTimeMs = Some(i * 5000L + 500L))).toMap
    val long  = (10 until 21).map(i => i -> job(jobId = i).copy(
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
}
