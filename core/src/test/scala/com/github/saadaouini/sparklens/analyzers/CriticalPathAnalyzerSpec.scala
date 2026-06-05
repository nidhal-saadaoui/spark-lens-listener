package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CriticalPathAnalyzerSpec extends AnyFlatSpec with Matchers {

  // Build a stage with given duration and parent IDs
  private def stageOf(id: Int, durationMs: Long, parents: Seq[Int] = Nil) =
    stage(stageId = id,
          submitMs = Some(0L), completeMs = Some(durationMs))
      .copy(parentIds = parents)

  "CriticalPathAnalyzer" should "flag a long sequential chain (3+ stages, >= 85% of wall time)" in {
    // s0(10s) → s1(20s) → s2(15s), appDuration = 45s, critical path = 45s = 100%
    val s0 = stageOf(0, 10000L)
    val s1 = stageOf(1, 20000L, Seq(0))
    val s2 = stageOf(2, 15000L, Seq(1))
    val a = app(
      stages = Map(0 -> s0, 1 -> s1, 2 -> s2),
    ).copy(endTimeMs = Some(45000L))
    val issues = CriticalPathAnalyzer.analyze(a)
    issues.exists(_.id.startsWith("critical-path-serial")) shouldBe true
    val issue = issues.head
    issue.affectedStages should contain allOf (0, 1, 2)
    issue.metrics("chain_length").toInt shouldBe 3
  }

  it should "identify the bottleneck stage" in {
    // s0(5s) → s1(40s) → s2(5s) — s1 is the bottleneck
    val s0 = stageOf(0, 5000L)
    val s1 = stageOf(1, 40000L, Seq(0))
    val s2 = stageOf(2, 5000L, Seq(1))
    val a = app(stages = Map(0 -> s0, 1 -> s1, 2 -> s2)).copy(endTimeMs = Some(50000L))
    val issues = CriticalPathAnalyzer.analyze(a)
    issues should not be empty
    issues.head.metrics("bottleneck_stage") shouldBe "1"
  }

  it should "not flag when fewer than 3 stages" in {
    val s0 = stageOf(0, 10000L)
    val s1 = stageOf(1, 10000L, Seq(0))
    CriticalPathAnalyzer.analyze(app(stages = Map(0 -> s0, 1 -> s1)))
      .exists(_.id.startsWith("critical-path-serial")) shouldBe false
  }

  it should "not flag when app duration is below 10s minimum" in {
    val s0 = stageOf(0, 1000L)
    val s1 = stageOf(1, 2000L, Seq(0))
    val s2 = stageOf(2, 2000L, Seq(1))
    CriticalPathAnalyzer.analyze(app(stages = Map(0 -> s0, 1 -> s1, 2 -> s2)))
      .exists(_.id.startsWith("critical-path-serial")) shouldBe false
  }

  it should "not flag when critical path fraction is below 85%" in {
    // s0(5s) → s1(10s) and s0 → s2(10s) → s3(5s), all parallel post-s0
    // critical path = 5+10+5=20s or 5+10=15s → pick s0→s2→s3=20s
    // appDuration = 20s → fraction = 100%... hmm that fires
    // Let's use: appDuration = 60s, critical = 20s → fraction = 33% → should not fire
    val s0 = stageOf(0, 5000L)
    val s1 = stageOf(1, 10000L, Seq(0))
    val s2 = stageOf(2, 10000L, Seq(0))
    val s3 = stageOf(3, 5000L,  Seq(1, 2))
    val a = app(stages = Map(0 -> s0, 1 -> s1, 2 -> s2, 3 -> s3))
      .copy(endTimeMs = Some(60000L))  // much longer app wall time
    val issues = CriticalPathAnalyzer.analyze(a)
    // critical path = 20s, app = 60s → 33% → below 85% threshold
    issues.exists(_.id.startsWith("critical-path-serial")) shouldBe false
  }

  it should "emit Warning when fraction exceeds 95%" in {
    val s0 = stageOf(0, 10000L)
    val s1 = stageOf(1, 30000L, Seq(0))
    val s2 = stageOf(2, 10000L, Seq(1))
    val a = app(stages = Map(0 -> s0, 1 -> s1, 2 -> s2)).copy(endTimeMs = Some(52000L))
    // critical = 50s, app = 52s → 96% → Warning
    val issues = CriticalPathAnalyzer.analyze(a)
    issues.filter(_.id.startsWith("critical-path-serial")).head.severity shouldBe Warning
  }

  it should "include critical_fraction metric" in {
    val s0 = stageOf(0, 10000L)
    val s1 = stageOf(1, 20000L, Seq(0))
    val s2 = stageOf(2, 15000L, Seq(1))
    val a = app(stages = Map(0 -> s0, 1 -> s1, 2 -> s2)).copy(endTimeMs = Some(45000L))
    val issues = CriticalPathAnalyzer.analyze(a)
    issues.head.metrics("critical_fraction").toDouble should be > 0.8
  }
}
