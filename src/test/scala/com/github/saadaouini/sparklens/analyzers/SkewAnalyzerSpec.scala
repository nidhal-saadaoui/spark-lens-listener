package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SkewAnalyzerSpec extends AnyFlatSpec with Matchers {

  "SkewAnalyzer" should "return no issues when all tasks have equal duration" in {
    val tasks = (1 to 10).map(_ => task(durationMs = 1000L))
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when fewer than MinTasks" in {
    val tasks = Seq(task(1000L), task(10000L), task(1000L))
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when p50 is below the minimum duration threshold" in {
    // p50 = 100ms < MinP50Ms — stage is too fast to care about skew
    val tasks = (1 to 10).map(_ => task(durationMs = 100L)) :+ task(durationMs = 900L)
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect warning skew via concentration signal" in {
    // 1 task is 4× the rest — concentration of top 5% exceeds ConcWarn threshold
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 4000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "detect critical skew via concentration signal" in {
    // 1 task holds >50% of total stage time → concCrit fires
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 10000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
  }

  it should "detect critical skew via p95/p50 ratio" in {
    // Multiple slow tasks so p95 itself is skewed, not just the single max
    val tasks = (1 to 8).map(_ => task(durationMs = 1000L)) ++ Seq(task(10000L), task(10000L))
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
    issues.head.metrics("p95_ratio").toDouble should be >= 8.0
  }

  it should "detect skew via shuffle read bytes and classify as shuffle type" in {
    // Uniform task durations but one task reads 30× more shuffle data → hot-key partition
    val normal = (0 until 9).map(_ => task(durationMs = 1000L, remoteShuffleBytes = 1 * MB))
    val hot    = task(durationMs = 1200L, remoteShuffleBytes = 30 * MB)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = normal :+ hot))))
    issues should not be empty
    issues.head.metrics("skew_type") shouldBe "shuffle"
  }

  it should "classify skew as input type when tasks read mostly from files" in {
    val normal = (0 until 9).map(_ => task(durationMs = 1000L, inputBytes = 10 * MB))
    val large  = task(durationMs = 5000L, inputBytes = 300 * MB)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = normal :+ large))))
    issues should not be empty
    issues.head.metrics("skew_type") shouldBe "input"
  }

  it should "detect skew in multiple stages independently" in {
    val skewedTasks = (1 to 9).map(_ => task(1000L)) :+ task(10000L)
    val normalTasks = (1 to 10).map(_ => task(1000L))
    val stages = Map(
      0 -> stage(stageId = 0, tasks = skewedTasks),
      1 -> stage(stageId = 1, tasks = normalTasks),
    )
    val issues = SkewAnalyzer.analyze(app(stages = stages))
    issues should have size 1
    issues.head.affectedStages should contain(0)
  }

  it should "not fire when p95 ratio is below a custom warnP95Ratio" in {
    // 100 tasks: 95 at 1000ms, 5 at 4000ms
    // p95/p50 ratio ~4× — above default 3.0 but below custom 5.0
    // concentration: top5% (5 tasks at 4000ms = 20000ms) / total (115000ms) = 17.4% < ConcWarn 25%
    val base  = (0 until 95).map(i => task(id = i, durationMs = 1000L))
    val strag = (95 until 100).map(i => task(id = i, durationMs = 4000L))
    val tasks = base ++ strag
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.skew.warnP95Ratio" -> "5.0"))
    SkewAnalyzer.analyze(a) shouldBe empty
  }

  it should "respect a custom minTasks threshold" in {
    // Only 5 tasks — below default minTasks=10 but above custom minTasks=3
    val skewed = Seq(task(1000L), task(1000L), task(1000L), task(1000L), task(20000L))
    val a = app(stages = Map(0 -> stage(tasks = skewed)), props = Map("spark.sparklens.skew.minTasks" -> "3"))
    SkewAnalyzer.analyze(a) should not be empty
  }
}
