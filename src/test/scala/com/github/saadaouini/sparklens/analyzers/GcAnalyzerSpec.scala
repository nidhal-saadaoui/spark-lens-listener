package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GcAnalyzerSpec extends AnyFlatSpec with Matchers {

  "GcAnalyzer" should "return no issues when GC is below threshold" in {
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 500L))
    GcAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when total run time is too short" in {
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 1000L, gcMs = 500L))
    GcAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect warning when GC is 10-20% of run time" in {
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 1200L))
    val issues = GcAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "detect critical when GC exceeds 20% of run time" in {
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 2500L))
    val issues = GcAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
  }

  it should "include gc_fraction in metrics" in {
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 1500L))
    val issues = GcAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues.head.metrics("gc_fraction").toDouble should be > 0.10
  }

  it should "fire when GC fraction exceeds a custom warnFraction" in {
    // 5 tasks × 400ms GC / 5 tasks × 10000ms run = 4% GC — above custom 3%, below default 10%
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 400L))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.gc.warnFraction" -> "0.03"))
    GcAnalyzer.analyze(a) should not be empty
  }

  it should "not fire when GC fraction is below a custom warnFraction" in {
    // 12% GC — above default 10% but below custom 30%
    val tasks = (1 to 5).map(_ => task(executorRunTimeMs = 10000L, gcMs = 1200L))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.gc.warnFraction" -> "0.30"))
    GcAnalyzer.analyze(a) shouldBe empty
  }
}
