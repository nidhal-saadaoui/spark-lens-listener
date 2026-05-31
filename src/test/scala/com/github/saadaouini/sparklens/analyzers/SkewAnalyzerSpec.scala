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

  it should "return no issues when fewer than 5 tasks" in {
    val tasks = Seq(task(1000L), task(10000L), task(1000L))
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when median is below 1 second" in {
    val tasks = (1 to 10).map(_ => task(durationMs = 100L)) :+ task(durationMs = 900L)
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect warning skew at ratio >= 3x" in {
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 4000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "detect critical skew at ratio >= 8x" in {
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 10000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
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
}
