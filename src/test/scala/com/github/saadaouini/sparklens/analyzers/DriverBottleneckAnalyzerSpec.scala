package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DriverBottleneckAnalyzerSpec extends AnyFlatSpec with Matchers {

  "DriverBottleneckAnalyzer" should "return no issues for an empty app" in {
    DriverBottleneckAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when total result size is below 50MB" in {
    val tasks = (0 until 5).map(i => task(id = i, resultSize = 5 * MB))
    val s0 = stage(stageId = 0, tasks = tasks)
    DriverBottleneckAnalyzer.analyze(app(stages = Map(0 -> s0))) shouldBe empty
  }

  it should "flag a Warning when total result size is between 50MB and 500MB" in {
    val tasks = (0 until 5).map(i => task(id = i, resultSize = 20 * MB)) // 100MB total
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = DriverBottleneckAnalyzer.analyze(app(stages = Map(0 -> s0)))
    val flagged = issues.filter(_.id.startsWith("driver-result"))
    flagged should have size 1
    flagged.head.severity shouldBe Warning
  }

  it should "flag Critical when total result size >= 500MB" in {
    val tasks = (0 until 5).map(i => task(id = i, resultSize = 120 * MB)) // 600MB total
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = DriverBottleneckAnalyzer.analyze(app(stages = Map(0 -> s0)))
    val flagged = issues.filter(_.id.startsWith("driver-result"))
    flagged should have size 1
    flagged.head.severity shouldBe Critical
  }

  it should "include result bytes in metrics" in {
    val tasks = (0 until 5).map(i => task(id = i, resultSize = 20 * MB))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = DriverBottleneckAnalyzer.analyze(app(stages = Map(0 -> s0)))
    issues.find(_.id.startsWith("driver-result")).get.metrics("result_bytes").toLong shouldBe (100 * MB)
  }

  it should "flag CollectLimit SQL plan as Info" in {
    val plan = "CollectLimit 1000\n+- LocalRelation"
    val issues = DriverBottleneckAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val flagged = issues.filter(_.id.startsWith("driver-collect-limit"))
    flagged should have size 1
    flagged.head.severity shouldBe Info
  }

  it should "flag TakeOrderedAndProject SQL plan as Info" in {
    val plan = "TakeOrderedAndProject(limit=100, orderBy=[score DESC])"
    val issues = DriverBottleneckAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("driver-collect-limit")) shouldBe true
  }

  it should "not flag regular scan plans" in {
    val plan = "HashAggregate\n+- Scan parquet orders"
    val issues = DriverBottleneckAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("driver-collect-limit")) shouldBe false
  }

  it should "flag the stage ID in the issue ID and affectedStages" in {
    val tasks = (0 until 5).map(i => task(id = i, resultSize = 20 * MB))
    val s = stage(stageId = 3, tasks = tasks)
    val issues = DriverBottleneckAnalyzer.analyze(app(stages = Map(3 -> s)))
    issues.find(_.id.startsWith("driver-result")).get.affectedStages shouldBe Seq(3)
  }
}
