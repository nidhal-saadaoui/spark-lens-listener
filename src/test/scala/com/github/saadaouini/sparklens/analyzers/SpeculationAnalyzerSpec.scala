package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpeculationAnalyzerSpec extends AnyFlatSpec with Matchers {

  "SpeculationAnalyzer" should "return no issues when speculation is disabled" in {
    val tasks = Seq(task(speculative = true))
    val s0 = stage(stageId = 0, tasks = tasks)
    SpeculationAnalyzer.analyze(app(stages = Map(0 -> s0))) shouldBe empty
  }

  it should "return no issues when spark.speculation property is absent" in {
    SpeculationAnalyzer.analyze(app()) shouldBe empty
  }

  it should "produce Info issue when speculation is enabled but no speculative tasks ran" in {
    val s0 = stage(stageId = 0, tasks = (0 until 5).map(i => task(id = i)))
    val issues = SpeculationAnalyzer.analyze(app(
      stages = Map(0 -> s0),
      props  = Map("spark.speculation" -> "true"),
    ))
    issues should have size 1
    issues.head.id shouldBe "speculation-configured-not-firing"
    issues.head.severity shouldBe Info
  }

  it should "produce Warning issue when speculative tasks ran" in {
    val tasks = Seq(task(speculative = false), task(speculative = true))
    val s0 = stage(stageId = 0, tasks = tasks)
    val issues = SpeculationAnalyzer.analyze(app(
      stages = Map(0 -> s0),
      props  = Map("spark.speculation" -> "true"),
    ))
    issues should have size 1
    issues.head.id shouldBe "speculation-active"
    issues.head.severity shouldBe Warning
  }

  it should "count speculative task total across all stages" in {
    val s0 = stage(stageId = 0, tasks = Seq(task(speculative = true), task(speculative = true)))
    val s1 = stage(stageId = 1, tasks = Seq(task(speculative = true)))
    val issues = SpeculationAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      props  = Map("spark.speculation" -> "true"),
    ))
    issues.head.metrics("speculative_tasks") shouldBe "3"
  }

  it should "produce Info issue when speculation enabled with no stages" in {
    val issues = SpeculationAnalyzer.analyze(app(props = Map("spark.speculation" -> "true")))
    issues should have size 1
    issues.head.severity shouldBe Info
  }

  it should "emit Critical when speculation fires on a write stage (duplicate-row risk)" in {
    val writeStage = stage(stageId = 0,
      tasks = Seq(task(speculative = true))).copy(details = "insertInto at HiveWriter.scala:42")
    val issues = SpeculationAnalyzer.analyze(app(
      stages = Map(0 -> writeStage),
      props  = Map("spark.speculation" -> "true"),
    ))
    val issue = issues.find(_.id == "speculation-active").get
    issue.severity shouldBe Critical
    issue.description should include("duplicate")
    issue.configFix.get should include("spark.speculation=false")
  }

  it should "emit Warning (not Critical) when speculation fires on a non-write stage" in {
    val computeStage = stage(stageId = 0,
      tasks = Seq(task(speculative = true))).copy(details = "count at App.scala:10")
    val issues = SpeculationAnalyzer.analyze(app(
      stages = Map(0 -> computeStage),
      props  = Map("spark.speculation" -> "true"),
    ))
    issues.find(_.id == "speculation-active").get.severity shouldBe Warning
  }
}
