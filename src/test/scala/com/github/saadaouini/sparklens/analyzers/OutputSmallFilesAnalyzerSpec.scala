package com.github.saadaouini.sparklens.analyzers

import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OutputSmallFilesAnalyzerSpec extends AnyFlatSpec with Matchers {

  "OutputSmallFilesAnalyzer" should "return no issues when no tasks write output" in {
    val tasks = (0 until 10).map(i => task(id = i))
    OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when fewer than 10 tasks write output" in {
    val tasks = (0 until 5).map(i => task(id = i, outputBytes = 1 * MB))
    OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when average output per task is healthy" in {
    // 10 tasks × 100 MB = avg 100 MB — above 64 MB threshold (128/2)
    val tasks = (0 until 10).map(i => task(id = i, outputBytes = 100 * MB))
    OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "flag small output files when average is below half the target" in {
    // 20 tasks × 5 MB = avg 5 MB — well below 64 MB threshold
    val tasks = (0 until 20).map(i => task(id = i, outputBytes = 5 * MB))
    val issues = OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.id shouldBe "output-small-files-0"
    issues.head.title should include("avg")
  }

  it should "include output metrics in the issue" in {
    val tasks = (0 until 20).map(i => task(id = i, outputBytes = 5 * MB))
    val issues = OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues.head.metrics("task_count") shouldBe "20"
    issues.head.metrics("total_output_bytes").toLong shouldBe 20 * 5 * MB
  }

  it should "include a coalesce code fix" in {
    val tasks = (0 until 20).map(i => task(id = i, outputBytes = 5 * MB))
    val issues = OutputSmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues.head.codeFix.get should include("coalesce")
  }

  it should "not flag when average is above a custom targetMb" in {
    // avg 30 MB — below default 64 MB threshold, but above custom 20 MB threshold
    val tasks = (0 until 10).map(i => task(id = i, outputBytes = 30 * MB))
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.outputSmallFiles.targetMb" -> "20"))
    OutputSmallFilesAnalyzer.analyze(a) shouldBe empty
  }
}
