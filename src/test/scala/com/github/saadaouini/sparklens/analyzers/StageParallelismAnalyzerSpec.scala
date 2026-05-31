package com.github.saadaouini.sparklens.analyzers

import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StageParallelismAnalyzerSpec extends AnyFlatSpec with Matchers {

  private def appWith(numTasks: Int, cores: Int, durationMs: Long = 30000L,
                      props: Map[String, String] = Map.empty) = {
    val tasks = (0 until numTasks).map(i => task(id = i, durationMs = durationMs / numTasks.max(1)))
    val s = stage(stageId = 0, tasks = tasks).copy(numTasks = numTasks,
      submissionTimeMs = Some(0L), completionTimeMs = Some(durationMs))
    val exc = executor(id = "0", host = "h1").copy(totalCores = cores)
    app(stages = Map(0 -> s), executors = Map("0" -> exc), props = props)
  }

  "StageParallelismAnalyzer" should "return no issues when cluster is too small to evaluate" in {
    // 4 cores < default minCores=8
    val a = appWith(numTasks = 2, cores = 4)
    StageParallelismAnalyzer.analyze(a) shouldBe empty
  }

  it should "return no issues when stage has enough tasks" in {
    // 10 tasks on 16 cores = 62.5% — above 50% threshold
    val a = appWith(numTasks = 10, cores = 16)
    StageParallelismAnalyzer.analyze(a) shouldBe empty
  }

  it should "return no issues when stage duration is below minimum" in {
    // Only 1 second — below default 10s minimum
    val a = appWith(numTasks = 2, cores = 100, durationMs = 1000L)
    StageParallelismAnalyzer.analyze(a) shouldBe empty
  }

  it should "flag low parallelism when few tasks run on many cores" in {
    // 3 tasks on 100 cores = 3% utilization, 30s stage
    val a = appWith(numTasks = 3, cores = 100)
    val issues = StageParallelismAnalyzer.analyze(a)
    issues should have size 1
    issues.head.category shouldBe "io"
    issues.head.title should include("3 tasks on 100 cores")
  }

  it should "include cores and utilisation in metrics" in {
    val a = appWith(numTasks = 3, cores = 100)
    val issues = StageParallelismAnalyzer.analyze(a)
    issues.head.metrics("num_tasks")   shouldBe "3"
    issues.head.metrics("total_cores") shouldBe "100"
  }

  it should "sum cores across multiple executors" in {
    val tasks = (0 until 3).map(i => task(id = i, durationMs = 10000L))
    val s  = stage(stageId = 0, tasks = tasks).copy(numTasks = 3,
      submissionTimeMs = Some(0L), completionTimeMs = Some(30000L))
    val e1 = executor(id = "0").copy(totalCores = 50)
    val e2 = executor(id = "1", host = "h2").copy(totalCores = 50)
    val a  = app(stages = Map(0 -> s), executors = Map("0" -> e1, "1" -> e2))
    val issues = StageParallelismAnalyzer.analyze(a)
    issues should have size 1
    issues.head.metrics("total_cores") shouldBe "100"
  }

  it should "not flag when above a custom underutilizationRatio" in {
    // 10 tasks on 100 cores = 10%, above custom threshold of 5%
    val a = appWith(numTasks = 10, cores = 100,
      props = Map("spark.sparklens.stageParallelism.underutilizationRatio" -> "0.05"))
    StageParallelismAnalyzer.analyze(a) shouldBe empty
  }
}
