package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SmallFilesAnalyzerSpec extends AnyFlatSpec with Matchers {

  // SmallFilesAnalyzer: MinTasks=10, TargetBytesPerTask=128MB
  // Issues when avgPerTask < 64MB AND >50% of tasks below 32MB

  "SmallFilesAnalyzer" should "return no issues for an empty app" in {
    SmallFilesAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when fewer than 10 tasks have input bytes" in {
    val tasks = (0 until 9).map(i => task(id = i, inputBytes = 5 * MB))
    SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when average bytes per task is healthy" in {
    // 10 tasks × 128MB = well above 64MB avg threshold
    val tasks = (0 until 10).map(i => task(id = i, inputBytes = 128 * MB))
    SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "flag small files when avg < 64MB and majority of tasks are small" in {
    // 10 tasks × 10MB — avg=10MB (< 64MB), all below 32MB threshold → small ratio = 100%
    val tasks = (0 until 10).map(i => task(id = i, inputBytes = 10 * MB))
    val issues = SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "not flag when tasks have no input bytes (shuffle-only stage)" in {
    val tasks = (0 until 20).map(i => task(id = i, remoteShuffleBytes = 100 * MB))
    SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "report avg bytes and task count in metrics" in {
    val tasks = (0 until 10).map(i => task(id = i, inputBytes = 8 * MB))
    val issues = SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    val m = issues.head.metrics
    m("task_count") shouldBe "10"
    m("avg_bytes_per_task").toLong shouldBe (8 * MB)
  }

  it should "flag the stage ID in the issue ID" in {
    val tasks = (0 until 10).map(i => task(id = i, inputBytes = 5 * MB))
    val s = stage(stageId = 7, tasks = tasks)
    val issues = SmallFilesAnalyzer.analyze(app(stages = Map(7 -> s)))
    issues.head.id shouldBe "small-files-7"
    issues.head.affectedStages shouldBe Seq(7)
  }

  it should "not flag when small ratio is below 50%" in {
    // 10 tasks: 4 tiny (5MB) and 6 large (200MB) → ratio 40% < 50%
    val tasks = (0 until 4).map(i => task(id = i, inputBytes = 5 * MB)) ++
                (4 until 10).map(i => task(id = i, inputBytes = 200 * MB))
    SmallFilesAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }
}
