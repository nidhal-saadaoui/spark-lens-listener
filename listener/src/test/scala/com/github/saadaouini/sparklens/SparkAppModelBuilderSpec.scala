package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.analyzers.AnalyzerFixtures._
import com.github.saadaouini.sparklens.model.StageData
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkAppModelBuilderSpec extends AnyFlatSpec with Matchers {

  // Minimal empty SparkPlanInfo for tests that don't exercise the plan tree.
  private val emptyPlanInfo = new SparkPlanInfo(
    "EmptyPlan", "EmptyPlan",
    Seq.empty[SparkPlanInfo],
    Map.empty[String, String],
    Seq.empty[SQLMetricInfo],
  )

  "SparkAppModelBuilder.linkSqlJob" should "associate a job ID with a SQL execution" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.onSqlExecutionStart(42L, "SELECT 1", "== Physical Plan ==\nSort", emptyPlanInfo, 0L)
    b.linkSqlJob(42L, 7)
    val app = b.build(1000L)
    app.sqlExecutions(42L).jobIds shouldBe Seq(7)
  }

  it should "accumulate multiple job IDs for the same execution" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.onSqlExecutionStart(1L, "q", "", emptyPlanInfo, 0L)
    b.linkSqlJob(1L, 10)
    b.linkSqlJob(1L, 11)
    b.linkSqlJob(1L, 12)
    val app = b.build(1000L)
    app.sqlExecutions(1L).jobIds shouldBe Seq(10, 11, 12)
  }

  it should "be a no-op for an unknown execution ID" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.linkSqlJob(999L, 1)
    val app = b.build(1000L)
    app.sqlExecutions shouldBe empty
  }

  it should "leave other executions unaffected" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.onSqlExecutionStart(1L, "q1", "", emptyPlanInfo, 0L)
    b.onSqlExecutionStart(2L, "q2", "", emptyPlanInfo, 0L)
    b.linkSqlJob(1L, 5)
    val app = b.build(1000L)
    app.sqlExecutions(1L).jobIds shouldBe Seq(5)
    app.sqlExecutions(2L).jobIds shouldBe Nil
  }

  // ── StageData exact-aggregate fallback ────────────────────────────────────

  "StageData" should "use tasks.sum when hasExactAggregates is false (unit-test fixture mode)" in {
    val t = task(inputBytes = 10 * MB, id = 0)
    val s = stage(tasks = Seq(t))
    s.hasExactAggregates shouldBe false
    s.totalInputBytes    shouldBe (10 * MB)
  }

  it should "use exactInputBytes when hasExactAggregates is true" in {
    val t = task(inputBytes = 10 * MB, id = 0)
    // exactInputBytes disagrees with what the task says — exact field wins
    val s = stage(tasks = Seq(t)).copy(
      hasExactAggregates = true,
      exactInputBytes    = 99 * MB,
    )
    s.totalInputBytes shouldBe (99 * MB)
  }

  it should "return exact GC, run-time, spill, shuffle, output, result from exact fields" in {
    val s = StageData(
      stageId  = 0, attemptId = 0, name = "s", numTasks = 1000,
      hasExactAggregates     = true,
      exactGcTimeMs          = 5000L,
      exactExecutorRunTimeMs = 60000L,
      exactDiskSpillBytes    = 200L * MB,
      exactMemorySpillBytes  = 50L * MB,
      exactShuffleRemoteBytes= 300L * MB,
      exactShuffleLocalBytes = 100L * MB,
      exactOutputBytes       = 128L * MB,
      exactResultSize        = 1024L,
    )
    s.totalGcTimeMs          shouldBe 5000L
    s.totalExecutorRunTimeMs shouldBe 60000L
    s.totalDiskSpillBytes    shouldBe (200L * MB)
    s.totalMemorySpillBytes  shouldBe (50L * MB)
    s.totalShuffleRemoteBytes shouldBe (300L * MB)
    s.totalShuffleLocalBytes  shouldBe (100L * MB)
    s.totalOutputBytes       shouldBe (128L * MB)
    s.totalResultSize        shouldBe 1024L
  }
}
