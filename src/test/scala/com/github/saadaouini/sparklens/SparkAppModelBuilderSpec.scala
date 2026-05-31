package com.github.saadaouini.sparklens

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkAppModelBuilderSpec extends AnyFlatSpec with Matchers {

  "SparkAppModelBuilder.linkSqlJob" should "associate a job ID with a SQL execution" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.onSqlExecutionStart(42L, "SELECT 1", "== Physical Plan ==\nSort", 0L)
    b.linkSqlJob(42L, 7)
    val app = b.build(1000L)
    app.sqlExecutions(42L).jobIds shouldBe Seq(7)
  }

  it should "accumulate multiple job IDs for the same execution" in {
    val b = new SparkAppModelBuilder("3.5.0")
    b.onSqlExecutionStart(1L, "q", "", 0L)
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
    b.onSqlExecutionStart(1L, "q1", "", 0L)
    b.onSqlExecutionStart(2L, "q2", "", 0L)
    b.linkSqlJob(1L, 5)
    val app = b.build(1000L)
    app.sqlExecutions(1L).jobIds shouldBe Seq(5)
    app.sqlExecutions(2L).jobIds shouldBe Nil
  }
}
