package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinAnalyzerSpec extends AnyFlatSpec with Matchers {

  private val SMJ_PLAN = "SortMergeJoin [key#1], [key#2]"
  private val BHJ_PLAN = "BroadcastHashJoin\n+- BroadcastExchange"
  private val MB       = 1024L * 1024L
  private val GB       = 1024L * MB

  "JoinAnalyzer" should "return no issues for an empty app" in {
    JoinAnalyzer.analyze(app()) shouldBe empty
  }

  it should "not flag SortMergeJoin when broadcast threshold is positive" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = SMJ_PLAN)),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> (10 * MB).toString),
    ))
    issues.exists(_.id.startsWith("join-broadcast-disabled")) shouldBe false
  }

  it should "flag SortMergeJoin as Info when broadcast threshold is -1" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = SMJ_PLAN)),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    ))
    val flagged = issues.filter(_.id.startsWith("join-broadcast-disabled"))
    flagged should have size 1
    flagged.head.severity shouldBe Info
  }

  it should "flag oversized broadcast threshold as Warning when >= 1 GB" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = BHJ_PLAN)),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> GB.toString),
    ))
    val flagged = issues.filter(_.id.startsWith("join-large-broadcast"))
    flagged should have size 1
    flagged.head.severity shouldBe Warning
  }

  it should "not flag broadcast threshold below 1 GB" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = BHJ_PLAN)),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> (200 * MB).toString),
    ))
    issues.exists(_.id.startsWith("join-large-broadcast")) shouldBe false
  }

  it should "flag excessive shuffles when Exchange appears 4+ times" in {
    val plan = Seq.fill(4)("Exchange hashpartitioning(k#1, 200)").mkString("\n")
    val issues = JoinAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val flagged = issues.filter(_.id.startsWith("join-excessive-shuffle"))
    flagged should have size 1
    flagged.head.severity shouldBe Warning
  }

  it should "not flag excessive shuffles below threshold" in {
    val plan = Seq.fill(3)("Exchange hashpartitioning(k#1, 200)").mkString("\n")
    val issues = JoinAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("join-excessive-shuffle")) shouldBe false
  }

  it should "report exchange count in metrics" in {
    val plan = Seq.fill(5)("Exchange hashpartitioning(k#1, 200)").mkString("\n")
    val issues = JoinAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val flagged = issues.filter(_.id.startsWith("join-excessive-shuffle"))
    flagged.head.metrics("exchange_count") shouldBe "5"
  }

  it should "propagate affected job IDs from the SQL execution" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = SMJ_PLAN, jobIds = Seq(7, 8),
        id = 0L)),
      props = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    ))
    val flagged = issues.filter(_.id.startsWith("join-broadcast-disabled"))
    flagged.head.affectedJobs shouldBe Seq(7, 8)
  }
}
