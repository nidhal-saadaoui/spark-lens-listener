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

  it should "not flag excessive shuffles below a custom excessiveShuffleCount" in {
    // 4 shuffles — fires at default 4 but not at custom 6
    val plan = "Exchange" * 4
    val a = app(
      sqlExecs = Map(0L -> sqlExec(plan = plan)),
      props = Map("spark.sparklens.join.excessiveShuffleCount" -> "6"),
    )
    JoinAnalyzer.analyze(a).exists(_.id.startsWith("join-excessive")) shouldBe false
  }

  // ── planTree-based detection ──────────────────────────────────────────────

  it should "not flag SortMergeJoin when planTree shows BroadcastHashJoin (AQE converted it)" in {
    // AQE can rewrite SMJ → BHJ at runtime.  If planTree has BHJ (the final plan)
    // we must NOT report a SortMergeJoin issue even though the text plan has SMJ.
    val tree = planNode("BroadcastHashJoin",
      children = Seq(planNode("BroadcastExchange"), planNode("LocalTableScan")))
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(
        plan     = SMJ_PLAN,   // initial text plan still says SMJ
        planTree = Some(tree), // but final tree says BHJ
      )),
      props = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    ))
    issues.exists(_.id.startsWith("join-broadcast-disabled")) shouldBe false
  }

  it should "flag SortMergeJoin when planTree confirms it was not converted" in {
    val tree = planNode("SortMergeJoin",
      children = Seq(planNode("Exchange"), planNode("Exchange")))
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(
        plan     = SMJ_PLAN,
        planTree = Some(tree),
      )),
      props = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    ))
    issues.exists(_.id.startsWith("join-broadcast-disabled")) shouldBe true
  }

  it should "count only non-broadcast Exchange nodes from planTree" in {
    // 3 ShuffleExchange + 1 BroadcastExchange → shuffle count = 3 (below default threshold 4)
    val tree = planNode("root", children = Seq(
      planNode("ShuffleExchange"),
      planNode("ShuffleExchange"),
      planNode("ShuffleExchange"),
      planNode("BroadcastExchange"),
    ))
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = "", planTree = Some(tree))),
    ))
    issues.exists(_.id.startsWith("join-excessive-shuffle")) shouldBe false
  }

  it should "flag excessive shuffles when planTree has 4+ non-broadcast exchanges" in {
    val tree = planNode("root", children = (1 to 4).map(_ => planNode("ShuffleExchange")))
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = "", planTree = Some(tree))),
    ))
    issues.filter(_.id.startsWith("join-excessive-shuffle")) should have size 1
  }

  it should "attach estimatedImpact to broadcast-disabled issue" in {
    // Build a stage with shuffle bytes so the impact estimate is non-zero
    val s  = stage(stageId = 0).copy(hasExactAggregates = true, exactShuffleRemoteBytes = 2L * GB)
    val j  = job(jobId = 0, stageIds = Seq(0))
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = SMJ_PLAN, jobIds = Seq(0))),
      stages   = Map(0 -> s),
      jobs     = Map(0 -> j),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> "-1"),
    ))
    val smjIssues = issues.filter(_.id.startsWith("join-broadcast-disabled"))
    smjIssues should not be empty
    val imp = smjIssues.head.estimatedImpact
    imp shouldBe defined
    imp.get.savedBytes.exists(_ > 0) shouldBe true
    imp.get.confidence shouldBe "medium"
  }

  it should "attach low-confidence estimatedImpact to config-risk issues" in {
    val issues = JoinAnalyzer.analyze(app(
      sqlExecs = Map(0L -> sqlExec(plan = BHJ_PLAN)),
      props    = Map("spark.sql.autoBroadcastJoinThreshold" -> GB.toString),
    ))
    val flagged = issues.filter(_.id.startsWith("join-large-broadcast"))
    flagged should not be empty
    flagged.head.estimatedImpact shouldBe defined
    flagged.head.estimatedImpact.get.confidence shouldBe "low"
  }
}
