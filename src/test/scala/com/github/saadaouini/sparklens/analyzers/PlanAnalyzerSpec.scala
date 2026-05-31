package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Info, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PlanAnalyzerSpec extends AnyFlatSpec with Matchers {

  "PlanAnalyzer" should "return no issues for an empty app" in {
    PlanAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues for an empty plan" in {
    PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = "")))) shouldBe empty
  }

  it should "flag CartesianProduct as Critical" in {
    val plan = "CartesianProduct\n:- LocalRelation\n+- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val cart = issues.filter(_.id.startsWith("plan-cartesian"))
    cart should have size 1
    cart.head.severity shouldBe Critical
  }

  it should "not flag plans without CartesianProduct" in {
    val plan = "SortMergeJoin [a#1], [b#2]\n:- LocalRelation\n+- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-cartesian")) shouldBe false
  }

  it should "flag Window without partitionBy as Warning" in {
    val plan = "Window [rank() windowspecdefinition(score#1 ASC)], [], [score#1 ASC]"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val win = issues.filter(_.id.startsWith("plan-window-nopart"))
    win should have size 1
    win.head.severity shouldBe Warning
  }

  it should "not flag Window that has partitionBy" in {
    val plan = "Window partitionBy [cat#1]\n+- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-window-nopart")) shouldBe false
  }

  it should "flag RoundRobinPartitioning as Info" in {
    val plan = "Exchange RoundRobinPartitioning(100)\n+- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val rr = issues.filter(_.id.startsWith("plan-roundrobin"))
    rr should have size 1
    rr.head.severity shouldBe Info
  }

  it should "not flag hash partitioning" in {
    val plan = "Exchange hashpartitioning(key#1, 200)\n+- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-roundrobin")) shouldBe false
  }

  it should "flag missing CBO stats in a SortMergeJoin plan" in {
    val plan =
      "SortMergeJoin [id#1], [id#2], Inner\n" +
      ":- Scan parquet orders Statistics(sizeInBytes=8.0 EiB)\n" +
      "+- Scan parquet products Statistics(sizeInBytes=8.0 EiB)"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val cbo = issues.filter(_.id.startsWith("plan-nocbo"))
    cbo should have size 1
    cbo.head.severity shouldBe Info
  }

  it should "not flag CBO when rowCount is present" in {
    val plan =
      "SortMergeJoin [id#1], [id#2], Inner\n" +
      ":- Scan parquet orders Statistics(sizeInBytes=512 MiB, rowCount=1000000)\n" +
      "+- Scan parquet products Statistics(sizeInBytes=10 MiB, rowCount=500)"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-nocbo")) shouldBe false
  }

  it should "produce issues for each SQL execution independently" in {
    val cartPlan = "CartesianProduct\n:- A\n+- B"
    val rrPlan   = "Exchange RoundRobinPartitioning(10)\n+- C"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(
      0L -> sqlExec(id = 0L, plan = cartPlan),
      1L -> sqlExec(id = 1L, plan = rrPlan),
    )))
    issues.exists(_.id.startsWith("plan-cartesian")) shouldBe true
    issues.exists(_.id.startsWith("plan-roundrobin")) shouldBe true
  }
}
