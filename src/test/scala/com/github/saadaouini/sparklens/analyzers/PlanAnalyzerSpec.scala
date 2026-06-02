package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Info, PlanNode, Warning}
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
    // Spark physical plan for Window.orderBy("ts") without partitionBy includes
    // "Exchange SinglePartition" — all rows routed to one executor
    val plan = "Window [rank() windowspecdefinition(ts#1 ASC)]\n" +
               "+- Exchange SinglePartition, ENSURE_REQUIREMENTS\n" +
               "   +- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val win = issues.filter(_.id.startsWith("plan-window-nopart"))
    win should have size 1
    win.head.severity shouldBe Warning
  }

  it should "not flag Window that has partitionBy" in {
    // Spark physical plan for Window.partitionBy("cat").orderBy("ts") uses
    // hashpartitioning, not SinglePartition
    val plan = "Window [rank() windowspecdefinition(cat#1, ts#1 ASC)]\n" +
               "+- Exchange hashpartitioning(cat#1, 200)\n" +
               "   +- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-window-nopart")) shouldBe false
  }

  it should "not flag Window with partitionBy in FORMATTED plan" in {
    // Spark FORMATTED plan: exchange types are inline in the tree section.
    // A partitioned window's Exchange SinglePartition belongs to the OUTER aggregation and
    // appears BEFORE "Window" in the tree (it's an ancestor).  The window's own child exchange
    // uses hashpartitioning and appears AFTER "Window".  PlanAnalyzer must NOT fire.
    val plan =
      "* HashAggregate (4)\n" +
      "+- Exchange SinglePartition, ENSURE_REQUIREMENTS (3)\n" +
      "   +- * HashAggregate (2)\n" +
      "      +- Window (1)\n" +
      "         +- Exchange hashpartitioning(user_id#1, 5) (0)\n" +
      "\n\n" +
      "(0) Exchange\n" +
      "Arguments: hashpartitioning(user_id#1, 5)\n\n" +
      "(1) Window\n" +
      "Arguments: [rank() windowspecdefinition(user_id#1, ts#2 ASC)], [user_id#1], [ts#2 ASC]\n\n" +
      "(2) HashAggregate\n\n" +
      "(3) Exchange\n" +
      "Arguments: SinglePartition, ENSURE_REQUIREMENTS\n\n" +
      "(4) HashAggregate\n"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.exists(_.id.startsWith("plan-window-nopart")) shouldBe false
  }

  it should "flag Window without partitionBy in FORMATTED plan" in {
    // Without PARTITION BY the window's own Exchange SinglePartition is a child of Window
    // and appears AFTER "Window" in the tree section.  PlanAnalyzer must fire.
    val plan =
      "* HashAggregate (4)\n" +
      "+- * HashAggregate (3)\n" +
      "   +- Window (2)\n" +
      "      +- Exchange SinglePartition, ENSURE_REQUIREMENTS (1)\n" +
      "         +- * Range (0)\n" +
      "\n\n" +
      "(0) Range\n\n" +
      "(1) Exchange\n" +
      "Arguments: SinglePartition, ENSURE_REQUIREMENTS\n\n" +
      "(2) Window\n" +
      "Arguments: [rank() windowspecdefinition(ts#1 ASC)], [ts#1 ASC]\n\n" +
      "(3) HashAggregate\n\n" +
      "(4) HashAggregate\n"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val win = issues.filter(_.id.startsWith("plan-window-nopart"))
    win should have size 1
    win.head.severity shouldBe Warning
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

  // ── explode() detection ───────────────────────────────────────────────────

  it should "flag Generate explode() in plan text as Warning" in {
    val plan = "Project [id#0, elem#1]\n+- Generate explode(arr#2), [id#0], false, [elem#1]\n   +- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    val explode = issues.filter(_.id.startsWith("plan-explode"))
    explode should have size 1
    explode.head.severity shouldBe Warning
  }

  it should "flag Generate posexplode() in plan text" in {
    val plan = "Project [id#0, pos#1, elem#2]\n+- Generate posexplode(arr#3), [id#0], false, [pos#1, elem#2]\n   +- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.filter(_.id.startsWith("plan-explode")) should not be empty
  }

  it should "flag explode() via plan tree when text lacks the Generate node" in {
    val genNode  = planNode("Generate", children = Seq(planNode("LocalRelation")))
    val genNode2 = genNode.copy(simpleString = "generate explode(arr#1), [id#0]")
    val tree     = planNode("Project", children = Seq(genNode2))
    val exec     = sqlExec(id = 1L, plan = "Project\n+- LocalRelation", planTree = Some(tree))
    val issues   = PlanAnalyzer.analyze(app(sqlExecs = Map(1L -> exec)))
    issues.filter(_.id.startsWith("plan-explode")) should not be empty
  }

  it should "not flag a plan with Generate but no explode (e.g. json_tuple)" in {
    val plan = "Project [id#0]\n+- Generate json_tuple(json#1, 'a'), [id#0], false, [c0#2]\n   +- LocalRelation"
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues.filter(_.id.startsWith("plan-explode")) shouldBe empty
  }

  // ── Deep Project chain (withColumn loop) ─────────────────────────────────

  it should "flag a plan tree with 20+ Project nodes as Warning" in {
    // Build a chain of 25 Project nodes
    val leaf    = planNode("LocalRelation")
    val chain   = (0 until 25).foldLeft(leaf: PlanNode) { (child, _) => planNode("Project", children = Seq(child)) }
    val exec    = sqlExec(id = 0L, plan = "", planTree = Some(chain))
    val issues  = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> exec)))
    val chain25 = issues.filter(_.id.startsWith("plan-project-chain"))
    chain25 should not be empty
    chain25.head.severity shouldBe Warning
    chain25.head.metrics("project_count").toInt shouldBe 25
  }

  it should "not flag a plan tree with fewer than 20 Project nodes" in {
    val leaf  = planNode("LocalRelation")
    val chain = (0 until 5).foldLeft(leaf: PlanNode) { (child, _) => planNode("Project", children = Seq(child)) }
    val exec  = sqlExec(id = 0L, plan = "", planTree = Some(chain))
    PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> exec)))
      .filter(_.id.startsWith("plan-project-chain")) shouldBe empty
  }

  it should "flag deep Project chain via plan text when planTree is absent" in {
    // 22 lines that start with "Project ["
    val projectLines = (0 until 22).map(i => s"Project [col$i#$i, col${i+1}#${i+1}]").mkString("\n")
    val plan = projectLines + "\n+- LocalRelation"
    val exec = sqlExec(id = 0L, plan = plan, planTree = None)
    val issues = PlanAnalyzer.analyze(app(sqlExecs = Map(0L -> exec)))
    issues.filter(_.id.startsWith("plan-project-chain")) should not be empty
  }

  it should "respect a custom projectChainWarn threshold" in {
    val leaf  = planNode("LocalRelation")
    val chain = (0 until 25).foldLeft(leaf: PlanNode) { (child, _) => planNode("Project", children = Seq(child)) }
    val exec  = sqlExec(id = 0L, plan = "", planTree = Some(chain))
    val a = app(
      sqlExecs = Map(0L -> exec),
      props    = Map("spark.sparklens.plan.projectChainWarn" -> "30"),
    )
    PlanAnalyzer.analyze(a).filter(_.id.startsWith("plan-project-chain")) shouldBe empty
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
