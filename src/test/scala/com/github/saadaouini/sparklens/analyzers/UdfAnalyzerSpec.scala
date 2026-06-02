package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UdfAnalyzerSpec extends AnyFlatSpec with Matchers {

  "UdfAnalyzer" should "return no issues for an empty app" in {
    UdfAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues for a plan with no UDFs" in {
    val plan = "SortMergeJoin [key#1], [key#2]\n+- LocalRelation"
    UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan)))) shouldBe empty
  }

  it should "flag PythonUDF in plan text as Warning" in {
    val plan = "Project [PythonUDF(myFunc, x#1) AS result#2]\n+- LocalRelation"
    val issues = UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues should have size 1
    issues.head.severity shouldBe Warning
    issues.head.title should include("Python UDF")
    issues.head.id shouldBe "plan-udf-0"
  }

  it should "flag BatchEvalPython in plan text" in {
    val plan = "BatchEvalPython [myFunc(x#1)]\n+- LocalRelation"
    val issues = UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues should not be empty
    issues.head.title should include("Python")
  }

  it should "flag ArrowEvalPython (pandas UDF) in plan text" in {
    val plan = "ArrowEvalPython [pandas_udf(x#1)]\n+- LocalRelation"
    val issues = UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues should not be empty
  }

  it should "flag PythonUDF via planTree when plan text lacks the node" in {
    val udfNode = planNode("PythonUDF", children = Seq(planNode("LocalRelation")))
    val tree    = planNode("Project", children = Seq(udfNode))
    val exec    = sqlExec(id = 1L, plan = "Project\n+- LocalRelation", planTree = Some(tree))
    val issues  = UdfAnalyzer.analyze(app(sqlExecs = Map(1L -> exec)))
    issues should not be empty
    issues.head.title should include("Python")
  }

  it should "flag Scala UDF marker UDF( in plan text" in {
    val plan = "Project [UDF(x#1) AS result#2]\n+- LocalRelation"
    val issues = UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan))))
    issues should not be empty
    issues.head.title should include("Scala")
  }

  it should "group multiple UDF executions into separate per-execution issues" in {
    val plan = "PythonUDF(f, x#1)"
    val issues = UdfAnalyzer.analyze(app(sqlExecs = Map(
      0L -> sqlExec(id = 0L, plan = plan),
      1L -> sqlExec(id = 1L, plan = plan),
    )))
    issues should have size 2
    issues.map(_.id).toSet shouldBe Set("plan-udf-0", "plan-udf-1")
  }

  it should "not flag a plan that contains only the word 'UDF' in a comment position" in {
    val plan = "SortMergeJoin\n// No UDF here"
    UdfAnalyzer.analyze(app(sqlExecs = Map(0L -> sqlExec(plan = plan)))) shouldBe empty
  }
}
