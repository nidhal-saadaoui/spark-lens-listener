package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{PlanNode, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Helpers for plan-aware tests
private object PlanFixtures {
  import AnalyzerFixtures._

  def singleTaskStage(id: Int = 0) =
    stage(stageId = id, tasks = Seq(task(durationMs = 30000L)),
          submitMs = Some(0L), completeMs = Some(30000L))
      .copy(numTasks = 1)

  def appWithPlan(planText: String = "", planTree: Option[PlanNode] = None) = {
    val s   = singleTaskStage(0)
    val j   = job(jobId = 0, stageIds = Seq(0))
    val sql = sqlExec(id = 0L, description = "test query", plan = planText,
                      jobIds = Seq(0), planTree = planTree)
    app(stages = Map(0 -> s), jobs = Map(0 -> j), sqlExecs = Map(0L -> sql))
  }
}

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
  // ── Single-task stage detection ──────────────────────────────────────────────

  it should "flag a single-task stage that runs longer than the threshold" in {
    val s = stage(stageId = 0, tasks = Seq(task(durationMs = 10000L)),
                  submitMs = Some(0L), completeMs = Some(10000L))
    // numTasks=1 on a StageData is inferred from tasks.size in the fixture
    val a = app(stages = Map(0 -> s.copy(numTasks = 1)))
    val issues = StageParallelismAnalyzer.analyze(a)
    val single = issues.filter(_.id.startsWith("single-task"))
    single should not be empty
    single.head.severity shouldBe Warning
    single.head.metrics("num_tasks") shouldBe "1"
  }

  it should "not flag a single-task stage shorter than the threshold" in {
    val s = stage(stageId = 0, tasks = Seq(task(durationMs = 100L)),
                  submitMs = Some(0L), completeMs = Some(100L))
    val a = app(stages = Map(0 -> s.copy(numTasks = 1)))
    StageParallelismAnalyzer.analyze(a).filter(_.id.startsWith("single-task")) shouldBe empty
  }

  it should "not flag single-task stages with a custom singleTaskMinMs" in {
    val s = stage(stageId = 0, tasks = Seq(task(durationMs = 3000L)),
                  submitMs = Some(0L), completeMs = Some(3000L))
    val a = app(
      stages = Map(0 -> s.copy(numTasks = 1)),
      props  = Map("spark.sparklens.stageParallelism.singleTaskMinMs" -> "10000"),
    )
    StageParallelismAnalyzer.analyze(a).filter(_.id.startsWith("single-task")) shouldBe empty
  }

  it should "not produce a low-parallelism issue for a single-task stage already caught by single-task check" in {
    val s = stage(stageId = 0, tasks = Seq(task(durationMs = 30000L)),
                  submitMs = Some(0L), completeMs = Some(30000L))
              .copy(numTasks = 1)
    val exc = executor(id = "0", host = "h1").copy(totalCores = 100)
    val a = app(stages = Map(0 -> s), executors = Map("0" -> exc))
    val issues = StageParallelismAnalyzer.analyze(a)
    issues.exists(_.id.startsWith("single-task"))    shouldBe true
    issues.exists(_.id.startsWith("low-parallelism")) shouldBe false
  }

  it should "not produce a single-task issue for a multi-task stage" in {
    val tasks = (0 until 4).map(i => task(id = i, durationMs = 10000L))
    val s = stage(stageId = 0, tasks = tasks, submitMs = Some(0L), completeMs = Some(10000L))
    val a = app(stages = Map(0 -> s.copy(numTasks = 4)))
    StageParallelismAnalyzer.analyze(a).filter(_.id.startsWith("single-task")) shouldBe empty
  }

  // ── Plan-aware cause detection ────────────────────────────────────────────

  "StageParallelismAnalyzer plan detection" should "identify Coalesce 1 from planTree" in {
    import PlanFixtures._
    val tree = planNode("Coalesce", simpleString = "Coalesce 1",
                 children = Seq(planNode("Project")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("Coalesce 1 node detected")
    i.codeFix.get   should include("coalesce(")
    i.configFix     shouldBe None
  }

  it should "not mistake Coalesce 10 for Coalesce 1 in planTree" in {
    import PlanFixtures._
    val tree = planNode("Coalesce", simpleString = "Coalesce 10",
                 children = Seq(planNode("Project")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    // Should fall through to unknown cause — Coalesce 10 is NOT a single-partition cause
    i.description should not include "Coalesce 1 node"
  }

  it should "identify repartition(1) via RoundRobinPartitioning(1) in planTree" in {
    import PlanFixtures._
    val tree = planNode("Exchange", simpleString = "Exchange RoundRobinPartitioning(1), ENSURE_REQUIREMENTS",
                 children = Seq(planNode("Project")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("repartition(1)")
    i.codeFix.get   should include("repartition(")
  }

  it should "identify SinglePartition exchange in planTree" in {
    import PlanFixtures._
    val tree = planNode("Exchange", simpleString = "Exchange SinglePartition, ENSURE_REQUIREMENTS",
                 children = Seq(planNode("Sort")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description   should include("SinglePartition")
    i.configFix     shouldBe defined
    i.configFix.get should include("spark.sql.adaptive.enabled")
  }

  it should "identify CollectLimit in planTree" in {
    import PlanFixtures._
    val tree = planNode("CollectLimit", simpleString = "CollectLimit 100",
                 children = Seq(planNode("Project")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("CollectLimit")
    i.codeFix.get should include("limit")
  }

  it should "identify TakeOrderedAndProject in planTree" in {
    import PlanFixtures._
    val tree = planNode("TakeOrderedAndProject", simpleString = "TakeOrderedAndProject(limit=10)",
                 children = Seq(planNode("Project")))
    val issues = StageParallelismAnalyzer.analyze(appWithPlan(planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("TakeOrderedAndProject")
  }

  it should "fall back to text plan when planTree is absent" in {
    import PlanFixtures._
    val issues = StageParallelismAnalyzer.analyze(
      appWithPlan(planText = "...Coalesce 1\n...Project...", planTree = None))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("Coalesce 1 node detected")
  }

  it should "fall back to text plan when planTree has no matching node" in {
    import PlanFixtures._
    // planTree present but no coalesce/exchange node → check text plan
    val tree = planNode("Project", children = Seq(planNode("Filter")))
    val issues = StageParallelismAnalyzer.analyze(
      appWithPlan(planText = "RoundRobinPartitioning(1)", planTree = Some(tree)))
    val i = issues.filter(_.id.startsWith("single-task")).head
    i.description should include("repartition(1)")
  }

  it should "use unknown cause when no SQL plan exists for the stage" in {
    // Stage with no matching SQL execution (no jobs/sqls in app)
    val s = PlanFixtures.singleTaskStage(0)
    val a = app(stages = Map(0 -> s))
    val i = StageParallelismAnalyzer.analyze(a).filter(_.id.startsWith("single-task")).head
    i.description should include("no SQL plan available")
  }

  it should "include SQL query description in the issue description when plan is found" in {
    import PlanFixtures._
    val tree = planNode("Coalesce", simpleString = "Coalesce 1")
    val issues = StageParallelismAnalyzer.analyze(
      appWithPlan(planTree = Some(tree)))
    issues.filter(_.id.startsWith("single-task")).head.description should include("test query")
  }
}
