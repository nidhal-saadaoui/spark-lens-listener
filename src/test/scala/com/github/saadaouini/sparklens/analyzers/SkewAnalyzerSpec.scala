package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Critical, Warning}
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SkewAnalyzerSpec extends AnyFlatSpec with Matchers {

  "SkewAnalyzer" should "return no issues when all tasks have equal duration" in {
    val tasks = (1 to 10).map(_ => task(durationMs = 1000L))
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when fewer than MinTasks" in {
    val tasks = Seq(task(1000L), task(10000L), task(1000L))
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "return no issues when p50 is below the minimum duration threshold" in {
    // p50 = 100ms < MinP50Ms — stage is too fast to care about skew
    val tasks = (1 to 10).map(_ => task(durationMs = 100L)) :+ task(durationMs = 900L)
    SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks)))) shouldBe empty
  }

  it should "detect warning skew via concentration signal" in {
    // 1 task is 4× the rest — concentration of top 5% exceeds ConcWarn threshold
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 4000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Warning
  }

  it should "detect critical skew via concentration signal" in {
    // 1 task holds >50% of total stage time → concCrit fires
    val tasks = (1 to 9).map(_ => task(durationMs = 1000L)) :+ task(durationMs = 10000L)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
  }

  it should "detect critical skew via p95/p50 ratio" in {
    // Multiple slow tasks so p95 itself is skewed, not just the single max
    val tasks = (1 to 8).map(_ => task(durationMs = 1000L)) ++ Seq(task(10000L), task(10000L))
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should have size 1
    issues.head.severity shouldBe Critical
    issues.head.metrics("p95_ratio").toDouble should be >= 8.0
  }

  it should "detect skew via shuffle read bytes and classify as shuffle type" in {
    // Uniform task durations but one task reads 30× more shuffle data → hot-key partition
    val normal = (0 until 9).map(_ => task(durationMs = 1000L, remoteShuffleBytes = 1 * MB))
    val hot    = task(durationMs = 1200L, remoteShuffleBytes = 30 * MB)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = normal :+ hot))))
    issues should not be empty
    issues.head.metrics("skew_type") shouldBe "shuffle"
  }

  it should "classify skew as input type when tasks read mostly from files" in {
    val normal = (0 until 9).map(_ => task(durationMs = 1000L, inputBytes = 10 * MB))
    val large  = task(durationMs = 5000L, inputBytes = 300 * MB)
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = normal :+ large))))
    issues should not be empty
    issues.head.metrics("skew_type") shouldBe "input"
  }

  it should "detect skew in multiple stages independently" in {
    val skewedTasks = (1 to 9).map(_ => task(1000L)) :+ task(10000L)
    val normalTasks = (1 to 10).map(_ => task(1000L))
    val stages = Map(
      0 -> stage(stageId = 0, tasks = skewedTasks),
      1 -> stage(stageId = 1, tasks = normalTasks),
    )
    val issues = SkewAnalyzer.analyze(app(stages = stages))
    issues should have size 1
    issues.head.affectedStages should contain(0)
  }

  it should "not fire when p95 ratio is below a custom warnP95Ratio" in {
    // 100 tasks: 95 at 1000ms, 5 at 4000ms
    // p95/p50 ratio ~4× — above default 3.0 but below custom 5.0
    // concentration: top5% (5 tasks at 4000ms = 20000ms) / total (115000ms) = 17.4% < ConcWarn 25%
    val base  = (0 until 95).map(i => task(id = i, durationMs = 1000L))
    val strag = (95 until 100).map(i => task(id = i, durationMs = 4000L))
    val tasks = base ++ strag
    val a = app(stages = Map(0 -> stage(tasks = tasks)), props = Map("spark.sparklens.skew.warnP95Ratio" -> "5.0"))
    SkewAnalyzer.analyze(a) shouldBe empty
  }

  it should "not inflate straggler count due to killed tasks with zero executorRunTimeMs" in {
    // 9 normal tasks (1 s each) + 1 skewed task (10 s) + 2 killed tasks (0 ms executorRunTimeMs).
    // The killed tasks should be dropped by the filter and must NOT cause the straggler count
    // to scale up (the reservoir was not truncated — all 12 tasks fit in the sample).
    val normal  = (0 until 9).map(i  => task(id = i,      durationMs = 1000L))
    val skewed  = task(id = 9,  durationMs = 10000L)
    val killed  = (10 until 12).map(i => task(id = i,     durationMs = 100L,
                                               executorRunTimeMs = 0L))
    val tasks = normal ++ Seq(skewed) ++ killed
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    issues should not be empty
    // Only 1 genuine straggler — killed tasks must not inflate this to 2 or 3
    issues.head.metrics("stragglers").toInt shouldBe 1
  }

  it should "respect a custom minTasks threshold" in {
    // Only 5 tasks — below default minTasks=10 but above custom minTasks=3
    val skewed = Seq(task(1000L), task(1000L), task(1000L), task(1000L), task(20000L))
    val a = app(stages = Map(0 -> stage(tasks = skewed)), props = Map("spark.sparklens.skew.minTasks" -> "3"))
    SkewAnalyzer.analyze(a) should not be empty
  }

  it should "attach estimatedImpact to stage-skew issue" in {
    val normal = (0 until 9).map(i => task(id = i, executorRunTimeMs = 1000L))
    val skewed = task(id = 9, executorRunTimeMs = 15000L)
    val tasks  = normal :+ skewed
    val issues = SkewAnalyzer.analyze(app(stages = Map(0 -> stage(tasks = tasks))))
    val stageIssues = issues.filter(i => i.id.startsWith("skew-") && !i.id.contains("exchange"))
    stageIssues should not be empty
    val imp = stageIssues.head.estimatedImpact
    imp shouldBe defined
    imp.get.savedTimeMs.exists(_ > 0) shouldBe true
    imp.get.confidence shouldBe "high"
  }

  // ── Exchange-node byte skew (SQL plan signal) ─────────────────────────────

  it should "detect Exchange byte skew when one partition holds 90% of bytes" in {
    // Simulate a 90%-hot-key case: 1 partition has 9 MB, 9 others share the remaining 1 MB.
    // p50 ≈ 55 KB; concentration ≈ 0.9 >> ExchConcWarn(0.25) → Warning.
    val accId = 42L
    val bytesPerTask: Map[Long, Long] = Map(
      accId -> (9L * MB)   // one task writes 9 MB
    ) // remaining 9 tasks in the resolved metrics would each have ~111 KB;
      // model via 10 separate accumulator IDs for a clean 10-entry distribution
    val taskDist: Map[Long, Long] =
      (accId until accId + 10).map { id =>
        id -> (if (id == accId) 9L * MB else 111L * 1024L)
      }.toMap
    val exchNode = planNode("ShuffleExchange", accumIds = taskDist.keys.toSeq, metrics = taskDist)
    val tree     = planNode("root", children = Seq(exchNode))
    val exec     = sqlExec(id = 1L, description = "hot-key query", planTree = Some(tree))
    val issues   = SkewAnalyzer.analyze(app(sqlExecs = Map(1L -> exec)))
    val skewIssues = issues.filter(_.id.startsWith("skew-"))
    skewIssues should not be empty
    skewIssues.head.metrics("skew_type") shouldBe "exchange"
  }

  it should "emit Critical when top-5% partitions hold 50%+ of Exchange bytes" in {
    // 10 tasks: 1 has 95 MB, 9 share 5 MB → concentration ≈ 0.95 > ExchConcCrit(0.50)
    val taskDist: Map[Long, Long] =
      (100L until 110L).map { id =>
        id -> (if (id == 100L) 95L * MB else 555L * 1024L)
      }.toMap
    val exchNode = planNode("ShuffleExchange", accumIds = taskDist.keys.toSeq, metrics = taskDist)
    val tree     = planNode("root", children = Seq(exchNode))
    val exec     = sqlExec(id = 2L, planTree = Some(tree))
    val issues   = SkewAnalyzer.analyze(app(sqlExecs = Map(2L -> exec)))
    issues.exists(_.id.startsWith("skew-crit")) shouldBe true
  }

  it should "not flag Exchange nodes with fewer than minTasks entries" in {
    // Only 5 entries (below default minTasks=10) — no issue
    val taskDist: Map[Long, Long] =
      (200L until 205L).map(id => id -> (if (id == 200L) 9L * MB else 50L * 1024L)).toMap
    val exchNode = planNode("ShuffleExchange", accumIds = taskDist.keys.toSeq, metrics = taskDist)
    val exec     = sqlExec(id = 3L, planTree = Some(planNode("root", children = Seq(exchNode))))
    SkewAnalyzer.analyze(app(sqlExecs = Map(3L -> exec))).filter(_.id.startsWith("skew-")) shouldBe empty
  }

  it should "not flag Exchange nodes with balanced byte distribution" in {
    // All 10 tasks write ~1 MB — concentration well below 0.25
    val taskDist: Map[Long, Long] = (300L until 310L).map(id => id -> MB).toMap
    val exchNode = planNode("ShuffleExchange", accumIds = taskDist.keys.toSeq, metrics = taskDist)
    val exec     = sqlExec(id = 4L, planTree = Some(planNode("root", children = Seq(exchNode))))
    SkewAnalyzer.analyze(app(sqlExecs = Map(4L -> exec))).filter(_.id.startsWith("skew-")) shouldBe empty
  }

  it should "not flag Exchange nodes below ExchMinBytes (trivial queries)" in {
    // Total = 10 × 50 bytes = 500 bytes < 1 KB threshold
    val taskDist: Map[Long, Long] = (400L until 410L).map(id => id -> 50L).toMap
    val exchNode = planNode("ShuffleExchange", accumIds = taskDist.keys.toSeq, metrics = taskDist)
    val exec     = sqlExec(id = 5L, planTree = Some(planNode("root", children = Seq(exchNode))))
    SkewAnalyzer.analyze(app(sqlExecs = Map(5L -> exec))).filter(_.id.startsWith("skew-")) shouldBe empty
  }
}
