package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.analyzers.AnalyzerFixtures
import com.github.saadaouini.sparklens.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalyzersSpec extends AnyFlatSpec with Matchers {
  import AnalyzerFixtures._

  // ── issue grouping ────────────────────────────────────────────────────────

  "Analyzers.group" should "leave a single issue unchanged" in {
    val issues = Seq(Issue("spill-0", Warning, "spill", "Spill in Stage 0", "desc", "rec"))
    Analyzers.group(issues) should have size 1
    Analyzers.group(issues).head.id shouldBe "spill-0"
  }

  it should "merge same-type issues into one with combined affectedStages" in {
    val issues = Seq(
      Issue("spill-0", Warning, "spill", "Disk Spill in Stage 0", "desc", "rec", affectedStages = Seq(0)),
      Issue("spill-1", Warning, "spill", "Disk Spill in Stage 1", "desc", "rec", affectedStages = Seq(1)),
      Issue("spill-2", Warning, "spill", "Disk Spill in Stage 2", "desc", "rec", affectedStages = Seq(2)),
    )
    val result = Analyzers.group(issues)
    result should have size 1
    result.head.id shouldBe "spill"
    result.head.affectedStages shouldBe Seq(0, 1, 2)
    result.head.title should include("[+2 more stages]")
  }

  it should "not merge issues with different group keys" in {
    val issues = Seq(
      Issue("spill-0",  Warning, "spill", "Disk Spill",   "desc", "rec", affectedStages = Seq(0)),
      Issue("gc-0",     Warning, "gc",    "High GC",      "desc", "rec", affectedStages = Seq(0)),
    )
    val result = Analyzers.group(issues)
    result should have size 2
  }

  it should "keep skew-crit and skew-warn as separate groups" in {
    val issues = Seq(
      Issue("skew-crit-0", Critical, "skew", "Crit Skew",    "desc", "rec", affectedStages = Seq(0)),
      Issue("skew-crit-1", Critical, "skew", "Crit Skew",    "desc", "rec", affectedStages = Seq(1)),
      Issue("skew-warn-2", Warning,  "skew", "Warn Skew",    "desc", "rec", affectedStages = Seq(2)),
    )
    val result = Analyzers.group(issues)
    result should have size 2
    // multi-element group gets its ID stripped ("skew-crit"); singleton keeps its original ID ("skew-warn-2")
    result.map(_.id).toSet shouldBe Set("skew-crit", "skew-warn-2")
  }

  it should "use the most severe issue as representative when merging" in {
    val issues = Seq(
      Issue("spill-0", Warning,  "spill", "Disk Spill in Stage 0", "desc", "rec", affectedStages = Seq(0)),
      Issue("spill-1", Critical, "spill", "Disk Spill in Stage 1", "desc", "rec", affectedStages = Seq(1)),
    )
    val result = Analyzers.group(issues)
    result should have size 1
    result.head.severity shouldBe Critical
    result.head.affectedStages shouldBe Seq(0, 1)
  }

  it should "preserve config fixes from the representative issue" in {
    val issues = Seq(
      Issue("spill-0", Critical, "spill", "Spill 0", "d", "r", configFix = Some("spark.x=1"), affectedStages = Seq(0)),
      Issue("spill-1", Warning,  "spill", "Spill 1", "d", "r", configFix = None,              affectedStages = Seq(1)),
    )
    val result = Analyzers.group(issues)
    result.head.configFix shouldBe Some("spark.x=1")
  }

  it should "merge affectedJobs and use 'queries' suffix when no stages" in {
    val issues = Seq(
      Issue("join-excessive-shuffle-0", Warning, "join", "Excessive Shuffles in q0", "d", "r", affectedJobs = Seq(0, 1)),
      Issue("join-excessive-shuffle-1", Warning, "join", "Excessive Shuffles in q1", "d", "r", affectedJobs = Seq(2, 3)),
    )
    val result = Analyzers.group(issues)
    result should have size 1
    result.head.affectedJobs shouldBe Seq(0, 1, 2, 3)
    result.head.title should include("[+1 more queries]")
  }

  it should "sum savedTimeMs and savedBytes across grouped issues" in {
    val imp = (ms: Long, b: Long) => Some(EstimatedImpact("s", Some(ms), Some(b), "medium"))
    val issues = Seq(
      Issue("cpu-0", Info, "io", "Low CPU Stage 0", "d", "r", affectedStages = Seq(0), estimatedImpact = imp(5000L, 100L)),
      Issue("cpu-1", Info, "io", "Low CPU Stage 1", "d", "r", affectedStages = Seq(1), estimatedImpact = imp(3000L, 200L)),
    )
    val result = Analyzers.group(issues)
    result should have size 1
    result.head.estimatedImpact.flatMap(_.savedTimeMs) shouldBe Some(8000L)
    result.head.estimatedImpact.flatMap(_.savedBytes)  shouldBe Some(300L)
  }

  it should "recover savings when rep has no estimatedImpact but other issues do" in {
    val imp = Some(EstimatedImpact("s", Some(4000L), None, "medium"))
    val issues = Seq(
      Issue("cpu-0", Info, "io", "Low CPU Stage 0", "d", "r", affectedStages = Seq(0), estimatedImpact = None),
      Issue("cpu-1", Info, "io", "Low CPU Stage 1", "d", "r", affectedStages = Seq(1), estimatedImpact = imp),
    )
    val result = Analyzers.group(issues)
    result should have size 1
    result.head.estimatedImpact.flatMap(_.savedTimeMs) shouldBe Some(4000L)
  }

  it should "use generic suffix when both stages and jobs are empty" in {
    val issues = Seq(
      Issue("cache-111", Warning, "cache", "Repeated Scan", "d", "r"),
      Issue("cache-222", Warning, "cache", "Repeated Scan", "d", "r"),
    )
    val result = Analyzers.group(issues)
    result.head.title should include("[+1 more]")
  }

  // ── relatedIds linking ────────────────────────────────────────────────────

  "Analyzers.linkRelated" should "populate relatedIds for issues sharing an affected stage" in {
    val imp = Some(EstimatedImpact("s", Some(5000L), None, "medium"))
    val issues = Seq(
      Issue("spill-1",  Warning, "spill", "Spill Stage 1",  "d", "r", affectedStages = Seq(1), estimatedImpact = imp),
      Issue("cpu-1",    Info,    "io",    "CPU Stage 1",    "d", "r", affectedStages = Seq(1), estimatedImpact = imp),
      Issue("gc-2",     Warning, "gc",    "GC Stage 2",     "d", "r", affectedStages = Seq(2), estimatedImpact = imp),
    )
    val linked = Analyzers.linkRelated(issues)
    linked.find(_.id == "spill-1").get.relatedIds should contain("cpu-1")
    linked.find(_.id == "cpu-1").get.relatedIds   should contain("spill-1")
    linked.find(_.id == "gc-2").get.relatedIds    shouldBe empty
  }

  it should "not populate relatedIds for issues without savedTimeMs" in {
    val withSavings  = Issue("spill-1", Warning, "spill", "Spill", "d", "r",
      affectedStages = Seq(1), estimatedImpact = Some(EstimatedImpact("s", Some(5000L), None, "medium")))
    val noSavings    = Issue("config-x", Info, "config", "Config", "d", "r",
      affectedStages = Seq(1), estimatedImpact = Some(EstimatedImpact("s", None, None, "low")))
    val linked = Analyzers.linkRelated(Seq(withSavings, noSavings))
    linked.find(_.id == "spill-1").get.relatedIds shouldBe empty
    linked.find(_.id == "config-x").get.relatedIds shouldBe empty
  }

  // ── end-to-end: grouping in runAll ────────────────────────────────────────

  "Analyzers.runAll" should "group spill issues from multiple stages into one" in {
    val stages = (0 until 3).map { i =>
      i -> stage(stageId = i, tasks = (1 to 5).map(_ => task(diskSpill = 200L * MB)))
    }.toMap
    val issues = Analyzers.runAll(app(stages = stages))
    val spillIssues = issues.filter(_.category == "spill")
    spillIssues should have size 1
    spillIssues.head.affectedStages should contain allOf (0, 1, 2)
  }

  it should "not add suffix when only one stage is affected" in {
    val stages = Map(0 -> stage(stageId = 0, tasks = (1 to 5).map(_ => task(diskSpill = 200L * MB))))
    val issues = Analyzers.runAll(app(stages = stages))
    val spillIssues = issues.filter(_.category == "spill")
    spillIssues should have size 1
    spillIssues.head.title should not include "[+"
  }
}
