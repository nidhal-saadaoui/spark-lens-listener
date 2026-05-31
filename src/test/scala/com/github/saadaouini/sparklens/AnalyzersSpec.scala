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

  it should "use generic suffix when both stages and jobs are empty" in {
    val issues = Seq(
      Issue("cache-111", Warning, "cache", "Repeated Scan", "d", "r"),
      Issue("cache-222", Warning, "cache", "Repeated Scan", "d", "r"),
    )
    val result = Analyzers.group(issues)
    result.head.title should include("[+1 more]")
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
