package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.analyzers.AnalyzerFixtures
import com.github.saadaouini.sparklens.model.{EstimatedImpact, Issue, Warning}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TextReporterSpec extends AnyFlatSpec with Matchers {

  import AnalyzerFixtures._

  private def issueWithSavings(id: String, ms: Long): Issue =
    Issue(id, Warning, "io", s"Issue $id", "desc", "rec",
      estimatedImpact = Some(EstimatedImpact("s", Some(ms), None, "medium")))

  "TextReporter" should "show all issues up to 20 by default" in {
    val issues = (1 to 7).map(i => issueWithSavings(s"issue-$i", i * 10000L))
    val output = TextReporter.renderString(app(), issues)
    // All 7 issues have savings >= 1s and total < 20 — all should appear
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 7
  }

  it should "respect spark.sparklens.report.maxPriorityFixes=2" in {
    val issues = (1 to 7).map(i => issueWithSavings(s"issue-$i", i * 10000L))
    val output = TextReporter.renderString(
      app(props = Map("spark.sparklens.report.maxPriorityFixes" -> "2")), issues)
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 2
  }

  it should "show relatedIds note when present" in {
    val issue = issueWithSavings("spill-1", 30000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("cpu", "low-parallelism"))
    val output = TextReporter.renderString(app(), Seq(issue))
    output should include("likely shares root cause with: cpu, low-parallelism")
  }

  it should "not show relatedIds note when relatedIds is empty" in {
    val issue = issueWithSavings("spill-1", 30000L)
    val output = TextReporter.renderString(app(), Seq(issue))
    output should not include "likely shares root cause"
  }

  it should "suppress pctOfApp label when savings exceed app duration" in {
    // app duration = 300s (default fixture), savings = 400s → > 100% → label suppressed
    val issue = issueWithSavings("big-savings-1", 400000L)
    val output = TextReporter.renderString(app(), Seq(issue))
    output should not include "% of app time"
  }

  it should "show pctOfApp when savings are within app duration" in {
    // savings = 60s out of 300s app → 20% → label shown
    val issue = issueWithSavings("reasonable-1", 60000L)
    val output = TextReporter.renderString(app(), Seq(issue))
    output should include("of app time")
  }

  // ── Priority fixes — root-cause cluster deduplication ───────────────────────

  it should "collapse a 3-issue cluster into one priority fix entry with (+2 covered) suffix" in {
    val i1 = issueWithSavings("spill-1", 30000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("cpu-1", "parallelism-1"))
    val i2 = issueWithSavings("cpu-1", 20000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("spill-1", "parallelism-1"))
    val i3 = issueWithSavings("parallelism-1", 15000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("spill-1", "cpu-1"))
    val output = TextReporter.renderString(app(), Seq(i1, i2, i3))
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 1
    priorityLines.head should include("(+2 covered)")
  }

  it should "show separate entries for issues in different clusters" in {
    val i1 = issueWithSavings("spill-1", 30000L).copy(affectedStages = Seq(1))
    val i2 = issueWithSavings("skew-2", 20000L).copy(affectedStages = Seq(2))
    val output = TextReporter.renderString(app(), Seq(i1, i2))
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 2
    output should not include "(+1 covered)"
  }

  it should "show the highest-savings issue as the representative when collapsing a cluster" in {
    // i1 has highest savings: should be the representative shown in the list
    val i1 = issueWithSavings("spill-1", 30000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("cpu-1"))
    val i2 = issueWithSavings("cpu-1", 5000L).copy(
      affectedStages = Seq(1), relatedIds = Seq("spill-1"))
    val output = TextReporter.renderString(app(), Seq(i1, i2))
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 1
    priorityLines.head should include("Issue spill-1")
  }

  // ── SparkLens overhead footer ─────────────────────────────────────────────

  it should "show overhead footer when listenerStats has task events" in {
    import com.github.saadaouini.sparklens.model.ListenerStats
    val appWithStats = app().copy(listenerStats = ListenerStats(2500000L, 340L))
    val output = TextReporter.renderString(appWithStats, Nil)
    output should include("SparkLens:")
    output should include("M task events")
    output should include("340ms listener overhead")
  }

  it should "omit overhead footer when no task events were processed" in {
    val output = TextReporter.renderString(app(), Nil)
    output should not include "SparkLens:"
  }
}
