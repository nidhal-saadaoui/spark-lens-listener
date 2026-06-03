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

  "TextReporter" should "show 5 priority fixes by default" in {
    val issues = (1 to 7).map(i => issueWithSavings(s"issue-$i", i * 10000L))
    val output = TextReporter.renderString(app(), issues)
    // Count lines matching "  N. [" pattern (priority fix entries)
    val priorityLines = output.linesIterator
      .filter(l => l.trim.matches("""\d+\. \[.*"""))
      .toSeq
    priorityLines should have size 5
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
}
