package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, EstimatedImpact, Info, Issue, SparkAppModel, Warning}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LogReporterSpec extends AnyFlatSpec with Matchers {

  import java.util.logging.{Level => JLevel}

  private val baseApp: SparkAppModel = SparkAppModel(
    appId           = "app-001",
    appName         = "MySparkJob",
    sparkVersion    = "3.5.0",
    startTimeMs     = 0L,
    endTimeMs       = Some(252000L),
    sparkProperties = Map.empty,
    jobs            = Map.empty,
    stages          = Map.empty,
    executors       = Map.empty,
    sqlExecutions   = Map.empty,
  )

  private def issue(
    id:       String  = "test-0",
    severity: com.github.saadaouini.sparklens.model.Severity = Warning,
    category: String  = "skew",
    title:    String  = "Test Issue",
    configFix: Option[String] = None,
    stages:   Seq[Int] = Nil,
    jobs:     Seq[Int] = Nil,
    savings:  Option[Long] = None,
    metrics:  Map[String, String] = Map.empty,
  ): Issue = Issue(
    id              = id,
    severity        = severity,
    category        = category,
    title           = title,
    description     = "description",
    recommendation  = "recommendation",
    configFix       = configFix,
    affectedStages  = stages,
    affectedJobs    = jobs,
    metrics         = metrics,
    estimatedImpact = savings.map(ms => EstimatedImpact("summary", Some(ms), None, "high")),
  )

  // ── Structure ──────────────────────────────────────────────────────────────

  "LogReporter" should "produce exactly one SUMMARY line plus one line per issue" in {
    val issues = Seq(
      issue(id = "a-0", severity = Critical),
      issue(id = "b-0", severity = Warning),
      issue(id = "c-0", severity = Info),
    )
    val lines = LogReporter.renderLines(baseApp, issues)
    lines should have size 4  // 1 SUMMARY + 3 issues
  }

  it should "prefix every line with [spark-lens]" in {
    val lines = LogReporter.renderLines(baseApp,
      Seq(issue("x-0", Warning), issue("y-0", Info)))
    lines.map(_._2).foreach(_ should startWith("[spark-lens]"))
  }

  it should "produce one line per issue with no embedded newlines" in {
    val issues = Seq(issue("a-0", Warning, title = "Title with\nnewline"))
    val lines = LogReporter.renderLines(baseApp, issues)
    lines.foreach { case (_, line) => line should not contain '\n' }
  }

  // ── Severity labels and log levels ────────────────────────────────────────

  it should "label Critical issues as CRITICAL" in {
    val lines = LogReporter.renderLines(baseApp, Seq(issue("c-0", Critical)))
    val issueLine = lines.find(_._2.contains("id=c-0")).get
    issueLine._2 should include("CRITICAL")
  }

  it should "label Warning issues as WARNING" in {
    val lines = LogReporter.renderLines(baseApp, Seq(issue("w-0", Warning)))
    lines.find(_._2.contains("id=w-0")).get._2 should include("WARNING")
  }

  it should "label Info issues as INFO" in {
    val lines = LogReporter.renderLines(baseApp, Seq(issue("i-0", Info)))
    lines.find(_._2.contains("id=i-0")).get._2 should include("INFO")
  }

  it should "set java.util.logging.Level.WARNING for Critical and Warning issues" in {
    val lines = LogReporter.renderLines(baseApp,
      Seq(issue("c-0", Critical), issue("w-0", Warning)))
    lines.filter(_._2.contains("id=c-0")).head._1 shouldBe JLevel.WARNING
    lines.filter(_._2.contains("id=w-0")).head._1 shouldBe JLevel.WARNING
  }

  it should "set java.util.logging.Level.INFO for Info issues" in {
    val lines = LogReporter.renderLines(baseApp, Seq(issue("i-0", Info)))
    lines.filter(_._2.contains("id=i-0")).head._1 shouldBe JLevel.INFO
  }

  // ── SUMMARY line content ──────────────────────────────────────────────────

  it should "include health score in SUMMARY" in {
    val out = LogReporter.renderString(baseApp, Seq(issue("w-0", Warning)))
    out should include("health=90")  // 100 - 1×10 = 90
  }

  it should "include issue counts in SUMMARY" in {
    val issues = Seq(issue("c-0", Critical), issue("w-0", Warning), issue("i-0", Info))
    val out = LogReporter.renderString(baseApp, issues)
    out should include("critical=1")
    out should include("warning=1")
    out should include("info=1")
  }

  it should "include savings in SUMMARY when issues have estimated savings" in {
    val issues = Seq(issue("w-0", Warning, savings = Some(5000L)))
    val out = LogReporter.renderString(baseApp, issues)
    out should include("savings=")
  }

  it should "omit savings from SUMMARY when no issues have savings" in {
    val issues = Seq(issue("i-0", Info, savings = None))
    val summaryLine = LogReporter.renderLines(baseApp, issues).head._2
    summaryLine should not include "savings="
  }

  // ── Issue line content ────────────────────────────────────────────────────

  it should "include id, category, and title in each issue line" in {
    val issues = Seq(issue("skew-0", Warning, category = "skew", title = "Hot Key Skew"))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=skew-0")).get._2
    line should include("id=skew-0")
    line should include("category=skew")
    line should include("title=")
    line should include("Hot Key Skew")
  }

  it should "include configFix as fix= field" in {
    val issues = Seq(issue("c-0", Warning, configFix = Some("spark.sql.adaptive.enabled=true")))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=c-0")).get._2
    line should include("fix=")
    line should include("spark.sql.adaptive.enabled=true")
  }

  it should "include affected stages as stages= field" in {
    val issues = Seq(issue("s-3", Warning, stages = Seq(3, 5)))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=s-3")).get._2
    line should include("stages=3,5")
  }

  it should "include affected jobs as jobs= field" in {
    val issues = Seq(issue("j-0", Warning, jobs = Seq(1, 2)))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=j-0")).get._2
    line should include("jobs=1,2")
  }

  it should "include savings as savings= field when present" in {
    val issues = Seq(issue("sp-0", Warning, savings = Some(3200L)))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=sp-0")).get._2
    line should include("savings=3.2s/run")
  }

  it should "include up to 3 metrics as key=value fields" in {
    val issues = Seq(issue("m-0", Warning, metrics = Map(
      "p50_ms" -> "500", "p95_ms" -> "2000", "stragglers" -> "3", "extra" -> "ignored")))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=m-0")).get._2
    // At most 3 metrics should appear
    val metricCount = Seq("p50_ms", "p95_ms", "stragglers", "extra").count(line.contains)
    metricCount should be <= 3
  }

  it should "quote titles that contain spaces" in {
    val issues = Seq(issue("t-0", Warning, title = "Skew in Stage 3"))
    val line = LogReporter.renderLines(baseApp, issues)
      .find(_._2.contains("id=t-0")).get._2
    line should include("title=\"Skew in Stage 3\"")
  }

  // ── renderString ──────────────────────────────────────────────────────────

  it should "produce no issues for an empty app" in {
    val out = LogReporter.renderString(baseApp, Nil)
    val lines = out.trim.split("\n")
    lines should have size 1  // only the SUMMARY line
    lines.head should include("SUMMARY")
    lines.head should include("issues=0")
  }
}
