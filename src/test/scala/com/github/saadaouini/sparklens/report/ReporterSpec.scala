package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReporterSpec extends AnyFlatSpec with Matchers {

  private def baseApp: SparkAppModel = SparkAppModel(
    appId        = "app-001",
    appName      = "MySparkJob",
    sparkVersion = "3.5.0",
    startTimeMs  = 0L,
    endTimeMs    = Some(60000L),
    sparkProperties = Map.empty,
    jobs         = Map.empty,
    stages       = Map.empty,
    executors    = Map.empty,
    sqlExecutions = Map.empty,
  )

  private def issue(
    id:       String   = "test-1",
    severity: Severity = Warning,
    title:    String   = "Test Issue",
    configFix: Option[String] = None,
    codeFix:   Option[String] = None,
  ): Issue = Issue(
    id             = id,
    severity       = severity,
    category       = "test",
    title          = title,
    description    = "description text",
    recommendation = "recommendation text",
    configFix      = configFix,
    codeFix        = codeFix,
  )

  // ── healthScore ─────────────────────────────────────────────────────────────

  "Reporter.healthScore" should "return 100 for no issues" in {
    TextReporter.renderString(baseApp, Nil) should include("100/100")
  }

  it should "deduct 25 for each Critical issue" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Critical)))
    out should include("75/100")
  }

  it should "deduct 10 for each Warning issue" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Warning)))
    out should include("90/100")
  }

  it should "deduct 3 for each Info issue" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Info)))
    out should include("97/100")
  }

  it should "floor the health score at 0" in {
    val issues = (1 to 10).map(i => issue(id = s"c-$i", severity = Critical))
    val out = TextReporter.renderString(baseApp, issues)
    out should include("0/100")
  }

  // ── TextReporter ─────────────────────────────────────────────────────────────

  "TextReporter" should "include the app name in the header" in {
    TextReporter.renderString(baseApp, Nil) should include("MySparkJob")
  }

  it should "include the app ID in the header" in {
    TextReporter.renderString(baseApp, Nil) should include("app-001")
  }

  it should "show no-issues banner when issues list is empty" in {
    TextReporter.renderString(baseApp, Nil) should include("No issues detected")
  }

  it should "include the issue title" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(title = "Skew Detected")))
    out should include("Skew Detected")
  }

  it should "include CRITICAL label for critical issues" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Critical)))
    out should include("CRITICAL")
  }

  it should "include WARNING label for warning issues" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Warning)))
    out should include("WARNING")
  }

  it should "include INFO label for info issues" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(severity = Info)))
    out should include("INFO")
  }

  it should "include config fix when present" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(configFix = Some("spark.foo=bar"))))
    out should include("spark.foo=bar")
  }

  it should "include code fix when present" in {
    val out = TextReporter.renderString(baseApp, Seq(issue(codeFix = Some("df.cache()"))))
    out should include("df.cache()")
  }

  it should "include severity counts in header when issues present" in {
    val issues = Seq(
      issue(id = "c1", severity = Critical),
      issue(id = "w1", severity = Warning),
      issue(id = "i1", severity = Info),
    )
    val out = TextReporter.renderString(baseApp, issues)
    out should include("1 critical")
    out should include("1 warning")
    out should include("1 info")
  }

  // ── JsonReporter ─────────────────────────────────────────────────────────────

  "JsonReporter" should "produce output starting with {" in {
    JsonReporter.render(baseApp, Nil).trim should startWith("{")
  }

  it should "include app_id field" in {
    JsonReporter.render(baseApp, Nil) should include(""""app_id": "app-001"""")
  }

  it should "include app_name field" in {
    JsonReporter.render(baseApp, Nil) should include(""""app_name": "MySparkJob"""")
  }

  it should "include health_score as 100 for no issues" in {
    JsonReporter.render(baseApp, Nil) should include(""""health_score": 100""")
  }

  it should "include issue_count as 0 for empty issues" in {
    JsonReporter.render(baseApp, Nil) should include(""""issue_count": 0""")
  }

  it should "include issue fields for a non-empty issue list" in {
    val out = JsonReporter.render(baseApp, Seq(issue(id = "skew-1", severity = Critical)))
    out should include(""""id": "skew-1"""")
    out should include(""""severity": "critical"""")
    out should include(""""issue_count": 1""")
  }

  it should "render config_fix as null when absent" in {
    val out = JsonReporter.render(baseApp, Seq(issue(configFix = None)))
    out should include(""""config_fix": null""")
  }

  it should "render config_fix as a string when present" in {
    val out = JsonReporter.render(baseApp, Seq(issue(configFix = Some("spark.foo=bar"))))
    out should include(""""config_fix": "spark.foo=bar"""")
  }

  it should "escape double-quotes in text fields" in {
    val out = JsonReporter.render(baseApp, Seq(issue(title = """He said "hello"""")))
    out should include("""He said \"hello\"""")
    out should not include ("""He said "hello"""")
  }

  it should "escape backslashes in text fields" in {
    val out = JsonReporter.render(baseApp, Seq(issue(title = """path\to\file""")))
    out should include("""path\\to\\file""")
  }

  it should "produce valid-looking JSON with matching braces" in {
    val out = JsonReporter.render(baseApp, Seq(issue()))
    out.count(_ == '{') shouldBe out.count(_ == '}')
  }

  // ── HtmlReporter ─────────────────────────────────────────────────────────────

  "HtmlReporter" should "start with <!DOCTYPE html>" in {
    HtmlReporter.render(baseApp, Nil).trim should startWith("<!DOCTYPE html>")
  }

  it should "contain the app name" in {
    HtmlReporter.render(baseApp, Nil) should include("MySparkJob")
  }

  it should "show no-issues message when empty" in {
    HtmlReporter.render(baseApp, Nil) should include("No issues detected")
  }

  it should "include the issue title HTML-escaped in the output" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(title = "Skew in Stage 0")))
    out should include("Skew in Stage 0")
  }

  it should "HTML-escape < and > in issue titles" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(title = "<script>alert(1)</script>")))
    out should not include "<script>"
    out should include("&lt;script&gt;")
  }

  it should "HTML-escape & in issue titles" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(title = "A & B problem")))
    out should include("A &amp; B problem")
  }

  it should "include the severity badge label" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(severity = Critical)))
    out should include("CRITICAL")
  }

  it should "include config_fix in a pre block" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(configFix = Some("spark.foo=bar"))))
    out should include("spark.foo=bar")
    out should include("<pre")
  }

  it should "show health score in the score card" in {
    val out = HtmlReporter.render(baseApp, Seq(issue(severity = Critical)))
    out should include("75")
  }

  it should "not include any external http resources in the style block" in {
    val html = HtmlReporter.render(baseApp, Seq(issue()))
    val styleSection = html.split("<style>").lift(1).flatMap(_.split("</style>").headOption).getOrElse("")
    styleSection should not include "http"
  }
}
