package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Unit tests for SparkLensMatchers — no Spark context needed.
 *
 *  Uses hand-crafted SparkLensResult objects to verify every matcher
 *  fires correctly in both pass and fail directions, and that failure
 *  messages are informative.
 */
class SparkLensMatchersSpec extends AnyFlatSpec with SparkLensMatchers {

  private val emptyApp = SparkAppModel(
    appId = "test-app", appName = "test", sparkVersion = "3.5.0",
    startTimeMs = 0L, endTimeMs = Some(1000L),
    sparkProperties = Map.empty, jobs = Map.empty,
    stages = Map.empty, executors = Map.empty, sqlExecutions = Map.empty,
  )

  private val noIssues: SparkLensResult = SparkLensResult(emptyApp, Nil)

  private def resultWith(issues: Issue*): SparkLensResult =
    SparkLensResult(emptyApp, issues)

  private def issue(id: String, sev: Severity, cat: String): Issue =
    Issue(id, sev, cat, s"title-$id", "desc", "rec")

  // ── haveIssue ─────────────────────────────────────────────────────────────

  "haveIssue" should "pass when issue id matches exactly" in {
    resultWith(issue("spill-3", Warning, "spill")) should haveIssue("spill-3")
  }

  it should "pass when issue id has the given prefix" in {
    resultWith(issue("plan-cartesian-5", Warning, "plan")) should haveIssue("plan-cartesian")
  }

  it should "fail when no matching issue is present" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      noIssues should haveIssue("spill")
    }
    ex.getMessage should include("Expected issue 'spill'")
    ex.getMessage should include("but got:")
  }

  it should "support negation" in {
    noIssues should not(haveIssue("spill"))
  }

  it should "fail negation when issue is present" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      resultWith(issue("spill-1", Critical, "spill")) should not(haveIssue("spill-1"))
    }
    ex.getMessage should include("Did not expect issue 'spill-1'")
  }

  // ── haveIssueOfCategory ───────────────────────────────────────────────────

  "haveIssueOfCategory" should "pass when a matching category is present" in {
    resultWith(issue("gc-2", Warning, "gc")) should haveIssueOfCategory("gc")
  }

  it should "fail when category is absent" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      resultWith(issue("spill-1", Warning, "spill")) should haveIssueOfCategory("gc")
    }
    ex.getMessage should include("category 'gc'")
  }

  it should "support negation" in {
    resultWith(issue("spill-1", Warning, "spill")) should not(haveIssueOfCategory("gc"))
  }

  // ── haveIssueOfSeverity ───────────────────────────────────────────────────

  "haveIssueOfSeverity" should "pass for Critical" in {
    resultWith(issue("spill-1", Critical, "spill")) should haveIssueOfSeverity(Critical)
  }

  it should "pass for Warning" in {
    resultWith(issue("gc-1", Warning, "gc")) should haveIssueOfSeverity(Warning)
  }

  it should "pass for Info" in {
    resultWith(issue("cfg-1", Info, "config")) should haveIssueOfSeverity(Info)
  }

  it should "fail when no issue of that severity" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      resultWith(issue("cfg-1", Info, "config")) should haveIssueOfSeverity(Critical)
    }
    ex.getMessage should include("CRITICAL")
  }

  it should "support negation" in {
    resultWith(issue("cfg-1", Info, "config")) should not(haveIssueOfSeverity(Critical))
  }

  // ── haveNoIssuesOfSeverity ────────────────────────────────────────────────

  "haveNoIssuesOfSeverity" should "pass when no issues at that severity" in {
    resultWith(issue("cfg-1", Info, "config")) should haveNoIssuesOfSeverity(Critical)
    noIssues should haveNoIssuesOfSeverity(Warning)
  }

  it should "fail when a matching issue exists" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      resultWith(issue("spill-1", Critical, "spill")) should haveNoIssuesOfSeverity(Critical)
    }
    ex.getMessage should include("spill-1")
  }

  // ── haveHealthScoreAbove / Below ──────────────────────────────────────────

  "haveHealthScoreAbove" should "pass when score exceeds threshold" in {
    noIssues should haveHealthScoreAbove(90)  // perfect score = 100
  }

  it should "fail when score is at or below threshold" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      noIssues should haveHealthScoreAbove(100)  // 100 > 100 is false
    }
    ex.getMessage should include("100")
  }

  it should "reflect deductions from Critical issues" in {
    val withCrit = resultWith(issue("spill-1", Critical, "spill"))
    withCrit should haveHealthScoreBelow(80)  // −30 → score = 70
  }

  "haveHealthScoreBelow" should "pass when score is below threshold" in {
    resultWith(issue("spill-1", Critical, "spill")) should haveHealthScoreBelow(80)
  }

  it should "fail when score is at or above threshold" in {
    val ex = intercept[org.scalatest.exceptions.TestFailedException] {
      noIssues should haveHealthScoreBelow(50)  // 100 < 50 is false
    }
    ex.getMessage should include("100")
  }

  // ── helper conversions ────────────────────────────────────────────────────

  "ByteOps" should "convert sizes correctly" in {
    (1L).GB shouldBe 1073741824L
    (1L).MB shouldBe 1048576L
    (1L).KB shouldBe 1024L
  }

  "TimeOps" should "convert durations correctly" in {
    (1L).minutes shouldBe 60000L
    (30L).seconds shouldBe 30000L
  }

  // ── SparkLensResult accessors ─────────────────────────────────────────────

  "SparkLensResult" should "return issuesOfCategory correctly" in {
    val r = resultWith(
      issue("spill-1", Critical, "spill"),
      issue("gc-1",    Warning,  "gc"),
      issue("spill-2", Warning,  "spill"),
    )
    r.issuesOfCategory("spill") should have size 2
    r.issuesOfCategory("gc")    should have size 1
    r.issuesOfCategory("join")  shouldBe empty
  }

  it should "return issuesOfSeverity correctly" in {
    val r = resultWith(
      issue("spill-1", Critical, "spill"),
      issue("gc-1",    Warning,  "gc"),
      issue("cfg-1",   Info,     "config"),
    )
    r.issuesOfSeverity(Critical) should have size 1
    r.issuesOfSeverity(Warning)  should have size 1
    r.issuesOfSeverity(Info)     should have size 1
  }

  it should "compute healthScore using cluster deduplication" in {
    noIssues.healthScore shouldBe 100
    resultWith(issue("s-1", Critical, "spill")).healthScore shouldBe 70   // −30
    resultWith(issue("s-1", Warning,  "spill")).healthScore shouldBe 90   // −10
    resultWith(issue("s-1", Info,     "config")).healthScore shouldBe 98  // −2
  }
}
