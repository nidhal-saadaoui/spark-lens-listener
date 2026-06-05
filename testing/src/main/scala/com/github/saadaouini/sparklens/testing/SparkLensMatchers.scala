package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.model.Severity
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.matchers.should.Matchers

trait SparkLensMatchers extends Matchers {

  def haveIssue(id: String): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        result.hasIssue(id),
        s"Expected issue '$id' but got: ${result.issues.map(_.id).mkString(", ")}",
        s"Did not expect issue '$id' but it was present",
      )
    }

  def haveIssueOfCategory(category: String): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        result.hasIssueOfCategory(category),
        s"Expected an issue of category '$category' but got categories: ${result.issues.map(_.category).distinct.mkString(", ")}",
        s"Did not expect any issue of category '$category' but one was present",
      )
    }

  def haveIssueOfSeverity(severity: Severity): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        result.hasIssueOfSeverity(severity),
        s"Expected an issue of severity '${severity.label}' but none found",
        s"Did not expect any issue of severity '${severity.label}' but one was present",
      )
    }

  def haveNoIssuesOfSeverity(severity: Severity): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        !result.hasIssueOfSeverity(severity),
        s"Expected no issues of severity '${severity.label}' but found: ${result.issuesOfSeverity(severity).map(_.id).mkString(", ")}",
        s"Expected at least one issue of severity '${severity.label}' but none found",
      )
    }

  def haveHealthScoreAbove(n: Int): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        result.healthScore > n,
        s"Expected health score > $n but got ${result.healthScore}",
        s"Expected health score <= $n but got ${result.healthScore}",
      )
    }

  def haveHealthScoreBelow(n: Int): Matcher[SparkLensResult] =
    Matcher { result =>
      MatchResult(
        result.healthScore < n,
        s"Expected health score < $n but got ${result.healthScore}",
        s"Expected health score >= $n but got ${result.healthScore}",
      )
    }

  implicit class ByteOps(n: Long) {
    def GB: Long = n * 1024L * 1024L * 1024L
    def MB: Long = n * 1024L * 1024L
    def KB: Long = n * 1024L
  }

  implicit class TimeOps(n: Long) {
    def seconds: Long = n * 1000L
    def minutes: Long = n * 60000L
  }
}
