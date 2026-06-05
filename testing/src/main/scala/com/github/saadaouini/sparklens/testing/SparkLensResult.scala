package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.model.{Issue, Severity, SparkAppModel}
import com.github.saadaouini.sparklens.report.{Scoring, TextReporter}

/** Result of an `analyse {}` block. Provides convenience accessors for assertions. */
case class SparkLensResult(app: SparkAppModel, issues: Seq[Issue]) {

  def healthScore: Int = Scoring.healthScore(issues)

  /** Full text report — included automatically in assertion failure messages. */
  def textReport: String = TextReporter.renderString(app, issues)

  def hasIssue(id: String): Boolean =
    issues.exists(i => i.id == id || i.id.startsWith(id + "-"))

  def hasIssueOfCategory(category: String): Boolean =
    issues.exists(_.category == category)

  def hasIssueOfSeverity(severity: Severity): Boolean =
    issues.exists(_.severity == severity)

  def issuesOfCategory(category: String): Seq[Issue] =
    issues.filter(_.category == category)

  def issuesOfSeverity(severity: Severity): Seq[Issue] =
    issues.filter(_.severity == severity)
}
