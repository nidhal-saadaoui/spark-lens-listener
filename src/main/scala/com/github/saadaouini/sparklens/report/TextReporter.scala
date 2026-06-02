package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

object TextReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(renderString(app, issues), path)

  def renderString(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score = healthScore(issues)
    val sb    = new StringBuilder

    // ── Header ───────────────────────────────────────────────────────────────
    sb.append("\n")
    sb.append("=" * 70).append("\n")
    sb.append(s"  spark-lens  |  ${app.appName}  (${app.appId})\n")
    sb.append(s"  Spark ${app.sparkVersion.padTo(6, ' ')}  |  Health: $score/100")
    if (issues.nonEmpty) {
      val c = issues.count(_.severity == Critical)
      val w = issues.count(_.severity == Warning)
      val i = issues.count(_.severity == Info)
      val parts = Seq(
        if (c > 0) s"$c critical" else "",
        if (w > 0) s"$w warning"  else "",
        if (i > 0) s"$i info"     else "",
      ).filter(_.nonEmpty).mkString("  ·  ")
      sb.append(s"  |  $parts")
    }
    sb.append("\n")
    sb.append("=" * 70).append("\n")

    if (issues.isEmpty) {
      sb.append("  ✔  No issues detected.\n")
      sb.append("=" * 70).append("\n\n")
      return sb.toString()
    }

    def msLabel(ms: Long): String =
      if (ms >= 60000) f"${ms / 60000.0}%.1fmin"
      else if (ms >= 1000) f"${ms / 1000.0}%.1fs"
      else s"${ms}ms"

    // ── Priority Issues — top 3 by estimated savings ──────────────────────────
    val ranked = issues
      .filter(i => i.estimatedImpact.flatMap(_.savedTimeMs).exists(_ >= 1000L))
      .take(3)
    if (ranked.nonEmpty) {
      sb.append("\n  Priority fixes (estimated savings per run):\n")
      ranked.zipWithIndex.foreach { case (issue, idx) =>
        val saving = issue.estimatedImpact.flatMap(_.savedTimeMs)
          .map(ms => s"  ~${msLabel(ms)}").getOrElse("")
        sb.append(s"  ${idx + 1}. [${issue.severity.label}] ${issue.title.take(65)}$saving\n")
      }
      sb.append("\n")
    }

    // ── Minimum-impact filter ─────────────────────────────────────────────────
    // When there are many issues, hide Info items with no quantifiable impact
    // to keep the report readable. They remain in JSON output.
    val quietInfo = issues.filter { i =>
      i.severity == Info && i.estimatedImpact.flatMap(_.savedTimeMs).forall(_ < 1000L)
    }
    val displayIssues =
      if (issues.size > 5 && quietInfo.nonEmpty) issues.filterNot(quietInfo.toSet)
      else issues

    // ── Issue list ────────────────────────────────────────────────────────────
    // Format per issue:
    //   [icon]  Title  (~impact)
    //     →  config fix  (or recommendation if no config fix)
    //        one-sentence description
    //        code: ...  (if present)
    //        stages/jobs (if non-empty)

    def icon(issue: Issue): String = issue.severity match {
      case Critical => "✖ CRITICAL"
      case Warning  => "⚠ WARNING"
      case Info     => "ℹ INFO"
    }

    // Return the first sentence of a multi-sentence string (split on ". " or ".\n").
    def firstSentence(s: String): String = {
      val normalized = s.replace("\n", " ").trim
      val idx = normalized.indexOf(". ")
      if (idx > 0) normalized.substring(0, idx + 1)
      else if (normalized.length > 130) normalized.substring(0, 130) + "…"
      else normalized
    }

    // Impact suffix for the title line — only shown when confidence >= medium and time is known.
    def impactSuffix(issue: Issue): String =
      issue.estimatedImpact.flatMap { imp =>
        if (imp.confidence != "low" && imp.savedTimeMs.exists(_ > 0))
          Some(s"  (~${msLabel(imp.savedTimeMs.get)} per run)")
        else None
      }.getOrElse("")

    def indented(prefix: String, value: String): String = {
      val pad = " " * prefix.length
      value.split("\n").zipWithIndex.map {
        case (line, 0) => prefix + line
        case (line, _) => pad + line
      }.mkString("\n")
    }

    if (quietInfo.nonEmpty && displayIssues.size < issues.size) {
      sb.append(s"  (${quietInfo.size} low-impact info issue(s) hidden — use spark.sparklens.output=json to see all)\n\n")
    }

    displayIssues.foreach { issue =>
      val ic       = icon(issue)
      val impSufx  = impactSuffix(issue)
      sb.append(s"\n  [$ic]  ${issue.title}$impSufx\n")

      // Primary fix line: prefer config_fix (single-line), else recommendation
      val primaryFix = issue.configFix.map(_.linesIterator.next().split('#').head.trim)
      primaryFix match {
        case Some(fix) => sb.append(s"    fix:   $fix\n")
        case None      => sb.append(s"    fix:   ${issue.recommendation.linesIterator.next()}\n")
      }

      // One-sentence description
      sb.append(s"       ${firstSentence(issue.description)}\n")

      // Multi-line config fix (if it has more than one line after the first)
      issue.configFix.foreach { f =>
        val lines = f.split("\n")
        if (lines.length > 1)
          lines.drop(1).foreach(l => sb.append(s"       $l\n"))
      }

      // Code fix (if any)
      issue.codeFix.foreach(f => sb.append(indented("    code:  ", f)).append("\n"))

      // Affected stages / jobs
      val stages = issue.affectedStages
      val jobs   = issue.affectedJobs
      if (stages.nonEmpty) sb.append(s"    stages: ${stages.mkString(", ")}\n")
      if (jobs.nonEmpty)   sb.append(s"    jobs:   ${jobs.mkString(", ")}\n")

      sb.append("  · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·\n")
    }

    // ── Quick wins ────────────────────────────────────────────────────────────
    // Group issues by their primary config fix (first non-comment line).
    // Show each distinct fix once, listing which issues it resolves.
    val fixGroups: Seq[(String, Seq[Issue])] = displayIssues
      .filter(_.configFix.isDefined)
      .groupBy(i => i.configFix.get.linesIterator.next().split('#').head.trim)
      .toSeq
      .sortBy { case (_, grp) =>
        // Sort groups: most issues resolved first, then by severity
        (-grp.size, grp.map(_.severity.order).min)
      }

    if (fixGroups.nonEmpty) {
      sb.append("\n  ──────────────────────────────────────────────────────────────────\n")
      sb.append("  Quick wins\n")
      fixGroups.foreach { case (fix, grp) =>
        val ids    = grp.map(_.id).mkString(", ")
        val count  = grp.size
        val plural = if (count == 1) "issue" else "issues"
        sb.append(s"    $fix\n")
        sb.append(s"       resolves $count $plural: $ids\n")
      }
    }

    sb.append("\n")
    sb.append("=" * 70).append("\n\n")
    sb.toString()
  }
}
