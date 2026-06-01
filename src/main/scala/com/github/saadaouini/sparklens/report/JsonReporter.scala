package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{EstimatedImpact, Issue, SparkAppModel}

object JsonReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(render(app, issues), path)

  def render(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score  = healthScore(issues)

    val esc: String => String = _.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case c if c.toInt < 0x20 => f"\\u${c.toInt}%04x"
      case c    => c.toString
    }

    // ── EstimatedImpact — omit null fields ───────────────────────────────────
    def renderImpact(imp: Option[EstimatedImpact]): String = imp.map { i =>
      val fields = scala.collection.mutable.ArrayBuffer[String]()
      fields += s""""summary": "${esc(i.summary)}""""
      i.savedTimeMs.foreach(ms => fields += s""""saved_time_ms": $ms""")
      i.savedBytes.foreach(b   => fields += s""""saved_bytes": $b""")
      fields += s""""confidence": "${esc(i.confidence)}""""
      s"{${fields.mkString(", ")}}"
    }.getOrElse("null")

    // ── Fixes block — omit entirely when both are null ────────────────────────
    def renderFixes(configFix: Option[String], codeFix: Option[String]): String = {
      val parts = scala.collection.mutable.ArrayBuffer[String]()
      configFix.foreach(f => parts += s""""config": "${esc(f)}"""")
      codeFix.foreach(f   => parts += s""""code": "${esc(f)}"""")
      if (parts.isEmpty) "null" else s"{${parts.mkString(", ")}}"
    }

    // ── Top actions — group issues by shared config fix ───────────────────────
    // Normalize: strip trailing comments and whitespace from the first line of config_fix.
    def normalizeKey(fix: String): String =
      fix.linesIterator.next().split('#').head.trim

    case class TopAction(fix: String, resolves: Seq[String], savedMs: Option[Long])

    val topActions: Seq[TopAction] = issues
      .filter(_.configFix.isDefined)
      .groupBy(i => normalizeKey(i.configFix.get))
      .map { case (fixKey, grp) =>
        val totalMs: Option[Long] = {
          val vals = grp.flatMap(_.estimatedImpact.flatMap(_.savedTimeMs))
          if (vals.nonEmpty) Some(vals.sum) else None
        }
        TopAction(fixKey, grp.map(_.id).sorted, totalMs)
      }
      .toSeq
      .sortBy(a => (-a.resolves.size, -a.savedMs.getOrElse(0L)))

    val totalSavingsMs: Option[Long] = {
      val vals = issues.flatMap(_.estimatedImpact.flatMap(_.savedTimeMs))
      if (vals.nonEmpty) Some(vals.sum) else None
    }

    val topActionsJson = topActions.map { a =>
      val ids      = a.resolves.map(id => s""""${esc(id)}"""").mkString(", ")
      val savedMs  = a.savedMs.map(_.toString).getOrElse("null")
      s"""{"config": "${esc(a.fix)}", "resolves": [$ids], "estimated_savings_ms": $savedMs}"""
    }.mkString(",\n    ")

    // ── Issues ────────────────────────────────────────────────────────────────
    val issueJson = issues.map { i =>
      val metricsPart = if (i.metrics.isEmpty) ""
        else s""",\n    "metrics": {${i.metrics.map { case (k, v) => s""""${esc(k)}": "${esc(v)}"""" }.mkString(", ")}}"""
      val fixesPart   = renderFixes(i.configFix, i.codeFix)
      val impactPart  = renderImpact(i.estimatedImpact)

      s"""{
         |    "id": "${esc(i.id)}",
         |    "severity": "${i.severity.label.toLowerCase}",
         |    "category": "${esc(i.category)}",
         |    "title": "${esc(i.title)}",
         |    "description": "${esc(i.description)}",
         |    "recommendation": "${esc(i.recommendation)}",
         |    "fixes": $fixesPart,
         |    "affected_stages": [${i.affectedStages.mkString(",")}],
         |    "affected_jobs": [${i.affectedJobs.mkString(",")}]$metricsPart,
         |    "estimated_impact": $impactPart
         |  }""".stripMargin
    }.mkString(",\n  ")

    val totalMs = totalSavingsMs.map(_.toString).getOrElse("null")
    val topActPart = if (topActions.isEmpty) "[]"
                     else s"[\n    $topActionsJson\n  ]"

    s"""{
       |  "app_id": "${esc(app.appId)}",
       |  "app_name": "${esc(app.appName)}",
       |  "spark_version": "${esc(app.sparkVersion)}",
       |  "health_score": $score,
       |  "issue_count": ${issues.size},
       |  "total_estimated_savings_ms": $totalMs,
       |  "top_actions": $topActPart,
       |  "issues": [$issueJson]
       |}""".stripMargin
  }
}
