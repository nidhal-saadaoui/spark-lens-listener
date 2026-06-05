package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

object HtmlReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(render(app, issues), path)

  private def e(s: String): String = s
    .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    .replace("\"", "&quot;")

  private def fmtMs(ms: Long): String =
    if (ms >= 3600000) f"${ms / 3600000.0}%.1fh"
    else if (ms >= 60000) f"${ms / 60000.0}%.1fm"
    else if (ms >= 1000)  f"${ms / 1000.0}%.1fs"
    else s"${ms}ms"

  private def fmtBytes(bytes: Long): String =
    if (bytes >= 1024L * 1024L * 1024L) f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1f GB"
    else if (bytes >= 1024L * 1024L) f"${bytes / (1024.0 * 1024.0)}%.1f MB"
    else if (bytes >= 1024L) f"${bytes / 1024.0}%.1f KB"
    else s"${bytes} B"

  private def severityClass(issue: Issue): String = issue.severity match {
    case Critical => "critical"
    case Warning  => "warning"
    case Info     => "info"
  }

  def render(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score    = healthScore(issues)
    val critical = issues.count(_.severity == Critical)
    val warning  = issues.count(_.severity == Warning)
    val info     = issues.count(_.severity == Info)

    // Metrics for dashboard
    val durationMs = app.durationMs.getOrElse(0L)
    val peakMems = app.stages.values.map(_.totalPeakExecutionMemory).toSeq
    val peakMemory = if (peakMems.isEmpty) 0L else peakMems.max
    val totalShuffleBytes = app.stages.values.map(_.totalShuffleBytesWritten).sum
    val totalGcMs = app.stages.values.map(_.totalGcTimeMs).sum

    val issueCards = issues.map { issue =>
      val cls   = severityClass(issue)
      val badge = issue.severity.label

      val impactBadge = issue.estimatedImpact.map { imp =>
        val timePart = imp.savedTimeMs.map(ms => s" &nbsp; ~${fmtMs(ms)} saved").getOrElse("")
        s"""<span class="impact-badge">${e(imp.confidence)} confidence$timePart</span>"""
      }.getOrElse("")

      val configBlock = issue.configFix.map(f =>
        s"""<div class="fix-label">config</div><pre class="fix-block">${e(f)}</pre>"""
      ).getOrElse("")

      val codeBlock = issue.codeFix.map(f =>
        s"""<div class="fix-label">code</div><pre class="fix-block">${e(f)}</pre>"""
      ).getOrElse("")

      val metricsBlock = if (issue.metrics.isEmpty) "" else {
        val rows = issue.metrics.map { case (k, v) =>
          s"""<tr><td class="mkey">${e(k)}</td><td class="mval">${e(v)}</td></tr>"""
        }.mkString("\n")
        s"""<div class="fix-label">metrics</div><table class="metrics-table">$rows</table>"""
      }

      val stagePills = if (issue.affectedStages.nonEmpty) {
        val callSites = issue.affectedStages.flatMap { sid =>
          app.stages.get(sid).map(s => sid -> s.callSite).filter(_._2.nonEmpty)
        }.toMap
        "<div class=\"locations\">" +
          issue.affectedStages.map { sid =>
            val tip = callSites.get(sid).map(cs => s""" title="${e(cs)}"""").getOrElse("")
            s"""<span class="pill"$tip>stage&nbsp;$sid</span>"""
          }.mkString(" ") +
          "</div>"
      } else ""

      val jobPills = if (issue.affectedJobs.nonEmpty)
        "<div class=\"locations\">" +
          issue.affectedJobs.map(j => s"""<span class="pill">job&nbsp;$j</span>""").mkString(" ") +
          "</div>"
      else ""

      s"""<details class="issue $cls" open>
         |  <summary>
         |    <span class="badge">$badge</span>
         |    <span class="issue-title">${e(issue.title)}</span>$impactBadge
         |  </summary>
         |  <div class="issue-body">
         |    <p class="desc">${e(issue.description)}</p>
         |    <p class="rec"><span class="arrow">&#8594;</span> ${e(issue.recommendation)}</p>
         |    $configBlock$codeBlock$metricsBlock$stagePills$jobPills
         |  </div>
         |</details>""".stripMargin
    }.mkString("\n")

    // ── Savings bar chart ────────────────────────────────────────────────────
    // Top issues with quantifiable savings — horizontal SVG bars, self-contained.
    val chartIssues = issues
      .filter(_.estimatedImpact.flatMap(_.savedTimeMs).exists(_ >= 1000L))
      .sortBy(i => -i.estimatedImpact.flatMap(_.savedTimeMs).getOrElse(0L))
      .take(10)

    val chart = if (chartIssues.isEmpty) "" else {
      val maxMs   = chartIssues.head.estimatedImpact.flatMap(_.savedTimeMs).getOrElse(1L).toDouble
      val rowH    = 36
      val labelW  = 280
      val barMaxW = 320
      val padH    = 12
      val totalH  = chartIssues.size * rowH + padH * 2
      val rows = chartIssues.zipWithIndex.map { case (iss, i) =>
        val ms      = iss.estimatedImpact.flatMap(_.savedTimeMs).getOrElse(0L)
        val barW    = math.max(4, (ms / maxMs * barMaxW).toInt)
        val y       = padH + i * rowH
        val midY    = y + rowH / 2
        val color   = if (iss.severity == Critical) "#e53e3e"
                      else if (iss.severity == Warning) "#F47920" else "#4a90d9"
        val label   = iss.title.take(42) + (if (iss.title.length > 42) "…" else "")
        val timeStr = fmtMs(ms)
        s"""<text x="${labelW - 8}" y="${midY + 5}" class="chart-lbl" text-anchor="end">${e(label)}</text>
           |  <rect x="$labelW" y="${y + 4}" width="$barW" height="${rowH - 8}" rx="3" fill="$color" opacity="0.85"/>
           |  <text x="${labelW + barW + 6}" y="${midY + 5}" class="chart-val">$timeStr</text>""".stripMargin
      }.mkString("\n  ")
      s"""<div class="chart-wrap">
         |  <div class="chart-title">Estimated savings per run</div>
         |  <svg width="${labelW + barMaxW + 80}" height="$totalH" class="chart-svg">
         |  $rows
         |  </svg>
         |</div>""".stripMargin
    }

    // Metrics summary dashboard
    val metricsSummary = {
      val metrics = Seq(
        ("Duration", fmtMs(durationMs)),
        ("Peak Memory", fmtBytes(peakMemory)),
        ("Shuffle Bytes", fmtBytes(totalShuffleBytes)),
        ("GC Time", fmtMs(totalGcMs)),
        ("Stages", app.stages.size.toString),
        ("Tasks", app.stages.values.map(s => if (s.hasExactAggregates) s.exactTaskCount else s.tasks.size).sum.toString),
      )
      val metricCards = metrics.map { case (label, value) =>
        s"""<div class="card"><div class="card-val">${e(value.take(12))}</div><div class="card-lbl">$label</div></div>"""
      }.mkString("\n    ")
      s"""<div class="metrics-section">
         |    <div class="section-title">Job Metrics</div>
         |    <div class="cards">
         |      $metricCards
         |    </div>
         |  </div>""".stripMargin
    }

    // Stage timeline chart
    val stageTimeline = {
      val timedStages = app.stages.values.filter(s => s.submissionTimeMs.isDefined && s.completionTimeMs.isDefined).toSeq
      if (timedStages.isEmpty) ""
      else {
        val sortedStages = timedStages.sortBy(_.submissionTimeMs.getOrElse(0L))
        val earliest = sortedStages.head.submissionTimeMs.getOrElse(0L)
        val latest = sortedStages.map(_.completionTimeMs.getOrElse(0L)).max
        val timeRange = if (latest > earliest) latest - earliest else 1L
        val chartW = 700
        val chartH = sortedStages.size * 24 + 40

        val stages = sortedStages.zipWithIndex.map { case (stage, idx) =>
          val submitMs = stage.submissionTimeMs.getOrElse(earliest)
          val submitOffset = (((submitMs - earliest).toDouble / timeRange) * (chartW - 100)).toInt
          val endTime = stage.completionTimeMs.getOrElse(latest)
          val completeOffset = (((endTime - earliest).toDouble / timeRange) * (chartW - 100)).toInt
          val barW = math.max(2, completeOffset - submitOffset)
          val y = 30 + idx * 24
          val color = if (stage.totalGcTimeMs > stage.durationMs * 0.1) "#ef5350"
                      else if (stage.totalDiskSpillBytes > 0) "#ffa726" else "#29b6f6"
          s"""<rect x="$submitOffset" y="$y" width="$barW" height="20" rx="2" fill="$color" opacity="0.8"/>
             |      <text x="${submitOffset + barW / 2}" y="${y + 14}" class="chart-lbl" text-anchor="middle" font-size="10px" font-weight="bold">S${stage.stageId}</text>"""
        }.mkString("\n      ")

        s"""<div class="chart-wrap">
           |  <div class="chart-title">Stage Timeline</div>
           |  <svg width="$chartW" height="$chartH" class="chart-svg">
           |    <text x="0" y="20" class="chart-lbl" font-weight="bold">Stages</text>
           |    $stages
           |  </svg>
           |</div>""".stripMargin
      }
    }

    // Memory usage timeline
    val memoryTimeline = {
      val timedStages = app.stages.values.filter(_.completionTimeMs.isDefined).toSeq
      if (timedStages.isEmpty) ""
      else {
        val sortedStages = timedStages.sortBy(_.completionTimeMs.getOrElse(0L))
        val submitTimes = app.stages.values.flatMap(_.submissionTimeMs)
        val earliest = if (submitTimes.isEmpty) 0L else submitTimes.min
        val latest = sortedStages.map(_.completionTimeMs.getOrElse(0L)).max
        val timeRange = if (latest > earliest) latest - earliest else 1L
        val maxMem = timedStages.map(_.totalPeakExecutionMemory).max
        val chartW = 700
        val chartH = 200

        val dataPoints: Seq[(Int, Int, Long)] = sortedStages.map { stage =>
          val endMs: Long = stage.completionTimeMs.getOrElse(latest)
          val x: Int = (((endMs - earliest).toDouble / timeRange) * chartW).toInt
          val y: Int = chartH - ((stage.totalPeakExecutionMemory.toDouble / maxMem) * (chartH - 40)).toInt
          val mem: Long = stage.totalPeakExecutionMemory
          (x, y, mem)
        }

        val points = dataPoints.map { case (x, y, _) => s"$x,$y" }.mkString(" ")
        val polyline = if (dataPoints.nonEmpty) s"""<polyline points="$points" fill="none" stroke="#3b82f6" stroke-width="2" opacity="0.7"/>""" else ""

        val labels = dataPoints.zipWithIndex.filter(_._2 % math.max(1, dataPoints.size / 5) == 0).map { case ((x, y, mem: Long), _) =>
          s"""<text x="$x" y="${y - 5}" class="chart-lbl" font-size="10px" text-anchor="middle">${fmtBytes(mem)}</text>"""
        }.mkString("\n      ")

        s"""<div class="chart-wrap">
           |  <div class="chart-title">Memory Pressure Over Time</div>
           |  <svg width="$chartW" height="$chartH" class="chart-svg">
           |    $polyline
           |    $labels
           |  </svg>
           |</div>""".stripMargin
      }
    }

    // Shuffle metrics breakdown
    val shuffleMetrics = {
      val shuffleStages = app.stages.values.filter(_.totalShuffleBytesWritten > 0).toSeq
      if (shuffleStages.isEmpty) ""
      else {
        val sortedStages = shuffleStages.sortBy(_.stageId)
        val maxBytes = math.max(sortedStages.map(_.totalInputBytes).max, sortedStages.map(_.totalShuffleBytesWritten).max)
        val chartW = 700
        val barH = 20
        val chartH = sortedStages.size * 30 + 40

        val bars = sortedStages.zipWithIndex.map { case (stage, idx) =>
          val inW = (stage.totalInputBytes.toDouble / maxBytes * (chartW - 150)).toInt
          val shuffleW = (stage.totalShuffleBytesWritten.toDouble / maxBytes * (chartW - 150)).toInt
          val y = 30 + idx * 30

          s"""<rect x="150" y="$y" width="$inW" height="${barH / 2}" fill="#2563eb" opacity="0.6"/>
             |      <rect x="150" y="${y + barH / 2}" width="$shuffleW" height="${barH / 2}" fill="#dc2626" opacity="0.6"/>
             |      <text x="5" y="${y + barH - 3}" class="chart-lbl" font-size="10px">S${stage.stageId}</text>
             |      <text x="155" y="${y + barH + 12}" class="chart-val" font-size="9px">${fmtBytes(stage.totalInputBytes)}</text>"""
        }.mkString("\n      ")

        s"""<div class="chart-wrap">
           |  <div class="chart-title">Shuffle Metrics (Input vs Written)</div>
           |  <svg width="$chartW" height="$chartH" class="chart-svg">
           |    <text x="150" y="20" class="chart-lbl" font-weight="bold" font-size="10px">Input (blue) | Written (red)</text>
           |    $bars
           |  </svg>
           |</div>""".stripMargin
      }
    }

    // GC timeline showing GC pause events
    val gcTimeline = {
      val timedStages = app.stages.values.filter(s => s.completionTimeMs.isDefined && s.totalGcTimeMs > 0).toSeq
      if (timedStages.isEmpty) ""
      else {
        val sortedStages = timedStages.sortBy(_.completionTimeMs.getOrElse(0L))
        val submitTimes = app.stages.values.flatMap(_.submissionTimeMs)
        val earliest = if (submitTimes.isEmpty) 0L else submitTimes.min
        val latest = sortedStages.map(_.completionTimeMs.getOrElse(0L)).max
        val timeRange = if (latest > earliest) latest - earliest else 1L
        val maxGc = sortedStages.map(_.totalGcTimeMs).max
        val chartW = 700
        val chartH = 200

        val gcBars = sortedStages.map { stage =>
          val endMs: Long = stage.completionTimeMs.getOrElse(latest)
          val x: Int = (((endMs - earliest).toDouble / timeRange) * chartW).toInt
          val gcMs: Long = stage.totalGcTimeMs
          val h: Int = ((gcMs.toDouble / maxGc) * (chartH - 40)).toInt
          val y: Int = chartH - h
          val pct: Double = (gcMs.toDouble / stage.durationMs) * 100.0
          val color = if (pct > 20.0) "#dc2626" else if (pct > 10.0) "#f97316" else "#3b82f6"

          s"""<rect x="${math.max(2, x - 3)}" y="$y" width="6" height="$h" fill="$color" opacity="0.8"/>
             |      <title>Stage GC: ${fmtMs(gcMs)} (${f"$pct%.1f"}%)</title>"""
        }.mkString("\n      ")

        s"""<div class="chart-wrap">
           |  <div class="chart-title">GC Timeline (ms by completion time)</div>
           |  <svg width="$chartW" height="$chartH" class="chart-svg">
           |    $gcBars
           |  </svg>
           |</div>""".stripMargin
      }
    }

    // Issue severity timeline: when critical/warning issues occur in the job
    val severityTimeline = {
      val criticalIssues = issues.filter(_.severity == Critical).flatMap(_.affectedStages).distinct
      val warningIssues = issues.filter(_.severity == Warning).flatMap(_.affectedStages).distinct
      if (criticalIssues.isEmpty && warningIssues.isEmpty) ""
      else {
        val affectedStages = (criticalIssues ++ warningIssues).distinct
        val stageData = affectedStages.flatMap(sid => app.stages.get(sid)).filter(_.submissionTimeMs.isDefined)
        if (stageData.isEmpty) ""
        else {
          val submitTimes = app.stages.values.flatMap(_.submissionTimeMs)
          val earliest = if (submitTimes.isEmpty) 0L else submitTimes.min
          val latest = stageData.map(_.completionTimeMs.getOrElse(0L)).max
          val timeRange = if (latest > earliest) latest - earliest else 1L
          val chartW = 700
          val chartH = 80

          val severityBars = stageData.map { stage =>
            val submitMs: Long = stage.submissionTimeMs.getOrElse(earliest)
            val submitX: Int = (((submitMs - earliest).toDouble / timeRange) * (chartW - 100)).toInt
            val endMs: Long = stage.completionTimeMs.getOrElse(latest)
            val endX: Int = (((endMs - earliest).toDouble / timeRange) * (chartW - 100)).toInt
            val barW: Int = math.max(1, endX - submitX)
            val isCritical = criticalIssues.contains(stage.stageId)
            val color = if (isCritical) "#dc2626" else "#f59e0b"
            val label = if (isCritical) "Critical" else "Warning"

            s"""<rect x="$submitX" y="20" width="$barW" height="30" fill="$color" opacity="0.6"/>
               |      <text x="${submitX + barW / 2}" y="43" class="chart-lbl" text-anchor="middle" font-size="9px">$label</text>"""
          }.mkString("\n      ")

          s"""<div class="chart-wrap">
             |  <div class="chart-title">Issue Severity Timeline</div>
             |  <svg width="$chartW" height="$chartH" class="chart-svg">
             |    $severityBars
             |  </svg>
             |</div>""".stripMargin
        }
      }
    }

    val body = if (issues.isEmpty)
      metricsSummary + "\n" + stageTimeline + "\n" + memoryTimeline + "\n" + shuffleMetrics + "\n" + gcTimeline + """<p class="no-issues">&#10004; No issues detected.</p>"""
    else metricsSummary + "\n" + stageTimeline + "\n" + memoryTimeline + "\n" + shuffleMetrics + "\n" + gcTimeline + "\n" + severityTimeline + "\n" + chart + "\n" + issueCards

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="UTF-8">
       |<meta name="viewport" content="width=device-width,initial-scale=1">
       |<title>spark-lens &#8212; ${e(app.appName)}</title>
       |<style>
       |$css
       |</style>
       |</head>
       |<body>
       |<div class="wrap">
       |  <div class="logo-wrap">${if (logoB64.nonEmpty) s"""<img src="data:image/png;base64,$logoB64" alt="SparkLens Listener" class="logo"/>""" else "<h1>SparkLens</h1>"}</div>
       |  <div class="meta">${e(app.appName)} &nbsp;&middot;&nbsp; ${e(app.appId)} &nbsp;&middot;&nbsp; Spark ${e(app.sparkVersion)}</div>
       |  <div class="cards">
       |    <div class="card"><div class="card-val score">$score<span class="denom">/100</span></div><div class="card-lbl">Health Score</div></div>
       |    <div class="card"><div class="card-val crit">$critical</div><div class="card-lbl">Critical</div></div>
       |    <div class="card"><div class="card-val warn">$warning</div><div class="card-lbl">Warning</div></div>
       |    <div class="card"><div class="card-val info-n">$info</div><div class="card-lbl">Info</div></div>
       |  </div>
       |  $body
       |</div>
       |</body>
       |</html>""".stripMargin
  }

  // Base64-encoded logo loaded from the JAR classpath (self-contained report, no external assets)
  private val logoB64: String = {
    val is = getClass.getResourceAsStream("/sparklens-logo.png")
    if (is == null) ""
    else try {
      val buf = new java.io.ByteArrayOutputStream
      val arr = new Array[Byte](8192)
      var n   = is.read(arr)
      while (n > 0) { buf.write(arr, 0, n); n = is.read(arr) }
      java.util.Base64.getEncoder.encodeToString(buf.toByteArray)
    } finally { is.close() }
  }

  private val css: String =
    """  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f9fafb;color:#111}
  .logo-wrap{text-align:center;padding:24px 0 8px}
  .chart-wrap{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:16px 20px;margin:0 0 20px}
  .chart-title{font-size:13px;font-weight:600;color:#8a9bb5;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px}
  .chart-svg{display:block;overflow:visible}
  .chart-lbl{font-size:12px;fill:#3d5178;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
  .chart-val{font-size:11px;fill:#8a9bb5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
  .logo{height:64px;width:auto}
      |  .wrap{max-width:900px;margin:32px auto;padding:0 20px}
      |  h1{font-size:20px;margin:0 0 4px;font-weight:700}
      |  .meta{color:#6b7280;font-size:13px;margin-bottom:24px}
      |  .cards{display:flex;gap:12px;margin-bottom:28px;flex-wrap:wrap}
      |  .card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px 20px;min-width:120px;text-align:center}
      |  .card-val{font-size:28px;font-weight:800}
      |  .denom{font-size:14px;font-weight:400}
      |  .card-lbl{font-size:12px;color:#6b7280;margin-top:2px}
      |  .score{color:#111}.crit{color:#dc2626}.warn{color:#d97706}.info-n{color:#2563eb}
      |  details.issue{border-radius:6px;margin-bottom:10px;overflow:hidden}
      |  details.issue.critical{border:1px solid #dc262640;border-left:4px solid #dc2626;background:#fef2f2}
      |  details.issue.warning {border:1px solid #d9770640;border-left:4px solid #d97706;background:#fffbeb}
      |  details.issue.info    {border:1px solid #2563eb40;border-left:4px solid #2563eb;background:#eff6ff}
      |  summary{display:flex;align-items:center;gap:8px;padding:12px 16px;cursor:pointer;list-style:none;user-select:none}
      |  summary::-webkit-details-marker{display:none}
      |  summary::before{content:'\25B6';font-size:9px;color:#9ca3af;transition:transform .15s;flex-shrink:0;display:inline-block}
      |  details[open]>summary::before{transform:rotate(90deg)}
      |  .badge{font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;letter-spacing:.5px;color:#fff;flex-shrink:0}
      |  .critical .badge{background:#dc2626}
      |  .warning  .badge{background:#d97706}
      |  .info     .badge{background:#2563eb}
      |  .issue-title{font-weight:600;font-size:14px;color:#111}
      |  .issue-body{padding:0 16px 14px 42px}
      |  .desc{margin:0 0 6px;color:#374151;font-size:13px;line-height:1.5}
      |  .rec{margin:0 0 4px;color:#374151;font-size:13px;line-height:1.5}
      |  .arrow{font-weight:700;margin-right:4px}
      |  .fix-label{font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin:10px 0 3px}
      |  .fix-block{margin:0;background:#1e1e1e;color:#d4d4d4;padding:10px 12px;border-radius:4px;font-size:12px;overflow-x:auto;white-space:pre;line-height:1.5;font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace}
      |  .locations{margin-top:10px;display:flex;flex-wrap:wrap;gap:5px}
      |  .pill{font-size:11px;background:#fff;border:1px solid #d1d5db;border-radius:4px;padding:2px 7px;color:#374151;font-family:'SFMono-Regular',Consolas,monospace}
      |  .no-issues{color:#15803d;font-weight:600;font-size:15px}
      |  .impact-badge{font-size:11px;background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb;border-radius:4px;padding:2px 7px;margin-left:8px;font-weight:500}
      |  .metrics-table{border-collapse:collapse;margin:8px 0;font-size:12px;font-family:'SFMono-Regular',Consolas,monospace}
      |  .metrics-table .mkey{color:#6b7280;padding:2px 10px 2px 0;white-space:nowrap}
      |  .metrics-table .mval{color:#111;font-weight:600;padding:2px 0}
      |  .metrics-section{margin-bottom:28px}
      |  .section-title{font-size:14px;font-weight:700;color:#111;margin-bottom:12px}""".stripMargin
}
