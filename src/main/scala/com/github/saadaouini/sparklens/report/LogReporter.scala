package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

/**
 * Log-format reporter — one line per event, structured key=value pairs,
 * suitable for Spark application logs, Splunk, ELK, Datadog, CloudWatch, etc.
 *
 * Output format (all lines prefixed with `[spark-lens]` for easy grep):
 * {{{
 * [spark-lens] SUMMARY  app="MyJob" app_id=app-123 spark=3.5.0 health=71 issues=5 critical=1 warning=2 info=2 duration=4m12s savings=12.3s/run
 * [spark-lens] CRITICAL id=spill-3          category=spill   title="Disk Spill in Stage 3 (join-stage)"                 savings=1.2s/run  fix="spark.sql.adaptive.enabled=true" stages=3
 * [spark-lens] WARNING  id=skew-warn-5      category=skew    title="Shuffle Hot-Key Skew in Stage 5"                    savings=3.1s/run  fix="spark.sql.adaptive.skewJoin.enabled=true" stages=5
 * [spark-lens] INFO     id=config-aqe       category=config  title="Adaptive Query Execution Disabled"                  fix="spark.sql.adaptive.enabled=true"
 * }}}
 *
 * When [[write]] is called without a report path, it writes to stdout — the driver
 * captures stdout in the application log automatically (YARN, K8s, Databricks).
 * [[renderLines]] returns (javaLogLevel, line) pairs for callers that prefer to
 * route through a Java logger directly.
 */
object LogReporter extends Reporter {

  private val Prefix   = "[spark-lens]"
  private val CritLbl  = "CRITICAL"
  private val WarnLbl  = "WARNING "
  private val InfoLbl  = "INFO    "

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(renderString(app, issues), path)

  def renderString(app: SparkAppModel, issues: Seq[Issue]): String =
    renderLines(app, issues).map(_._2).mkString("", "\n", "\n")

  /**
   * Returns (java.util.logging.Level, logLine) pairs so the listener can route
   * each line through the Java logger at the correct severity, placing spark-lens
   * output inline with Spark's own log stream.
   */
  def renderLines(app: SparkAppModel, issues: Seq[Issue]): Seq[(java.util.logging.Level, String)] = {
    import java.util.logging.Level._

    val buf = scala.collection.mutable.ArrayBuffer[(java.util.logging.Level, String)]()

    // ── Summary line ──────────────────────────────────────────────────────────
    val score = healthScore(issues)
    val c = issues.count(_.severity == Critical)
    val w = issues.count(_.severity == Warning)
    val i = issues.count(_.severity == Info)
    val totalSavingsMs = issues.flatMap(_.estimatedImpact.flatMap(_.savedTimeMs)).sum
    val savingsFld = if (totalSavingsMs > 0) s" savings=${msLabel(totalSavingsMs)}/run" else ""
    val durationFld = app.durationMs.map(ms => s" duration=${msLabel(ms)}").getOrElse("")
    val summaryLevel = if (c > 0) WARNING else if (w > 0) INFO else INFO

    val summaryLine =
      s"$Prefix SUMMARY  " +
      s"app=${q(app.appName)} app_id=${app.appId} spark=${app.sparkVersion}" +
      s" health=$score issues=${issues.size} critical=$c warning=$w info=$i" +
      s"$durationFld$savingsFld"
    buf += ((summaryLevel, summaryLine))

    // ── One line per issue ────────────────────────────────────────────────────
    issues.foreach { issue =>
      val (label, jLevel) = issue.severity match {
        case Critical => (CritLbl, WARNING)  // java.util.logging has no CRITICAL; WARNING is highest
        case Warning  => (WarnLbl, WARNING)
        case _        => (InfoLbl, INFO)
      }

      val savings = issue.estimatedImpact.flatMap(_.savedTimeMs)
        .map(ms => s" savings=${msLabel(ms)}/run").getOrElse("")

      val fix = issue.configFix
        .map(f => s" fix=${q(f.linesIterator.next().split('#').head.trim)}")
        .getOrElse("")

      // Include up to 3 key metrics as key=value
      val metricsStr = issue.metrics.toSeq.take(3)
        .map { case (k, v) => s"$k=${qIfNeeded(v)}" }
        .mkString(" ")
      val metricsFld = if (metricsStr.nonEmpty) s" $metricsStr" else ""

      val stagesFld = if (issue.affectedStages.nonEmpty)
        s" stages=${issue.affectedStages.mkString(",")}" else ""
      val jobsFld = if (issue.affectedJobs.nonEmpty)
        s" jobs=${issue.affectedJobs.mkString(",")}" else ""

      val line =
        s"$Prefix $label id=${issue.id}" +
        s" category=${issue.category}" +
        s" title=${q(issue.title.take(120))}" +
        s"$savings$fix$metricsFld$stagesFld$jobsFld"
      buf += ((jLevel, line))
    }

    buf.toSeq
  }

  // ── Formatting helpers ────────────────────────────────────────────────────

  private def msLabel(ms: Long): String = {
    import java.util.Locale
    if (ms >= 3600000) String.format(Locale.ROOT, "%.1fh", ms / 3600000.0: java.lang.Double)
    else if (ms >= 60000) String.format(Locale.ROOT, "%.1fm", ms / 60000.0: java.lang.Double)
    else if (ms >= 1000)  String.format(Locale.ROOT, "%.1fs", ms / 1000.0: java.lang.Double)
    else s"${ms}ms"
  }

  /** Quote a string with double-quotes, escaping inner quotes. */
  private def q(s: String): String = {
    val escaped = s.replace("\\", "\\\\").replace("\"", "'").replace("\n", " ")
    s""""$escaped""""
  }

  /** Quote only if the value contains spaces; otherwise pass through. */
  private def qIfNeeded(s: String): String =
    if (s.contains(' ') || s.contains('"')) q(s) else s
}
