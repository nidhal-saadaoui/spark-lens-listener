package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.report._
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart,
}

import java.util.logging.{Level, Logger}

/**
 * Zero-config Spark performance analyzer.
 *
 * Register via spark.extraListeners:
 * {{{
 *   spark-submit \
 *     --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
 *     --conf spark.sparklens.output=text \
 *     myJob.jar
 * }}}
 *
 * Configuration (all optional, all prefixed with spark.sparklens.*):
 *
 *   output            — Comma-separated list of formats: off | text | json | html | log
 *                       Examples: "text"  "text,json"  "log,json"
 *                       (default: off — silent unless fail.on is set)
 *
 *   report.path       — Base path for all formats.  When multiple formats are active
 *                       each gets its own extension: .txt .json .html .log
 *                       A format-specific path takes priority:
 *                         spark.sparklens.report.path.json=hdfs://bucket/report.json
 *                         spark.sparklens.report.path.text=/tmp/report.txt
 *                       Omit to write to driver stdout (log mode writes to driver logger).
 *
 *   fail.on           — critical | warning | info | none
 *                       Throw at app end if issues at this severity or above are found.
 */
class SparkLensListener(conf: SparkConf) extends SparkListener {

  // Secondary no-arg constructor for older Spark versions that don't pass SparkConf
  def this() = this(new SparkConf())

  private val log = Logger.getLogger(classOf[SparkLensListener].getName)

  // Parse comma-separated output list; normalise and deduplicate.
  private val outputFormats: Set[String] =
    conf.get("spark.sparklens.output", "off")
      .split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet

  private val failOn = conf.getOption("spark.sparklens.fail.on").map(_.toLowerCase)

  private val builder = new SparkAppModelBuilder(SPARK_VERSION)

  // ── Path resolution ────────────────────────────────────────────────────────
  // Priority: spark.sparklens.report.path.<format>
  //         → spark.sparklens.report.path + .<ext>  (when multiple formats active)
  //         → spark.sparklens.report.path            (when single format, for compat)
  //         → None (stdout / logger)

  private val basePath: Option[String] = conf.getOption("spark.sparklens.report.path")

  private def pathFor(format: String): Option[String] = {
    val perFormat = conf.getOption(s"spark.sparklens.report.path.$format")
    perFormat.orElse {
      basePath.map { base =>
        val activeNonOff = outputFormats - "off"
        if (activeNonOff.size <= 1) base               // single format: use path as-is
        else s"$base.${extensionFor(format)}"           // multi-format: append extension
      }
    }
  }

  private def extensionFor(format: String): String = format match {
    case "json" => "json"
    case "html" => "html"
    case "log"  => "log"
    case _      => "txt"
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  override def onApplicationStart(e: SparkListenerApplicationStart): Unit = {
    builder.onApplicationStart(e)
    val activeFormats = (outputFormats - "off").mkString(",")
    val pathInfo = basePath.map(p => s", path=$p").getOrElse("")
    val failInfo = failOn.map(f => s", fail.on=$f").getOrElse("")
    val fmt = if (activeFormats.nonEmpty) activeFormats else "off"
    log.info(s"spark-lens attached (output=$fmt$pathInfo$failInfo)")
  }

  override def onEnvironmentUpdate(e: SparkListenerEnvironmentUpdate): Unit =
    builder.onEnvironmentUpdate(e)

  override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit =
    builder.onExecutorAdded(e)

  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit =
    builder.onExecutorRemoved(e)

  override def onJobStart(e: SparkListenerJobStart): Unit = {
    builder.onJobStart(e)
    Option(e.properties)
      .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
      .flatMap(id => scala.util.Try(id.toLong).toOption)
      .foreach(builder.linkSqlJob(_, e.jobId))
  }

  override def onJobEnd(e: SparkListenerJobEnd): Unit =
    builder.onJobEnd(e)

  override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit =
    builder.onStageSubmitted(e)

  override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
    builder.onTaskEnd(e)

  override def onStageCompleted(e: SparkListenerStageCompleted): Unit =
    builder.onStageCompleted(e)

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      builder.onSqlExecutionStart(
        e.executionId, e.description, e.physicalPlanDescription, e.sparkPlanInfo, e.time)
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      builder.onSqlPlanUpdate(e.executionId, e.sparkPlanInfo)
    case e: SparkListenerSQLExecutionEnd =>
      builder.onSqlExecutionEnd(e.executionId, e.time)
    case _ =>
  }

  // ── Application end: run analysis and emit all requested formats ──────────

  override def onApplicationEnd(e: SparkListenerApplicationEnd): Unit = {
    val activeFormats = outputFormats - "off"
    if (activeFormats.isEmpty && failOn.isEmpty) return

    val app    = builder.build(e.time)
    val issues = Analyzers.runAll(app)

    activeFormats.foreach { format =>
      try {
        emitFormat(format, app, issues)
      } catch {
        case ex: Exception =>
          log.log(Level.WARNING,
            s"spark-lens: failed to write '$format' report: ${ex.getMessage}", ex)
      }
    }

    failOn.foreach { severity =>
      val threshold = Map("critical" -> 0, "warning" -> 1, "info" -> 2).getOrElse(severity, -1)
      if (threshold >= 0) {
        val blocking = issues.filter(_.severity.order <= threshold)
        if (blocking.nonEmpty) {
          val lines = blocking.take(5)
            .map(i => s"  [${i.severity.toString.toUpperCase}] ${i.title}")
            .mkString("\n")
          val tail = if (blocking.size > 5) s"\n  ... and ${blocking.size - 5} more" else ""
          throw new RuntimeException(
            s"spark-lens: ${blocking.size} issue(s) at '$severity' severity or above:\n" +
            s"$lines$tail\nSet spark.sparklens.output=text to see the full report."
          )
        }
      }
    }
  }

  private def emitFormat(
    format: String,
    app:    model.SparkAppModel,
    issues: Seq[model.Issue],
  ): Unit = {
    val path = pathFor(format)
    format match {
      case "json" => JsonReporter.write(app, issues, path)
      case "html" => HtmlReporter.write(app, issues, path)
      case "log"  =>
        if (path.isDefined) {
          LogReporter.write(app, issues, path)
        } else {
          // No path → print directly to stdout. Using java.util.logging was unreliable:
          // when Spark's JUL-to-SLF4J bridge is active the lines enter the log4j pipeline
          // where most cluster configs filter out com.github.saadaouini.sparklens and the
          // messages are silently dropped. Driver stdout is always captured by YARN, K8s,
          // Databricks, and EMR regardless of logging configuration.
          LogReporter.renderLines(app, issues).foreach { case (_, line) => println(line) }
          System.out.flush()
        }
      case _ => TextReporter.write(app, issues, path)
    }
  }
}
