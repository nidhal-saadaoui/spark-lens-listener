package com.github.saadaouini.sparklens

import com.github.saadaouini.sparklens.report._
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

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
 *   output       — off | text | json | html   (default: off)
 *   report.path  — local path or hdfs:// path to write the report file
 *                  omit to write to driver stdout
 *   fail.on      — critical | warning | info | none
 *                  throws RuntimeException at app end if blocking issues found (default: none)
 */
class SparkLensListener(conf: SparkConf) extends SparkListener {

  // Secondary no-arg constructor for older Spark versions that don't pass SparkConf
  def this() = this(new SparkConf())

  private val log = Logger.getLogger(classOf[SparkLensListener].getName)

  private val outputMode = conf.get("spark.sparklens.output", "off").toLowerCase
  private val reportPath = conf.getOption("spark.sparklens.report.path")
  private val failOn     = conf.getOption("spark.sparklens.fail.on").map(_.toLowerCase)

  private val builder = new SparkAppModelBuilder(SPARK_VERSION)

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  override def onApplicationStart(e: SparkListenerApplicationStart): Unit =
    builder.onApplicationStart(e)

  override def onEnvironmentUpdate(e: SparkListenerEnvironmentUpdate): Unit =
    builder.onEnvironmentUpdate(e)

  override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit =
    builder.onExecutorAdded(e)

  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit =
    builder.onExecutorRemoved(e)

  override def onJobStart(e: SparkListenerJobStart): Unit =
    builder.onJobStart(e)

  override def onJobEnd(e: SparkListenerJobEnd): Unit =
    builder.onJobEnd(e)

  override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit =
    builder.onStageSubmitted(e)

  override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
    builder.onTaskEnd(e)

  override def onStageCompleted(e: SparkListenerStageCompleted): Unit =
    builder.onStageCompleted(e)

  // SQL plan events — captured via the generic onOtherEvent hook
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      builder.onSqlExecutionStart(e.executionId, e.description, e.physicalPlanDescription, e.time)
    case e: SparkListenerSQLExecutionEnd =>
      builder.onSqlExecutionEnd(e.executionId, e.time)
    case _ =>
  }

  // ── Application end: run analysis and emit report ─────────────────────────

  override def onApplicationEnd(e: SparkListenerApplicationEnd): Unit = {
    if (outputMode == "off" && failOn.isEmpty) return

    val app    = builder.build(e.time)
    val issues = Analyzers.runAll(app)

    if (outputMode != "off") {
      val reporter: Reporter = outputMode match {
        case "json" => JsonReporter
        case "html" => HtmlReporter
        case _      => TextReporter
      }
      try {
        reporter.write(app, issues, reportPath)
      } catch {
        case ex: Exception =>
          log.log(Level.WARNING, s"spark-lens: failed to write report: ${ex.getMessage}", ex)
      }
    }

    failOn.foreach { severity =>
      val threshold = Map("critical" -> 0, "warning" -> 1, "info" -> 2).getOrElse(severity, -1)
      if (threshold >= 0) {
        val blocking = issues.filter(_.severity.order <= threshold)
        if (blocking.nonEmpty) {
          val summary = TextReporter.renderString(app, issues)
          throw new RuntimeException(
            s"spark-lens: found ${blocking.size} issue(s) at '$severity' or above.\n$summary"
          )
        }
      }
    }
  }
}
