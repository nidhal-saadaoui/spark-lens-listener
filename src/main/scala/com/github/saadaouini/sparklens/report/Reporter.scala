package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int = {
    // Each severity category has a per-category cap so that a flood of config warnings
    // (which fire on nearly every job) doesn't kill the score the way real criticals do.
    val critDeduct = math.min(issues.count(_.severity.order == 0) * 25, 100)
    val warnDeduct = math.min(issues.count(_.severity.order == 1) * 10,  30)
    val infoDeduct = math.min(issues.count(_.severity.order == 2) * 3,   15)
    math.max(0, 100 - critDeduct - warnDeduct - infoDeduct)
  }

  protected def writeOrPrint(content: String, path: Option[String]): Unit =
    path match {
      case Some(p) =>
        val out = new OutputStreamWriter(new FileOutputStream(p), StandardCharsets.UTF_8)
        try { out.write(content) } finally { out.close() }
      case None =>
        print(content)
    }
}
