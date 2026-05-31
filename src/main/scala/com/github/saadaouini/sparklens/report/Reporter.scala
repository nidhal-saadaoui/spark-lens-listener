package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int = {
    val deduct = issues.map(_.severity.order match {
      case 0 => 25
      case 1 => 10
      case _ => 3
    }).sum
    math.max(0, 100 - deduct)
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
