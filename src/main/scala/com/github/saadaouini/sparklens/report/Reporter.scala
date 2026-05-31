package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int = {
    // Each severity category has a per-category cap so that a flood of config warnings
    // (which fire on nearly every job) doesn't kill the score the way real criticals do.
    // Critical: −30 pts each, cap −100 (was −25 / −100)
    // Warning:  −10 pts each, cap  −25 (was −10 / −30)
    // Info:      −2 pts each, cap  −10 (was  −3 / −15)
    // With these weights, 1 Critical (−30 → score 70) is worse than 3 Warnings
    // capped at −25 (→ score 75), which was previously inverted.
    val critDeduct = math.min(issues.count(_.severity.order == 0) * 30, 100)
    val warnDeduct = math.min(issues.count(_.severity.order == 1) * 10,  25)
    val infoDeduct = math.min(issues.count(_.severity.order == 2) * 2,   10)
    math.max(0, 100 - critDeduct - warnDeduct - infoDeduct)
  }

  protected def writeOrPrint(content: String, path: Option[String]): Unit =
    path match {
      case Some(p) =>
        val stream: OutputStream =
          if (p.contains("://")) {
            // Remote path (hdfs://, s3://, gs://, …) — delegate to Hadoop FileSystem.
            // hadoop-client is a transitive provided dep of spark-core; it is always
            // present at runtime on any Spark cluster.
            val hadoopPath = new org.apache.hadoop.fs.Path(p)
            val fs = org.apache.hadoop.fs.FileSystem.get(
              hadoopPath.toUri, new org.apache.hadoop.conf.Configuration())
            fs.create(hadoopPath, /* overwrite= */ true)
          } else {
            new FileOutputStream(p)
          }
        val out = new OutputStreamWriter(stream, StandardCharsets.UTF_8)
        try { out.write(content) } finally { out.close() }
      case None =>
        print(content)
    }
}
