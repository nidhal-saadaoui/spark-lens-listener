package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int = {
    // Linear deduction, no per-category caps, floored at 0.
    // Critical: −30 pts  (1 critical → 70, 3 criticals → 10)
    // Warning:  −10 pts  (5 warnings → 50)
    // Info:      −2 pts  (5 info     → 90)
    // Caps produced misleading results where 4+ Criticals looked identical to 1 Critical.
    val deduct = issues.count(_.severity.order == 0) * 30 +
                 issues.count(_.severity.order == 1) * 10 +
                 issues.count(_.severity.order == 2) * 2
    math.max(0, 100 - deduct)
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
