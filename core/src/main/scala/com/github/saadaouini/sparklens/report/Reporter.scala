package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int              = Scoring.healthScore(issues)
  protected def issueClusterGroups(issues: Seq[Issue])            = Scoring.issueClusterGroups(issues)
  protected def deduplicatedSavingsMs(issues: Seq[Issue], appDurationMs: Option[Long]) =
    Scoring.deduplicatedSavingsMs(issues, appDurationMs)

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
