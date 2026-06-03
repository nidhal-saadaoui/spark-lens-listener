package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

import java.io.{FileOutputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

trait Reporter {
  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit

  protected def healthScore(issues: Seq[Issue]): Int = {
    // Collapse issues sharing a root cause (linked via relatedIds) into clusters
    // before scoring. Fixing one root cause typically resolves all issues in a
    // cluster — scoring them independently inflates the penalty for single-cause
    // scenarios (e.g. coalesce(1) causing spill + low-CPU + single-task).
    // Critical: −30 pts, Warning: −10 pts, Info: −2 pts, floored at 0.
    val representative = issueClusterGroups(issues).map(_.minBy(_.severity.order))
    val deduct = representative.count(_.severity.order == 0) * 30 +
                 representative.count(_.severity.order == 1) * 10 +
                 representative.count(_.severity.order == 2) * 2
    math.max(0, 100 - deduct)
  }

  // Union-find: groups issues sharing relatedIds into connected components.
  // Returns one Seq per cluster; singletons are their own cluster of size 1.
  protected def issueClusterGroups(issues: Seq[Issue]): Seq[Seq[Issue]] = {
    if (issues.isEmpty) return Nil
    val parent = scala.collection.mutable.Map(issues.map(i => i.id -> i.id): _*)
    def find(x: String): String = {
      val p = parent.getOrElse(x, x)
      if (p == x) x else { val r = find(p); parent(x) = r; r }
    }
    def union(a: String, b: String): Unit = {
      val ra = find(a); val rb = find(b)
      if (ra != rb) parent(ra) = rb
    }
    issues.foreach { i =>
      i.relatedIds.filter(parent.contains).foreach(union(i.id, _))
    }
    issues.groupBy(i => find(i.id)).values.toSeq
  }

  // Deduplicated total savings: for each root-cause cluster take the MAX savings
  // (not the sum), then sum across clusters, capped at app duration.
  // This prevents the total from exceeding reality when one fix resolves many issues.
  protected def deduplicatedSavingsMs(issues: Seq[Issue], appDurationMs: Option[Long]): Option[Long] = {
    val clusterMaxes = issueClusterGroups(issues).flatMap { cluster =>
      val savings = cluster.flatMap(_.estimatedImpact.flatMap(_.savedTimeMs))
      if (savings.nonEmpty) Some(savings.max) else None
    }
    if (clusterMaxes.isEmpty) None
    else {
      val total  = clusterMaxes.sum
      val capped = appDurationMs.map(d => math.min(total, d)).getOrElse(total)
      Some(capped)
    }
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
