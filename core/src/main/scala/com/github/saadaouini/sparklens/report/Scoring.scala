package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.Issue

/** Public scoring utilities shared by Reporter and SparkLensResult (testing module). */
object Scoring {

  def healthScore(issues: Seq[Issue]): Int = {
    val representative = issueClusterGroups(issues).map(_.minBy(_.severity.order))
    val deduct = representative.count(_.severity.order == 0) * 30 +
                 representative.count(_.severity.order == 1) * 10 +
                 representative.count(_.severity.order == 2) * 2
    math.max(0, 100 - deduct)
  }

  def issueClusterGroups(issues: Seq[Issue]): Seq[Seq[Issue]] = {
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

  def deduplicatedSavingsMs(issues: Seq[Issue], appDurationMs: Option[Long]): Option[Long] = {
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
}
