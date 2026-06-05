package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

object UdfAnalyzer extends Analyzer {

  private val PythonUdfNodes  = Seq("PythonUDF", "BatchEvalPython", "ArrowEvalPython")
  private val ScalaUdfMarkers = Seq("UDF(", "UDF[")

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.sqlExecutions.values.toSeq.flatMap { sql =>
      val plan = sql.physicalPlan
      val desc = sql.description

      val hasPythonText = PythonUdfNodes.exists(plan.contains)
      val hasPythonTree = sql.planTree.exists(tree =>
        PythonUdfNodes.exists(n => tree.nodesContaining(n).nonEmpty))
      val hasScalaText  = ScalaUdfMarkers.exists(plan.contains)

      if (!(hasPythonText || hasPythonTree || hasScalaText)) Nil
      else {
        val udfKind = if (hasPythonText || hasPythonTree) "Python" else "Scala"
        Seq(Issue(
          id              = s"plan-udf-${sql.executionId}",
          severity        = Warning,
          category        = "plan",
          title           = s"""${udfKind} UDF Detected in "${desc.take(80)}" — Consider Native Functions""",
          description     =
            s"A ${udfKind} UDF was found in the physical plan. UDFs bypass Catalyst optimisations " +
            "(predicate pushdown, column pruning, Tungsten encoding)" +
            (if (udfKind == "Python") " and require row-by-row serialisation across the JVM/Python boundary." else "."),
          recommendation  =
            "Rewrite using native org.apache.spark.sql.functions.* where possible. " +
            "For Python UDFs that cannot be rewritten, call df.repartition(n) before the UDF " +
            "so each partition fits in memory and Python worker startup cost is amortised.",
          codeFix         = Some(
            "// Replace Python UDF with native equivalent:\n" +
            "df.withColumn(\"doubled\", col(\"x\") * 2)  // instead of udf(_ * 2)"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        ))
      }
    }
}
