package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._
import ImpactEstimator._

// All checks below match against Spark's FORMATTED physical plan description (Spark 3.x+).
// Node type names (CartesianProduct, SortMergeJoin, etc.) are stable identifiers in the
// Catalyst plan tree and unlikely to change. The CBO statistics strings ("Statistics(sizeInBytes=",
// "rowCount=") and the FORMATTED layout used for window detection (the "\n\n(" detail separator)
// are presentation-layer strings tied to Spark's explain format — fragile across Spark versions.
object PlanAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] =
    app.sqlExecutions.values.toSeq.flatMap { sql =>
      val plan   = sql.physicalPlan
      val desc   = sql.description
      val issues = scala.collection.mutable.ArrayBuffer[Issue]()

      if (plan.contains("CartesianProduct")) {
        issues += Issue(
          id              = s"plan-cartesian-${sql.executionId}",
          severity        = Critical,
          category        = "plan",
          title           = s"""Cartesian Product in "${desc.take(80)}"""",
          description     = "A CartesianProduct node was found in the physical plan. This produces M×N rows and scales quadratically with dataset size.",
          recommendation  = "Add an explicit join condition. If a cross join is intentional on small data, use df1.crossJoin(df2) to make the intent clear and ensure both sides are small.",
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      // Text-based detection: for unpartitioned Window the child Exchange SinglePartition
      // appears AFTER "Window" in the tree section; for partitioned windows the
      // SinglePartition exchange belongs to the outer aggregation and appears BEFORE.
      val windowTextFires: Boolean = if (plan.contains("Window")) {
        val treePlan = treeSection(plan)
        val wIdx = treePlan.indexOf("Window")
        wIdx >= 0 && treePlan.indexOf("SinglePartition", wIdx) >= 0
      } else false

      // Plan-tree fallback: Window without PARTITION BY has Exchange(SinglePartition) as a
      // DESCENDANT of the Window node in the SparkPlanInfo tree.  For partitioned windows
      // the SinglePartition exchange is an ancestor (outer agg), not a descendant.
      val windowTreeFires: Boolean = sql.planTree.exists { tree =>
        tree.nodesNamed("Window").exists { w =>
          w.flatten.exists(n => n.simpleString.contains("SinglePartition"))
        }
      }

      if (windowTextFires || windowTreeFires) {
        issues += Issue(
          id              = s"plan-window-nopart-${sql.executionId}",
          severity        = Warning,
          category        = "plan",
          title           = s"""Window Function Without PARTITION BY in "${desc.take(80)}"""",
          description     = "A window function runs without a PARTITION BY clause, forcing all data to a single partition — effectively serial execution.",
          recommendation  = "Add PARTITION BY <column> to your window spec to distribute computation across partitions.",
          codeFix         = Some("Window.partitionBy(\"user_id\").orderBy(\"timestamp\")"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      if (plan.contains("RoundRobinPartitioning")) {
        issues += Issue(
          id              = s"plan-roundrobin-${sql.executionId}",
          severity        = Info,
          category        = "plan",
          title           = s"""Round-Robin Repartition in "${desc.take(80)}"""",
          description     = "repartition(N) without a column triggers a full shuffle with round-robin assignment. This is rarely necessary.",
          recommendation  = "Use repartition(N, joinKey) to co-locate data by key, or coalesce(N) to reduce partitions without a full shuffle.",
          codeFix         = Some("df.repartition(200, col(\"join_key\"))"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      if (plan.contains("Statistics(sizeInBytes=") &&
          !plan.contains("rowCount=") &&
          plan.contains("SortMergeJoin")) {
        issues += Issue(
          id              = s"plan-nocbo-${sql.executionId}",
          severity        = Info,
          category        = "plan",
          title           = s"""Missing Row Count Statistics — CBO Cannot Optimize "${desc.take(80)}"""",
          description     = "The query optimizer lacks row count statistics, preventing cost-based join reordering and broadcast decisions.",
          recommendation  = "Run ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS for tables involved in the join.",
          codeFix         = Some("spark.sql(\"ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS\")"),
          affectedJobs    = sql.jobIds,
          estimatedImpact = Some(configRisk),
        )
      }

      // ── explode() / posexplode() — row multiplication ──────────────────────
      // A Generate node with explode produces more rows than it receives.
      // Severity is elevated to Warning only when byte evidence confirms a real
      // explosion (output > 5× input); otherwise Info since explode is often
      // intentional (normalising arrays, flattening nested JSON).
      val hasExplodeText = plan.contains("Generate explode") ||
                           plan.contains("Generate posexplode")
      val hasExplodeTree = sql.planTree.exists(tree =>
        tree.nodesNamed("Generate").exists(n =>
          n.simpleString.toLowerCase.contains("explode")))
      if (hasExplodeText || hasExplodeTree) {
        // Use record counts (more precise than bytes) to detect unintended row explosion.
        val linkedStages = for {
          jobId   <- sql.jobIds
          job     <- app.jobs.get(jobId).toSeq
          stageId <- job.stageIds
          stage   <- app.stages.get(stageId).toSeq
        } yield stage

        val inputRecords  = linkedStages.map(s => s.totalInputRecords + s.totalShuffleRecordsRead).sum
        val outputRecords = linkedStages.map(s => s.totalShuffleRecordsWritten + s.totalOutputRecords).sum
        val inputBytes    = linkedStages.map(s => s.totalInputBytes + s.totalShuffleRemoteBytes + s.totalShuffleLocalBytes).sum
        val outputBytes   = linkedStages.map(s => s.totalOutputBytes + s.totalShuffleBytesWritten).sum

        val explodeRatio  = propDouble(app, "spark.sparklens.plan.explodeRatio", 5.0)

        // Prefer records ratio; fall back to bytes when record metrics unavailable
        val (explodeSeverity, explodeDesc) =
          if (inputRecords > 1000 && outputRecords > (inputRecords * explodeRatio).toLong)
            (Warning, s"explode() expanded ${inputRecords} input records to ${outputRecords} output records " +
              s"(${fmtDouble(outputRecords.toDouble / inputRecords, 1)}× expansion). This is typically an unintended many-to-many explosion.")
          else if (inputBytes > MB && outputBytes > (inputBytes * explodeRatio).toLong)
            (Warning, s"explode() expanded input ${fmtBytes(inputBytes)} to output ${fmtBytes(outputBytes)} " +
              s"(${fmtDouble(outputBytes.toDouble / inputBytes, 1)}× by bytes). Verify this is expected.")
          else
            (Info, "A Generate(explode) node was found in the physical plan. explode() multiplies rows for every array or map element — verify this is intentional.")

        issues += Issue(
          id              = s"plan-explode-${sql.executionId}",
          severity        = explodeSeverity,
          category        = "plan",
          title           = s"""explode() / posexplode() in "${desc.take(80)}"${if (explodeSeverity == Warning) " — Row Explosion Confirmed" else " — Verify Row Multiplication is Intentional"}""",
          description     = explodeDesc,
          recommendation  =
            "If you only need to check existence, use array_contains() instead of explode(). " +
            "If downstream joins run on the exploded column, consider a lateral join to avoid materialising the full cross-product.",
          codeFix         = Some(
            "// Instead of:\n" +
            "df.withColumn(\"item\", explode(col(\"items\"))).join(ref, \"item\")\n" +
            "// Consider:\n" +
            "df.join(ref, array_contains(df(\"items\"), ref(\"item\")))"),
          affectedJobs    = sql.jobIds,
          metrics         = Map(
            "input_records"  -> inputRecords.toString,
            "output_records" -> outputRecords.toString,
          ),
          estimatedImpact = Some(configRisk),
        )
      }

      // ── Slow plan compilation — driver bottleneck ──────────────────────────
      // The gap between SQL execution start and first job submission is the time
      // the driver spent building and optimising the query plan.  A large gap
      // indicates an overly complex plan — typically from withColumn() in a loop,
      // deeply nested CTEs, or a very wide schema.
      if (sql.startTimeMs > 0) {
        val submitTimes = sql.jobIds
          .flatMap(jid => app.jobs.get(jid))
          .map(_.submissionTimeMs)
          .filter(_ > 0)
        if (submitTimes.nonEmpty) {
          val compileMs     = submitTimes.min - sql.startTimeMs
          val compileWarnMs = propLong(app, "spark.sparklens.plan.compileWarnMs", 5000L)
          if (compileMs >= compileWarnMs) {
            issues += Issue(
              id              = s"plan-slow-compile-${sql.executionId}",
              severity        = Warning,
              category        = "plan",
              title           = s"""Slow Plan Compilation in "${desc.take(80)}" — ${fmtMs(compileMs)} on driver""",
              description     = s"Spark spent ${fmtMs(compileMs)} on the driver compiling the query plan before any executor work started. This is usually caused by withColumn() in a loop, deeply nested CTEs, or a very wide schema.",
              recommendation  =
                "Combine all withColumn() calls into a single select(). " +
                "Replace Python/Scala loops that build DataFrames incrementally with set-based operations.",
              codeFix         = Some(
                "// Instead of:\n" +
                "for name, expr in cols.items():\n" +
                "    df = df.withColumn(name, expr)\n" +
                "// Use:\n" +
                "df.select('*', *[expr.alias(name) for name, expr in cols.items()])"),
              affectedJobs    = sql.jobIds,
              metrics         = Map("compile_ms" -> compileMs.toString),
              estimatedImpact = Some(EstimatedImpact(
                summary     = s"${fmtMs(compileMs)} of driver compile time before first executor task",
                savedTimeMs = timeOpt(compileMs),
                savedBytes  = None,
                confidence  = "high",
              )),
            )
          }
        }
      }

      issues.toSeq
    }
}
