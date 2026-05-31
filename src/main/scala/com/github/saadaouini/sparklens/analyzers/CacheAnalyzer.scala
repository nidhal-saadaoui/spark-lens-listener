package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object CacheAnalyzer extends Analyzer {

  // Spark-internal RDD class names that are infrastructure, not user datasets.
  // "Python" covers PySpark's PythonRDD wrapper — it's an execution vehicle, not a dataset.
  private val InternalPrefixes = Set(
    "Map", "Parallel", "Shuffled", "HadoopRDD", "FileScan", "SQLExecution",
    "WholeStage", "Exchange", "Filter", "Project", "Scan", "Union", "Coalesced",
    "Zipped", "Python",
  )

  private def isInternal(name: String): Boolean =
    InternalPrefixes.exists(name.startsWith)

  def analyze(app: SparkAppModel): Seq[Issue] = {
    // Map each RDD name → set of job IDs that scanned it
    val rddToJobs = scala.collection.mutable.Map[String, Set[Int]]()

    for {
      (jobId, job) <- app.jobs
      stageId      <- job.stageIds
      stage        <- app.stages.get(stageId)
      rddName      <- stage.rddNames
      if rddName.nonEmpty && !isInternal(rddName)
    } {
      rddToJobs(rddName) = rddToJobs.getOrElse(rddName, Set.empty) + jobId
    }

    // Collect all RDD names that have caching configured across all stages — if the
    // user already called .cache()/.persist(), isCached is true and we suppress the issue.
    val cachedRddNames: Set[String] = app.stages.values.flatMap(_.rddCachedNames).toSet

    val rddIssues = rddToJobs.toSeq.collect {
      case (rddName, jobs) if jobs.size >= 2 && !cachedRddNames.contains(rddName) =>
        val sortedJobs = jobs.toSeq.sorted
        val jobNames   = sortedJobs.flatMap(id => app.jobs.get(id).map(j => s"'${j.name.take(60)}'"))
                                   .take(3).mkString(", ")
        Issue(
          id             = s"cache-${rddName.hashCode.abs}",
          severity       = Warning,
          category       = "cache",
          title          = s"Repeated Scan of '$rddName' Across ${jobs.size} Jobs Without Caching",
          description    = s"The dataset '$rddName' is read from scratch in ${jobs.size} jobs ($jobNames). Each job re-executes the full upstream lineage — reading source files, applying filters, and transforming data — when the result could be reused.",
          recommendation = "Call .cache() or .persist(StorageLevel.MEMORY_AND_DISK) before the first action, then .unpersist() when done. Use MEMORY_AND_DISK instead of MEMORY_ONLY to avoid re-computation if an executor is lost.",
          codeFix        = Some("val cached = df.persist(StorageLevel.MEMORY_AND_DISK)\ncached.count()  // materialise\n// ... use cached for subsequent jobs ...\ncached.unpersist()"),
          affectedJobs   = sortedJobs,
          metrics        = Map("job_count" -> jobs.size.toString, "rdd_name" -> rddName),
        )
    }

    // ── DataFrame / SQL repeated-scan detection ───────────────────────────────
    // When users call df.cache() Spark inserts InMemoryRelation into the plan.
    // If the same table appears in ≥ N SQL executions without InMemoryRelation,
    // the user is re-reading it from source every time.
    // Default threshold 5: three reads is common in any multi-step pipeline
    // (count → filter → join); five is a stronger signal of a reuse opportunity.
    val sqlIssues: Seq[Issue] = if (app.sqlExecutions.isEmpty) Nil else {
      val sqlMinExecCount = propLong(app, "spark.sparklens.cache.sql.minExecCount", 5L).toInt
      val sqlMaxGbForWarn = propLong(app, "spark.sparklens.cache.sql.warnMaxGb",    5L)
      val FileScanRe      = """FileScan \w+ ([\w.`/]+)\[""".r

      // If a table appears under InMemoryRelation in ANY execution, df.cache() was called
      // on it somewhere — suppress that table globally.
      val cachedTableNames: Set[String] = app.sqlExecutions.values
        .filter(_.physicalPlan.contains("InMemoryRelation"))
        .flatMap(sql => FileScanRe.findAllMatchIn(sql.physicalPlan).map(_.group(1)))
        .toSet

      // Track which execution IDs scan each table so we can estimate data size.
      val tableToExecIds = scala.collection.mutable.Map[String, List[Long]]()
      app.sqlExecutions.values.foreach { sql =>
        FileScanRe.findAllMatchIn(sql.physicalPlan)
          .map(_.group(1))
          .toSet
          .filterNot(cachedTableNames.contains)
          .foreach { t =>
            tableToExecIds(t) = sql.executionId :: tableToExecIds.getOrElse(t, Nil)
          }
      }

      // Estimate average bytes read per execution for a set of execution IDs.
      // Correlates execution → jobs → stages → totalInputBytes.  Deduplicates by
      // stage ID so that stages shared across multiple SQL executions (same underlying
      // job) are not counted more than once before dividing by execIds.size.
      def estimateAvgBytesPerExec(execIds: List[Long]): Long = {
        if (execIds.isEmpty) return 0L
        val stageInputMap = (for {
          id    <- execIds
          sql   <- app.sqlExecutions.get(id).toSeq
          jobId <- sql.jobIds
          job   <- app.jobs.get(jobId).toSeq
          sid   <- job.stageIds
          stage <- app.stages.get(sid).toSeq
        } yield (sid, stage.totalInputBytes)).toMap  // deduplicate by stage ID
        if (stageInputMap.isEmpty) 0L
        else stageInputMap.values.sum / execIds.size
      }

      tableToExecIds.toSeq.collect {
        case (table, execIds) if execIds.size >= sqlMinExecCount =>
          val count    = execIds.size
          val avgBytes = estimateAvgBytesPerExec(execIds)
          val isLarge  = avgBytes > sqlMaxGbForWarn * GB
          val severity = if (isLarge) Info else Warning
          val rec      =
            if (isLarge)
              s"The estimated scan size is ${fmtBytes(avgBytes)}/execution. Caching this table " +
              s"requires enough executor memory to hold the full dataset — only cache if it fits. " +
              s"For large tables, prefer broadcast joins or bucketed tables instead."
            else
              "Cache the DataFrame before the first action and unpersist when done. " +
              "Use persist(StorageLevel.MEMORY_AND_DISK) so the cache survives executor loss."
          Issue(
            id             = s"cache-sql-${table.hashCode.abs}",
            severity       = severity,
            category       = "cache",
            title          = s"Repeated Scan of '$table' Across $count SQL Executions Without Caching",
            description    = s"The table '$table' is read from source in $count separate SQL executions. Each execution re-reads the full dataset from storage when the result could be reused.",
            recommendation = rec,
            codeFix        = if (isLarge) None
                             else Some("val cached = df.cache()\ncached.count()  // materialise\n// ... reuse cached ...\ncached.unpersist()"),
            metrics        = Map(
              "execution_count"    -> count.toString,
              "table"              -> table,
              "avg_bytes_per_exec" -> avgBytes.toString,
            ),
          )
      }
    }

    rddIssues ++ sqlIssues
  }
}
