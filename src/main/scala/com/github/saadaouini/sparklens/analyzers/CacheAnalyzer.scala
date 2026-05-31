package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model._

object CacheAnalyzer extends Analyzer {

  def analyze(app: SparkAppModel): Seq[Issue] = {
    // Map each RDD name → set of job IDs that scanned it
    val rddToJobs = scala.collection.mutable.Map[String, Set[Int]]()

    for {
      (jobId, job) <- app.jobs
      stageId      <- job.stageIds
      stage        <- app.stages.get(stageId)
      rddName      <- stage.rddNames
      if rddName.nonEmpty && !rddName.startsWith("Map")  // skip internal shuffle/map names
    } {
      rddToJobs(rddName) = rddToJobs.getOrElse(rddName, Set.empty) + jobId
    }

    rddToJobs.toSeq.collect {
      case (rddName, jobs) if jobs.size >= 2 =>
        Issue(
          id             = s"cache-${rddName.hashCode.abs}",
          severity       = Warning,
          category       = "cache",
          title          = s"Repeated Scan of '$rddName' Across ${jobs.size} Jobs Without Caching",
          description    = s"The dataset '$rddName' is read from scratch in ${jobs.size} separate jobs. Each read re-executes the full lineage.",
          recommendation = "Call .cache() or .persist() on the DataFrame/RDD before the first action, then .unpersist() when done.",
          codeFix        = Some("df.cache()  // before the first .count()/.show()/etc."),
          affectedJobs   = jobs.toSeq.sorted,
          metrics        = Map("job_count" -> jobs.size.toString, "rdd_name" -> rddName),
        )
    }
  }
}
