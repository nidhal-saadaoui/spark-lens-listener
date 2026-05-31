package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.Warning
import AnalyzerFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CacheAnalyzerSpec extends AnyFlatSpec with Matchers {

  "CacheAnalyzer" should "return no issues for an empty app" in {
    CacheAnalyzer.analyze(app()) shouldBe empty
  }

  it should "return no issues when an RDD appears in only one job" in {
    val s0 = stage(stageId = 0, rddNames = Seq("ParquetScan"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    CacheAnalyzer.analyze(app(stages = Map(0 -> s0), jobs = Map(0 -> j0))) shouldBe empty
  }

  it should "flag an RDD scanned in two different jobs" in {
    val s0 = stage(stageId = 0, rddNames = Seq("events"))
    val s1 = stage(stageId = 1, rddNames = Seq("events"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.severity shouldBe Warning
  }

  it should "include both job IDs in affected jobs" in {
    val s0 = stage(stageId = 0, rddNames = Seq("orders"))
    val s1 = stage(stageId = 1, rddNames = Seq("orders"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    issues.head.affectedJobs.sorted shouldBe Seq(0, 1)
  }

  it should "flag an RDD scanned in three jobs once with all three jobs listed" in {
    val stages = (0 to 2).map(i => i -> stage(stageId = i, rddNames = Seq("users"))).toMap
    val jobs   = (0 to 2).map(i => i -> job(jobId = i, stageIds = Seq(i))).toMap
    val issues = CacheAnalyzer.analyze(app(stages = stages, jobs = jobs))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.affectedJobs.sorted shouldBe Seq(0, 1, 2)
    cacheIssues.head.metrics("job_count") shouldBe "3"
  }

  it should "not flag different RDD names" in {
    val s0 = stage(stageId = 0, rddNames = Seq("users"))
    val s1 = stage(stageId = 1, rddNames = Seq("products"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "skip internal Map-prefixed RDD names" in {
    val s0 = stage(stageId = 0, rddNames = Seq("MapPartitionsRDD"))
    val s1 = stage(stageId = 1, rddNames = Seq("MapPartitionsRDD"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "not flag an RDD that is already cached" in {
    // rddCachedNames populated because user called .cache() — suppress the issue
    val s0 = stage(stageId = 0, rddNames = Seq("events"), rddCachedNames = Set("events"))
    val s1 = stage(stageId = 1, rddNames = Seq("events"), rddCachedNames = Set("events"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  it should "skip PySpark PythonRDD as an internal infrastructure name" in {
    val s0 = stage(stageId = 0, rddNames = Seq("PythonRDD"))
    val s1 = stage(stageId = 1, rddNames = Seq("PythonRDD"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    )) shouldBe empty
  }

  // ── DataFrame / SQL repeated-scan detection ─────────────────────────────

  it should "flag a table scanned in 3+ SQL executions without InMemoryRelation" in {
    val plan = "FileScan parquet default.orders[id#1L,amount#2] ..."
    val execs = (0 to 2).map(i => i.toLong -> sqlExec(id = i.toLong, plan = plan)).toMap
    val issues = CacheAnalyzer.analyze(app(sqlExecs = execs))
    val cacheIssues = issues.filter(_.category == "cache")
    cacheIssues should have size 1
    cacheIssues.head.title should include("default.orders")
    cacheIssues.head.metrics("execution_count") shouldBe "3"
  }

  it should "not flag a table scanned in only 2 SQL executions" in {
    val plan = "FileScan parquet default.orders[id#1L] ..."
    val execs = (0 to 1).map(i => i.toLong -> sqlExec(id = i.toLong, plan = plan)).toMap
    CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.category == "cache") shouldBe empty
  }

  it should "not flag SQL executions that contain InMemoryRelation" in {
    val cachedPlan = "InMemoryRelation [...]\n   +- FileScan parquet default.events[...]"
    val uncachedPlan = "FileScan parquet default.events[...]"
    val execs = Map(
      0L -> sqlExec(id = 0L, plan = cachedPlan),
      1L -> sqlExec(id = 1L, plan = uncachedPlan),
      2L -> sqlExec(id = 2L, plan = uncachedPlan),
      3L -> sqlExec(id = 3L, plan = uncachedPlan),
    )
    // execs 1,2,3 scan default.events but exec 0 has InMemoryRelation so
    // those three are NOT flagged (the table IS cached somewhere in the app)
    CacheAnalyzer.analyze(app(sqlExecs = execs)).filter(_.id.startsWith("cache-sql")) shouldBe empty
  }

  it should "flag the RDD name in the issue title" in {
    val s0 = stage(stageId = 0, rddNames = Seq("clickstream"))
    val s1 = stage(stageId = 1, rddNames = Seq("clickstream"))
    val j0 = job(jobId = 0, stageIds = Seq(0))
    val j1 = job(jobId = 1, stageIds = Seq(1))
    val issues = CacheAnalyzer.analyze(app(
      stages = Map(0 -> s0, 1 -> s1),
      jobs   = Map(0 -> j0, 1 -> j1),
    ))
    issues.head.title should include("clickstream")
  }
}
