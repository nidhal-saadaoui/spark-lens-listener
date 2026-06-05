package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.model.{Critical, Warning}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterEach

/** Integration tests for SparkLensSpec — run real Spark jobs and assert on analysis output.
 *
 *  Covers all 6 matchers in both positive and negative directions.
 *  `BeforeAndAfterEach` resets any Spark config changes made inside analyse blocks
 *  so tests are isolated from each other.
 */
class ExampleSparkLensSpec extends SparkLensSpec with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // Reset configs that individual tests may have changed
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // default 10MB
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    super.afterEach()
  }

  // ── haveIssue ─────────────────────────────────────────────────────────────

  "cross join" should "trigger a CartesianProduct issue" in {
    analyse {
      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      spark.range(1000).crossJoin(spark.range(100)).count()
    } should haveIssue("plan-cartesian")
  }

  "well-formed join" should "not trigger a CartesianProduct issue" in {
    analyse {
      val a = spark.range(1000).withColumn("k", (col("id") % 10).cast("int"))
      val b = spark.range(10).withColumn("k", col("id").cast("int"))
      a.join(b, "k").count()
    } should not(haveIssue("plan-cartesian"))
  }

  // ── haveIssueOfCategory ───────────────────────────────────────────────────

  "AQE-disabled job" should "trigger a config category issue" in {
    // AQE is disabled in SparkLensSpec — ConfigAnalyzer fires config-aqe-disabled
    analyse {
      spark.range(5000).groupBy((col("id") % 50).alias("k")).count().collect()
    } should haveIssueOfCategory("config")
  }

  "simple count" should "not trigger any spill issue" in {
    analyse {
      spark.range(10000).groupBy("id").count().collect()
    } should not(haveIssueOfCategory("spill"))
  }

  // ── haveIssueOfSeverity / haveNoIssuesOfSeverity ──────────────────────────

  "AQE-disabled job" should "have at least one Warning issue" in {
    analyse {
      spark.range(5000).count()
    } should haveIssueOfSeverity(Warning)
  }

  "simple count" should "have no Critical issues" in {
    analyse {
      spark.range(1000).count()
    } should haveNoIssuesOfSeverity(Critical)
  }

  // ── haveHealthScoreAbove / haveHealthScoreBelow ───────────────────────────

  "health score" should "be between 0 and 100" in {
    val result = analyse { spark.range(1000).count() }
    result.healthScore should (be >= 0 and be <= 100)
  }

  "job with config issues" should "have health score below 100" in {
    // AQE disabled + Java serializer + other config issues → score < 100
    analyse {
      spark.range(5000).groupBy((col("id") % 50).alias("k")).count().collect()
    } should haveHealthScoreBelow(100)
  }

  "trivial job" should "not have a critically low health score" in {
    analyse {
      spark.range(100).count()
    } should haveHealthScoreAbove(0)
  }

  // ── SparkLensResult accessors ─────────────────────────────────────────────

  "analyse block" should "return all detected issues" in {
    val result = analyse {
      spark.range(500).filter(col("id") > 100).count()
    }
    result.issues should not be empty
    result.issues.forall(_.id.nonEmpty) shouldBe true
  }

  it should "return correct issuesOfCategory" in {
    val result = analyse {
      spark.range(5000).groupBy((col("id") % 50).alias("k")).count().collect()
    }
    val configIssues = result.issuesOfCategory("config")
    configIssues should not be empty
    configIssues.forall(_.category == "config") shouldBe true
  }

  // ── analyse block isolation ───────────────────────────────────────────────

  "consecutive analyse blocks" should "each have independent issue lists" in {
    val r1 = analyse { spark.range(100).count() }
    val r2 = analyse { spark.range(200).count() }
    // Both return results; neither should bleed state into the other
    r1.issues should not be null
    r2.issues should not be null
  }
}

/** Verifies that SparkLensSuite (FunSuite style) works end-to-end. */
class ExampleSparkLensSuite extends SparkLensSuite {

  test("SparkLensSuite: analyse block returns a valid result") {
    val result = analyse { spark.range(1000).count() }
    result.healthScore should (be >= 0 and be <= 100)
    result.issues should not be null
  }

  test("SparkLensSuite: haveIssueOfCategory matcher works") {
    analyse {
      spark.range(5000).groupBy((col("id") % 50).alias("k")).count().collect()
    } should haveIssueOfCategory("config")
  }

  test("SparkLensSuite: not matcher works") {
    analyse {
      spark.range(10000).count()
    } should not(haveIssueOfCategory("spill"))
  }
}
