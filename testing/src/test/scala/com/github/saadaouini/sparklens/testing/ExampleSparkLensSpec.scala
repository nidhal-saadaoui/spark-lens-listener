package com.github.saadaouini.sparklens.testing

import org.apache.spark.sql.functions._

/** End-to-end validation of the SparkLens testing framework.
 *
 *  These tests run real Spark jobs locally (local[*]) and assert on the
 *  spark-lens analysis output. They serve both as framework self-tests and
 *  as usage examples for library consumers.
 */
class ExampleSparkLensSpec extends SparkLensSpec {

  "cross join" should "trigger a CartesianProduct issue" in {
    analyse {
      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      spark.range(100).crossJoin(spark.range(10)).count()
    } should haveIssue("plan-cartesian")
  }

  "well-formed aggregation" should "not produce any spill issue" in {
    analyse {
      spark.range(10000).groupBy("id").count().collect()
    } should not(haveIssueOfCategory("spill"))
  }

  "health score" should "be between 0 and 100 for any job" in {
    val result = analyse {
      spark.range(1000).count()
    }
    result.healthScore should be >= 0
    result.healthScore should be <= 100
  }

  "SparkLensSuite style" should "also work" in {
    // Verify FunSuite variant compiles and the analyse block returns a result
    val result = analyse {
      spark.range(500).filter(col("id") > 100).count()
    }
    result.issues should not be null
  }
}
