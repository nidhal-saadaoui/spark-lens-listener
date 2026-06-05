package com.github.saadaouini.sparklens.testing

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite

/** ScalaTest FlatSpec mixin for spark-lens performance assertions.
 *
 *  {{{
 *  class MyJobSpec extends SparkLensSpec {
 *    "cross join" should "trigger CartesianProduct" in {
 *      analyse {
 *        spark.range(100).crossJoin(spark.range(10)).count()
 *      } should haveIssue("plan-cartesian")
 *    }
 *  }
 *  }}}
 */
trait SparkLensSpec extends AnyFlatSpec with SparkLensMatchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled",           "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    super.afterAll()
  }

  def analyse(block: => Unit): SparkLensResult = SparkLensAnalyser.run(block)
}

/** ScalaTest FunSuite mixin for spark-lens performance assertions.
 *
 *  {{{
 *  class MyJobSuite extends SparkLensSuite {
 *    test("cartesian join triggers plan-cartesian issue") {
 *      analyse {
 *        spark.range(100).crossJoin(spark.range(10)).count()
 *      } should haveIssue("plan-cartesian")
 *    }
 *  }
 *  }}}
 */
trait SparkLensSuite extends AnyFunSuite with SparkLensMatchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled",           "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    super.afterAll()
  }

  def analyse(block: => Unit): SparkLensResult = SparkLensAnalyser.run(block)
}
