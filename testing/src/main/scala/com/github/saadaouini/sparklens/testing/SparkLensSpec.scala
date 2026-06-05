package com.github.saadaouini.sparklens.testing

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Canceled, Outcome}
import org.scalatest.exceptions.TestCanceledException
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

  // Spark 3.5 / Hadoop 3.3.4 uses Subject.getSubject() removed in Java 23.
  // Cancel each test on Java 23+ rather than aborting the suite.
  override def withFixture(test: NoArgTest): Outcome = {
    val jvmMajor = sys.props("java.version").split("\\.")(0).toInt
    if (jvmMajor >= 23)
      Canceled(new TestCanceledException(
        s"SparkLensSpec requires JVM < 23 (detected $jvmMajor); " +
        "Spark 3.5/Hadoop 3.3.4 incompatible with Subject.getSubject removal", 0))
    else
      super.withFixture(test)
  }

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

  override def withFixture(test: NoArgTest): Outcome = {
    val jvmMajor = sys.props("java.version").split("\\.")(0).toInt
    if (jvmMajor >= 23)
      Canceled(new TestCanceledException(
        s"SparkLensSuite requires JVM < 23 (detected $jvmMajor); " +
        "Spark 3.5/Hadoop 3.3.4 incompatible with Subject.getSubject removal", 0))
    else
      super.withFixture(test)
  }

  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    super.afterAll()
  }

  def analyse(block: => Unit): SparkLensResult = SparkLensAnalyser.run(block)
}
