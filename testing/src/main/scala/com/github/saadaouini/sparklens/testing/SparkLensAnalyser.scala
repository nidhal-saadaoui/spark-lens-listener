package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.{Analyzers, SparkAppModelBuilder}
import org.apache.spark.sql.SparkSession

object SparkLensAnalyser {

  def run(block: => Unit)(implicit spark: SparkSession): SparkLensResult = {
    val builder  = new SparkAppModelBuilder(spark.version)
    val listener = new SparkLensTestListener(builder)
    spark.sparkContext.addSparkListener(listener)
    try {
      block
      // Wait until all active jobs complete (actions are blocking in local mode,
      // but the status tracker confirms Spark has finished scheduling too).
      // Then drain the async listener bus with a short sleep before calling build().
      val deadline = System.currentTimeMillis() + 30000L
      while (spark.sparkContext.statusTracker.getActiveJobIds().nonEmpty &&
             System.currentTimeMillis() < deadline) {
        Thread.sleep(20)
      }
      Thread.sleep(200) // brief drain for listener bus async events
      val model  = builder.build(System.currentTimeMillis())
      val issues = Analyzers.runAll(model)
      SparkLensResult(model, issues)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
