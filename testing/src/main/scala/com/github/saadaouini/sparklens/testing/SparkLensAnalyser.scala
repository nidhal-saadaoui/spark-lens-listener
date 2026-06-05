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
      drainListenerBus(spark.sparkContext)
      val model  = builder.build(System.currentTimeMillis())
      val issues = Analyzers.runAll(model)
      SparkLensResult(model, issues)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  // Drain the async listener bus before reading results.
  // listenerBus is private[spark] so we access it via reflection; fall back to
  // a 1-second sleep if the API changes in a future Spark version.
  private def drainListenerBus(sc: org.apache.spark.SparkContext): Unit =
    try {
      val f = classOf[org.apache.spark.SparkContext].getDeclaredField("listenerBus")
      f.setAccessible(true)
      val bus = f.get(sc)
      val wait = bus.getClass.getMethods
        .find(m => m.getName == "waitUntilEmpty" && m.getParameterCount == 1)
        .orNull
      if (wait != null) wait.invoke(bus, 30000L.asInstanceOf[AnyRef])
      else Thread.sleep(1000L)
    } catch { case _: Exception => Thread.sleep(1000L) }
}
