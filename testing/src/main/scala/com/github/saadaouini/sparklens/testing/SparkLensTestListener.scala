package com.github.saadaouini.sparklens.testing

import com.github.saadaouini.sparklens.SparkAppModelBuilder
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart,
}

private[testing] class SparkLensTestListener(builder: SparkAppModelBuilder)
    extends SparkListener {

  override def onApplicationStart(e: SparkListenerApplicationStart): Unit  = builder.onApplicationStart(e)
  override def onEnvironmentUpdate(e: SparkListenerEnvironmentUpdate): Unit = builder.onEnvironmentUpdate(e)
  override def onExecutorAdded(e: SparkListenerExecutorAdded): Unit         = builder.onExecutorAdded(e)
  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit     = builder.onExecutorRemoved(e)
  override def onJobStart(e: SparkListenerJobStart): Unit = {
    builder.onJobStart(e)
    Option(e.properties)
      .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
      .flatMap(id => scala.util.Try(id.toLong).toOption)
      .foreach(builder.linkSqlJob(_, e.jobId))
  }
  override def onJobEnd(e: SparkListenerJobEnd): Unit                       = builder.onJobEnd(e)
  override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit       = builder.onStageSubmitted(e)
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit                     = builder.onTaskEnd(e)
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit       = builder.onStageCompleted(e)

  // SQL plan events arrive via onOtherEvent (not typed listener methods)
  // because SparkListenerSQLExecutionStart etc. live in the SQL module.
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      builder.onSqlExecutionStart(e.executionId, e.description, e.physicalPlanDescription, e.sparkPlanInfo, e.time)
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      builder.onSqlPlanUpdate(e.executionId, e.sparkPlanInfo)
    case e: SparkListenerSQLExecutionEnd =>
      builder.onSqlExecutionEnd(e.executionId, e.time)
    case _ =>
  }
}
