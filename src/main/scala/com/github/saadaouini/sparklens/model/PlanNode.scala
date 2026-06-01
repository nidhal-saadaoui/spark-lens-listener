package com.github.saadaouini.sparklens.model

/**
 * A node in the physical query plan, built from SparkPlanInfo at execution-start time and
 * optionally enriched with per-task accumulator values at execution-end time.
 *
 * @param nodeName        operator name (e.g. "SortMergeJoin", "ShuffleExchange", "InMemoryRelation")
 * @param simpleString    one-line description string from the physical plan
 * @param accumulatorIds  accumulator IDs registered by this node — used to correlate with
 *                        per-task AccumulableInfo updates collected in TaskEnd events
 * @param children        child nodes in DFS order
 * @param resolvedMetrics accumulatorId → sum-of-per-task-updates, populated at ExecutionEnd
 */
case class PlanNode(
  nodeName:        String,
  simpleString:    String,
  accumulatorIds:  Seq[Long],
  children:        Seq[PlanNode],
  resolvedMetrics: Map[Long, Long] = Map.empty,
) {
  /** DFS walk returning this node and all descendants. */
  def flatten: Seq[PlanNode] = this +: children.flatMap(_.flatten)

  /** True if any node in the subtree has a nodeName containing `substr`. */
  def contains(substr: String): Boolean = flatten.exists(_.nodeName.contains(substr))

  /** All nodes in the subtree whose nodeName equals `name` exactly. */
  def nodesNamed(name: String): Seq[PlanNode] = flatten.filter(_.nodeName == name)

  /** All nodes in the subtree whose nodeName contains `substr`. */
  def nodesContaining(substr: String): Seq[PlanNode] = flatten.filter(_.nodeName.contains(substr))
}
