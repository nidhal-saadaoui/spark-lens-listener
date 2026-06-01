package com.github.saadaouini.sparklens.model

sealed abstract class Severity(val order: Int, val label: String)
case object Critical extends Severity(0, "CRITICAL")
case object Warning  extends Severity(1, "WARNING")
case object Info     extends Severity(2, "INFO")

object Severity {
  def fromString(s: String): Severity = s.toLowerCase match {
    case "critical" => Critical
    case "warning"  => Warning
    case "info"     => Info
    case other      => throw new IllegalArgumentException(s"Unknown severity: $other")
  }
}

case class Issue(
  id:             String,
  severity:       Severity,
  category:       String,
  title:          String,
  description:    String,
  recommendation: String,
  configFix:      Option[String]  = None,
  codeFix:        Option[String]  = None,
  affectedStages: Seq[Int]        = Nil,
  affectedJobs:   Seq[Int]        = Nil,
  metrics:        Map[String, String] = Map.empty,
  estimatedImpact: Option[EstimatedImpact] = None,
)
