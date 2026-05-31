package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

object TextReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(renderString(app, issues), path)

  def renderString(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score = healthScore(issues)
    val sb    = new StringBuilder

    sb.append("\n")
    sb.append("=" * 70).append("\n")
    sb.append(s"  spark-lens  |  ${app.appName}  (${app.appId})\n")
    sb.append(s"  Spark ${app.sparkVersion.padTo(6, ' ')}  |  Health: $score/100")
    if (issues.nonEmpty) {
      val c = issues.count(_.severity == Critical)
      val w = issues.count(_.severity == Warning)
      val i = issues.count(_.severity == Info)
      val parts = Seq(
        if (c > 0) s"$c critical" else "",
        if (w > 0) s"$w warning"  else "",
        if (i > 0) s"$i info"     else "",
      ).filter(_.nonEmpty).mkString("  ·  ")
      sb.append(s"  |  $parts")
    }
    sb.append("\n")
    sb.append("=" * 70).append("\n")

    if (issues.isEmpty) {
      sb.append("  ✔  No issues detected.\n")
    } else {
      issues.foreach { issue =>
        val icon = issue.severity match {
          case Critical => "✖ CRITICAL"
          case Warning  => "⚠ WARNING "
          case Info     => "ℹ INFO    "
        }
        sb.append(s"\n  [$icon]  ${issue.title}\n")
        sb.append(s"           ${issue.description}\n")
        sb.append(s"        →  ${issue.recommendation}\n")
        issue.configFix.foreach(f => sb.append(s"           config: $f\n"))
        issue.codeFix.foreach(f   => sb.append(s"           code:   $f\n"))
      }
    }

    sb.append("=" * 70).append("\n\n")
    sb.toString()
  }
}
