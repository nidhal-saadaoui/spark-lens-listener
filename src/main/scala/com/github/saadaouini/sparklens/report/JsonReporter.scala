package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

object JsonReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(render(app, issues), path)

  def render(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score  = healthScore(issues)
    val esc    = (s: String) => s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
    val issueJson = issues.map { i =>
      s"""{
         |    "id": "${esc(i.id)}",
         |    "severity": "${i.severity.label.toLowerCase}",
         |    "category": "${esc(i.category)}",
         |    "title": "${esc(i.title)}",
         |    "description": "${esc(i.description)}",
         |    "recommendation": "${esc(i.recommendation)}",
         |    "config_fix": ${i.configFix.map(f => s""""${esc(f)}"""").getOrElse("null")},
         |    "code_fix": ${i.codeFix.map(f => s""""${esc(f)}"""").getOrElse("null")},
         |    "affected_stages": [${i.affectedStages.mkString(",")}],
         |    "affected_jobs": [${i.affectedJobs.mkString(",")}]
         |  }""".stripMargin
    }.mkString(",\n  ")

    s"""{
       |  "app_id": "${esc(app.appId)}",
       |  "app_name": "${esc(app.appName)}",
       |  "spark_version": "${esc(app.sparkVersion)}",
       |  "health_score": $score,
       |  "issue_count": ${issues.size},
       |  "issues": [$issueJson]
       |}""".stripMargin
  }
}
