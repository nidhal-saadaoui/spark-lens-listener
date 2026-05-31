package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

object HtmlReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(render(app, issues), path)

  private def e(s: String) = s
    .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    .replace("\"", "&quot;")

  private def severityColor(issue: Issue): (String, String) = issue.severity match {
    case Critical => ("#dc2626", "#fef2f2")
    case Warning  => ("#d97706", "#fffbeb")
    case Info     => ("#2563eb", "#eff6ff")
  }

  def render(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score = healthScore(issues)
    val critical = issues.count(_.severity == Critical)
    val warning  = issues.count(_.severity == Warning)
    val info     = issues.count(_.severity == Info)

    val issueCards = issues.map { issue =>
      val (fg, bg) = severityColor(issue)
      val badge = issue.severity.label
      val configBlock = issue.configFix.map(f =>
        s"""<pre style="margin:8px 0 0;background:#f4f4f5;padding:8px;border-radius:4px;font-size:12px;overflow-x:auto">${e(f)}</pre>"""
      ).getOrElse("")
      val codeBlock = issue.codeFix.map(f =>
        s"""<pre style="margin:8px 0 0;background:#f4f4f5;padding:8px;border-radius:4px;font-size:12px;overflow-x:auto">${e(f)}</pre>"""
      ).getOrElse("")
      s"""<div style="border:1px solid ${fg}40;border-left:4px solid $fg;background:$bg;border-radius:6px;padding:14px 16px;margin-bottom:12px">
         |  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
         |    <span style="background:$fg;color:#fff;font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;letter-spacing:.5px">$badge</span>
         |    <span style="font-weight:600;color:#111">${e(issue.title)}</span>
         |  </div>
         |  <p style="margin:0 0 6px;color:#444;font-size:13px">${e(issue.description)}</p>
         |  <p style="margin:0;color:#555;font-size:13px"><strong>→</strong> ${e(issue.recommendation)}</p>
         |  $configBlock$codeBlock
         |</div>""".stripMargin
    }.mkString("\n")

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="UTF-8">
       |<meta name="viewport" content="width=device-width,initial-scale=1">
       |<title>spark-lens — ${e(app.appName)}</title>
       |<style>
       |  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f9fafb;color:#111}
       |  .wrap{max-width:860px;margin:32px auto;padding:0 16px}
       |  h1{font-size:20px;margin:0 0 4px}
       |  .meta{color:#666;font-size:13px;margin-bottom:24px}
       |  .cards{display:flex;gap:12px;margin-bottom:28px;flex-wrap:wrap}
       |  .card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px 20px;min-width:120px;text-align:center}
       |  .card-val{font-size:28px;font-weight:800}
       |  .card-lbl{font-size:12px;color:#666;margin-top:2px}
       |  pre{white-space:pre-wrap;word-break:break-all}
       |</style>
       |</head>
       |<body>
       |<div class="wrap">
       |  <h1>spark-lens report</h1>
       |  <div class="meta">${e(app.appName)} &nbsp;·&nbsp; ${e(app.appId)} &nbsp;·&nbsp; Spark ${e(app.sparkVersion)}</div>
       |  <div class="cards">
       |    <div class="card"><div class="card-val">$score<span style="font-size:14px">/100</span></div><div class="card-lbl">Health Score</div></div>
       |    <div class="card"><div class="card-val" style="color:#dc2626">$critical</div><div class="card-lbl">Critical</div></div>
       |    <div class="card"><div class="card-val" style="color:#d97706">$warning</div><div class="card-lbl">Warning</div></div>
       |    <div class="card"><div class="card-val" style="color:#2563eb">$info</div><div class="card-lbl">Info</div></div>
       |  </div>
       |  ${if (issues.isEmpty) """<p style="color:#15803d;font-weight:600">✔ No issues detected.</p>""" else issueCards}
       |</div>
       |</body>
       |</html>""".stripMargin
  }
}
