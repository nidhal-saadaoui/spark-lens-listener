package com.github.saadaouini.sparklens.report

import com.github.saadaouini.sparklens.model.{Critical, Info, Issue, SparkAppModel, Warning}

object HtmlReporter extends Reporter {

  def write(app: SparkAppModel, issues: Seq[Issue], path: Option[String]): Unit =
    writeOrPrint(render(app, issues), path)

  private def e(s: String): String = s
    .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    .replace("\"", "&quot;")

  private def severityClass(issue: Issue): String = issue.severity match {
    case Critical => "critical"
    case Warning  => "warning"
    case Info     => "info"
  }

  def render(app: SparkAppModel, issues: Seq[Issue]): String = {
    val score    = healthScore(issues)
    val critical = issues.count(_.severity == Critical)
    val warning  = issues.count(_.severity == Warning)
    val info     = issues.count(_.severity == Info)

    val issueCards = issues.map { issue =>
      val cls   = severityClass(issue)
      val badge = issue.severity.label

      val configBlock = issue.configFix.map(f =>
        s"""<div class="fix-label">config</div><pre class="fix-block">${e(f)}</pre>"""
      ).getOrElse("")

      val codeBlock = issue.codeFix.map(f =>
        s"""<div class="fix-label">code</div><pre class="fix-block">${e(f)}</pre>"""
      ).getOrElse("")

      val stagePills = if (issue.affectedStages.nonEmpty)
        "<div class=\"locations\">" +
          issue.affectedStages.map(s => s"""<span class="pill">stage&nbsp;$s</span>""").mkString(" ") +
          "</div>"
      else ""

      val jobPills = if (issue.affectedJobs.nonEmpty)
        "<div class=\"locations\">" +
          issue.affectedJobs.map(j => s"""<span class="pill">job&nbsp;$j</span>""").mkString(" ") +
          "</div>"
      else ""

      s"""<details class="issue $cls" open>
         |  <summary>
         |    <span class="badge">$badge</span>
         |    <span class="issue-title">${e(issue.title)}</span>
         |  </summary>
         |  <div class="issue-body">
         |    <p class="desc">${e(issue.description)}</p>
         |    <p class="rec"><span class="arrow">&#8594;</span> ${e(issue.recommendation)}</p>
         |    $configBlock$codeBlock$stagePills$jobPills
         |  </div>
         |</details>""".stripMargin
    }.mkString("\n")

    val body = if (issues.isEmpty)
      """<p class="no-issues">&#10004; No issues detected.</p>"""
    else issueCards

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="UTF-8">
       |<meta name="viewport" content="width=device-width,initial-scale=1">
       |<title>spark-lens &#8212; ${e(app.appName)}</title>
       |<style>
       |$css
       |</style>
       |</head>
       |<body>
       |<div class="wrap">
       |  <h1>spark-lens report</h1>
       |  <div class="meta">${e(app.appName)} &nbsp;&middot;&nbsp; ${e(app.appId)} &nbsp;&middot;&nbsp; Spark ${e(app.sparkVersion)}</div>
       |  <div class="cards">
       |    <div class="card"><div class="card-val score">$score<span class="denom">/100</span></div><div class="card-lbl">Health Score</div></div>
       |    <div class="card"><div class="card-val crit">$critical</div><div class="card-lbl">Critical</div></div>
       |    <div class="card"><div class="card-val warn">$warning</div><div class="card-lbl">Warning</div></div>
       |    <div class="card"><div class="card-val info-n">$info</div><div class="card-lbl">Info</div></div>
       |  </div>
       |  $body
       |</div>
       |</body>
       |</html>""".stripMargin
  }

  private val css: String =
    """  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f9fafb;color:#111}
      |  .wrap{max-width:900px;margin:32px auto;padding:0 20px}
      |  h1{font-size:20px;margin:0 0 4px;font-weight:700}
      |  .meta{color:#6b7280;font-size:13px;margin-bottom:24px}
      |  .cards{display:flex;gap:12px;margin-bottom:28px;flex-wrap:wrap}
      |  .card{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px 20px;min-width:120px;text-align:center}
      |  .card-val{font-size:28px;font-weight:800}
      |  .denom{font-size:14px;font-weight:400}
      |  .card-lbl{font-size:12px;color:#6b7280;margin-top:2px}
      |  .score{color:#111}.crit{color:#dc2626}.warn{color:#d97706}.info-n{color:#2563eb}
      |  details.issue{border-radius:6px;margin-bottom:10px;overflow:hidden}
      |  details.issue.critical{border:1px solid #dc262640;border-left:4px solid #dc2626;background:#fef2f2}
      |  details.issue.warning {border:1px solid #d9770640;border-left:4px solid #d97706;background:#fffbeb}
      |  details.issue.info    {border:1px solid #2563eb40;border-left:4px solid #2563eb;background:#eff6ff}
      |  summary{display:flex;align-items:center;gap:8px;padding:12px 16px;cursor:pointer;list-style:none;user-select:none}
      |  summary::-webkit-details-marker{display:none}
      |  summary::before{content:'\25B6';font-size:9px;color:#9ca3af;transition:transform .15s;flex-shrink:0;display:inline-block}
      |  details[open]>summary::before{transform:rotate(90deg)}
      |  .badge{font-size:11px;font-weight:700;padding:2px 8px;border-radius:4px;letter-spacing:.5px;color:#fff;flex-shrink:0}
      |  .critical .badge{background:#dc2626}
      |  .warning  .badge{background:#d97706}
      |  .info     .badge{background:#2563eb}
      |  .issue-title{font-weight:600;font-size:14px;color:#111}
      |  .issue-body{padding:0 16px 14px 42px}
      |  .desc{margin:0 0 6px;color:#374151;font-size:13px;line-height:1.5}
      |  .rec{margin:0 0 4px;color:#374151;font-size:13px;line-height:1.5}
      |  .arrow{font-weight:700;margin-right:4px}
      |  .fix-label{font-size:11px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin:10px 0 3px}
      |  .fix-block{margin:0;background:#1e1e1e;color:#d4d4d4;padding:10px 12px;border-radius:4px;font-size:12px;overflow-x:auto;white-space:pre;line-height:1.5;font-family:'SFMono-Regular',Consolas,'Liberation Mono',Menlo,monospace}
      |  .locations{margin-top:10px;display:flex;flex-wrap:wrap;gap:5px}
      |  .pill{font-size:11px;background:#fff;border:1px solid #d1d5db;border-radius:4px;padding:2px 7px;color:#374151;font-family:'SFMono-Regular',Consolas,monospace}
      |  .no-issues{color:#15803d;font-weight:600;font-size:15px}""".stripMargin
}
