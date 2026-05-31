package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}

trait Analyzer {
  def analyze(app: SparkAppModel): Seq[Issue]

  protected val MB: Long = 1024L * 1024L
  protected val GB: Long = 1024L * MB

  protected def median(sorted: Seq[Long]): Long =
    if (sorted.isEmpty) 0L else sorted(sorted.size / 2)

  protected def fmtBytes(bytes: Long): String = {
    if (bytes >= GB)      f"${bytes.toDouble / GB}%.1f GB"
    else if (bytes >= MB) f"${bytes.toDouble / MB}%.1f MB"
    else                  s"${bytes} B"
  }

  protected def fmtMs(ms: Long): String = {
    if (ms >= 3600000) f"${ms.toDouble / 3600000}%.1fh"
    else if (ms >= 60000) f"${ms.toDouble / 60000}%.1fm"
    else if (ms >= 1000)  f"${ms.toDouble / 1000}%.1fs"
    else s"${ms}ms"
  }
}
