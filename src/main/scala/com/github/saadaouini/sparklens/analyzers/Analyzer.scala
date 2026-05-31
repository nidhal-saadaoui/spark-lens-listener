package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}
import java.util.Locale

trait Analyzer {
  def analyze(app: SparkAppModel): Seq[Issue]

  protected val MB: Long = 1024L * 1024L
  protected val GB: Long = 1024L * MB

  protected def median(sorted: Seq[Long]): Long =
    if (sorted.isEmpty) 0L else sorted(sorted.size / 2)

  // Locale.ROOT ensures dot as decimal separator regardless of JVM locale
  protected def fmtDouble(value: Double, decimals: Int): String =
    String.format(Locale.ROOT, s"%.${decimals}f", value: java.lang.Double)

  protected def fmtBytes(bytes: Long): String = {
    if (bytes >= GB)      s"${fmtDouble(bytes.toDouble / GB, 1)} GB"
    else if (bytes >= MB) s"${fmtDouble(bytes.toDouble / MB, 1)} MB"
    else                  s"${bytes} B"
  }

  protected def fmtMs(ms: Long): String = {
    if (ms >= 3600000) s"${fmtDouble(ms.toDouble / 3600000, 1)}h"
    else if (ms >= 60000) s"${fmtDouble(ms.toDouble / 60000, 1)}m"
    else if (ms >= 1000)  s"${fmtDouble(ms.toDouble / 1000, 1)}s"
    else s"${ms}ms"
  }
}
