package com.github.saadaouini.sparklens.analyzers

import com.github.saadaouini.sparklens.model.{Issue, SparkAppModel}
import java.util.Locale

trait Analyzer {
  def analyze(app: SparkAppModel): Seq[Issue]

  protected val MB: Long = 1024L * 1024L
  protected val GB: Long = 1024L * MB

  protected def median(sorted: Seq[Long]): Long =
    if (sorted.isEmpty) 0L else sorted(sorted.size / 2)

  // Linear-interpolated percentile on a pre-sorted sequence
  protected def percentile(sorted: Seq[Long], p: Double): Long = {
    if (sorted.isEmpty) return 0L
    val idx  = (sorted.size - 1).toDouble * p / 100.0
    val lo   = idx.toInt
    val hi   = math.min(lo + 1, sorted.size - 1)
    val frac = idx - lo
    (sorted(lo) * (1.0 - frac) + sorted(hi) * frac).toLong
  }

  // Fraction of total held by the top `topFrac` items (sorted ascending)
  protected def concentration(sorted: Seq[Long], topFrac: Double = 0.05): Double = {
    val total = sorted.sum.toDouble
    if (total == 0.0) return 0.0
    val nTop   = math.max(1, (sorted.size * topFrac).toInt)
    val topSum = sorted.takeRight(nTop).sum.toDouble
    topSum / total
  }

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

  protected def propLong(app: SparkAppModel, key: String, default: Long): Long =
    app.prop(key).flatMap(s => scala.util.Try(s.toLong).toOption).getOrElse(default)

  protected def propDouble(app: SparkAppModel, key: String, default: Double): Double =
    app.prop(key).flatMap(s => scala.util.Try(s.toDouble).toOption).getOrElse(default)
}
