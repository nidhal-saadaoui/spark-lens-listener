package com.github.saadaouini.sparklens.model

/**
 * A quantified estimate of the cost of an issue and the savings from fixing it.
 *
 * @param summary     human-readable one-liner shown in text/JSON reports
 * @param savedTimeMs estimated wall-clock ms saved per run (used for sorting issues by impact)
 * @param savedBytes  estimated bytes eliminated per run (network or I/O)
 * @param confidence  "high"   — derived from directly measured data (no speed assumptions)
 *                   "medium" — requires one speed assumption (bytes ÷ network/disk rate)
 *                   "low"    — conceptual only; no numeric derivation possible
 */
case class EstimatedImpact(
  summary:     String,
  savedTimeMs: Option[Long],
  savedBytes:  Option[Long],
  confidence:  String,
)
