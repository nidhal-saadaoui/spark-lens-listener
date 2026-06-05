package com.github.saadaouini.sparklens.model

/** Snapshot of an RDD's cache state at stage submission time. */
case class RddCacheInfo(
  name:             String,
  numPartitions:    Int,
  cachedPartitions: Int,
  memSizeBytes:     Long,
  diskSizeBytes:    Long,
  storageLevel:     String,   // e.g. "MEMORY_AND_DISK_SER"
)
