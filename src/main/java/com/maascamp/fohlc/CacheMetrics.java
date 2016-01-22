package com.maascamp.fohlc;

import com.google.common.base.MoreObjects;

public class CacheMetrics {

  public final long sizeInBytes;
  public final long numBuckets;
  public final long numEntries;
  public final double loadFactor;
  public final long hits;
  public final long misses;
  public final long evictions;

  public CacheMetrics(
      long sizeInBytes,
      long numBuckets,
      long numEntries,
      double loadFactor,
      long hits,
      long misses,
      long evictions
  ) {
    this.sizeInBytes = sizeInBytes;
    this.numBuckets = numBuckets;
    this.numEntries = numEntries;
    this.loadFactor = loadFactor;
    this.hits = hits;
    this.misses = misses;
    this.evictions = evictions;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sizeInBytes", sizeInBytes)
        .add("numBuckets", numBuckets)
        .add("numEntries", numEntries)
        .add("loadFactor", String.format("%.2f", loadFactor))
        .add("hits", hits)
        .add("misses", misses)
        .add("evictions", evictions)
        .toString();
  }
}
