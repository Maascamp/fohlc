package com.maascamp.fohlc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBuilder {

  @Test
  public void testSizePowerOfTwo() {
    FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100)
        .build();

    CacheMetrics metrics = cache.getCacheMetrics();
    assertEquals(metrics.numBuckets, 128);
  }

  @Test(expected = IllegalStateException.class)
  public void testZeroSize() {
    new FifoOffHeapLongCache.Builder()
        .setSize(0)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testNegativeSize() {
    new FifoOffHeapLongCache.Builder()
        .setSize(-1)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testMissingEvictionListener() {
    new FifoOffHeapLongCache.Builder()
        .setSize(100)
        .setEvictionListener(null)
        .build();
  }
}
