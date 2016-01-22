package com.maascamp.fohlc;


import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

@SuppressWarnings("all")
public class TestByteToLongHashTable {

  @Test
  public void testBookKeeping() {

    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .setProbeMax(100)
        .setDrainThreshold(100)
        .setNeighborhoodSize(64)
        .build()
    ) {
      for (int i = 0; i < 10; i++) {
        cache.put(("string" + i).getBytes(), i);
      }

      CacheMetrics metrics = cache.getCacheMetrics();
      assertTrue("Expected 10", metrics.numEntries == 10);
      assertTrue("Expected 128", metrics.numBuckets == 128);
      assertTrue("Expected 3072 (128 * 32)", metrics.sizeInBytes == 4096);
      assertTrue("Expected 0.8 (10 / 128)", metrics.loadFactor == 0.08);
    }
  }

  @Test
  public void testGet() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      for (int i = 0; i < 10; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }

      for (int i = 0; i < 10; i++) {
        assertEquals((long) cache.get(String.format("string%d", i).getBytes()), i);
      }
    }
  }

  @Test
  public void testGetAndPutIfEmpty() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      Long existing = cache.getAndPutIfEmpty("test".getBytes(), 100L);
      assertEquals(existing, null);

      existing = cache.getAndPutIfEmpty("test".getBytes(), 200L);
      assertTrue(existing == 100L);
    }
  }

  @Test
  public void testGetOldest() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      for (int i = 1; i <= 10; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }

      assertEquals(1L, (long) cache.getOldestEntry());
    }
  }

  @Test
  public void testEvictions() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      CacheMetrics metrics = cache.getCacheMetrics();
      for (int i = 1; i <= (metrics.numBuckets * 2); i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }

      metrics = cache.getCacheMetrics();
      assertTrue(metrics.evictions > metrics.numBuckets);
    }
  }

  @Test
  public void testFifoConcurrentEvictions() throws InterruptedException {
    AtomicInteger evictionCount = new AtomicInteger(0);
    AtomicLong expected = new AtomicLong(0);
    AtomicBoolean monotonicallyIncreasingEvictions = new AtomicBoolean(true);
    FifoOffHeapLongCache.EvictionListener listener = spy(
        new FifoOffHeapLongCache.EvictionListener() {
          @Override
          public void onEvict(long key, long value) {
            if (evictionCount.incrementAndGet() < 800 // <- size * evictionThreshold
                && expected.incrementAndGet() != value) {
              monotonicallyIncreasingEvictions.set(false);
            }
          }
        });
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(1000L)
        .setEvictionThreshold(0.80)
        .setEvictionListener(listener)
        .build()
    ) {

      CacheMetrics metrics = cache.getCacheMetrics();
      final long numBuckets = metrics.numBuckets;
      List<Thread> threads = Lists.newArrayList();
      final AtomicLong id = new AtomicLong(0);
      for (int i = 0; i < 8; i++) {
        threads.add(new Thread(() -> {
          long start = System.nanoTime();
          try {
            for (int j = 1; j <= numBuckets; j++) {
              String key = String.format("string-%d", j);
              cache.put(key.getBytes(), j);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }));
      }

      long start = System.nanoTime();
      threads.forEach(t -> t.start());
      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
        }
      });

      // ensure that the first (size * eviction threshold)
      // entries evicted are monotonically increasing
      assertTrue(monotonicallyIncreasingEvictions.get());
    }
  }
}
