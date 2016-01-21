package com.maascamp.fohlc;


import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("all")
public class TestByteToLongHashTable {

  private FifoOffHeapLongCache cache;

  @Before
  public void setUp() {
    this.cache = new FifoOffHeapLongCache(100L);
  }

  @After
  public void tearDown() {
    this.cache.destroy();
  }

  @Test
  public void testBookKeeping() {
    for (int i=0; i < 10; i++) {
      cache.put(("string" + i).getBytes(), i);
    }

    CacheMetrics metrics = cache.getCacheMetrics();
    assertTrue("Expected 10", metrics.numEntries == 10);
    assertTrue("Expected 128", metrics.numBuckets == 128);
    assertTrue("Expected 3072 (128 * 32)", metrics.sizeInBytes == 4096);
    assertTrue("Expected 0.8 (10 / 128)", metrics.loadFactor == 0.08);
  }

  @Test
  public void testGet() {
    for (int i=0; i < 10; i++) {
      cache.put(String.format("string%d", i).getBytes(), i);
    }

    for (int i=0; i < 10; i++) {
      long value = cache.get(String.format("string%d", i).getBytes());
      assertTrue(value == i);
    }
  }

  @Test
  public void testGetAndPutIfEmpty() {
    Long existing = cache.getAndPutIfEmpty("test".getBytes(), 100L);
    assertEquals(existing, null);

    existing = cache.getAndPutIfEmpty("test".getBytes(), 200L);
    assertTrue(existing == 100L);
  }

  @Test
  public void testGetOldest() {
    for (int i=1; i <= 10; i++) {
      cache.put(String.format("string%d", i).getBytes(), i);
    }

    assertEquals(1L, (long) cache.getOldestEntry());
  }

  @Test
  public void testEvictions() {
    CacheMetrics metrics = cache.getCacheMetrics();
    for (int i=1; i <= (metrics.numBuckets * 2); i++) {
      cache.put(String.format("string%d", i).getBytes(), i);
    }

    metrics = cache.getCacheMetrics();
    assertTrue(metrics.evictions > metrics.numBuckets);
  }

  @Test
  public void testFifoConcurrentEvictions() throws InterruptedException {

    AtomicInteger evictionCount = new AtomicInteger(0);
    AtomicBoolean monotonicallyIncreasingEvictions = new AtomicBoolean(true);

    this.cache.destroy();
    this.cache = new FifoOffHeapLongCache(1000L, new FifoOffHeapLongCache.EvictionListener() {

      private final AtomicLong expected = new AtomicLong(0);

      @Override
      public void onEvict(long key, long value) {
        if (evictionCount.incrementAndGet() < 800
            && expected.incrementAndGet() != value) {
          monotonicallyIncreasingEvictions.set(false);
        }
      }
    });

    CacheMetrics metrics = cache.getCacheMetrics();
    final long numBuckets = metrics.numBuckets;
    List<Thread> threads = Lists.newArrayList();
    final AtomicLong id = new AtomicLong(0);
    for (int i=0; i<8; i++) {
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
      } catch (InterruptedException e) {}
    });

    // we check to ensure that the first (size * eviction threshold)
    // entries evicted are monotonically increasing
    assertTrue(monotonicallyIncreasingEvictions.get());
  }
}
