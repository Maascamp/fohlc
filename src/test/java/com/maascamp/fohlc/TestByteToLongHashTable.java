package com.maascamp.fohlc;


import com.google.common.collect.Lists;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jdk.nashorn.internal.ir.annotations.Ignore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("all")
public class TestByteToLongHashTable {

  @Test
  public void testCacheMetrics() {

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
      assertTrue(metrics.numEntries == 10);
      assertTrue(metrics.numBuckets == 128);
      assertTrue(metrics.sizeInBytes == 4096);
      assertTrue(metrics.loadFactor == 0.08); // 10/128
    }
  }

  @Test(expected = NullPointerException.class)
  public void testGet_Null() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      cache.get(null);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testPut_Null() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      cache.put(null, 1);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testGetAndPutIfEmpty_Null() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      cache.getAndPutIfEmpty(null, 1);
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
      assertNull(existing);

      existing = cache.getAndPutIfEmpty("test".getBytes(), 200L);
      assertEquals((long) existing, 100L);
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
  public void testPutDuplicate() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      for (int i = 1; i <= 10; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }
      long size = cache.getCacheMetrics().numEntries;

      // try to add the same values again
      for (int i = 1; i <= 10; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }

      assertEquals(size, (long) cache.getCacheMetrics().numEntries);
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
  public void testEvictOldest() {
    double evictionThreshold = 0.8;
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .setEvictionThreshold(evictionThreshold)
        .build()
    ) {
      CacheMetrics metrics = cache.getCacheMetrics();
      long actualSize = (long) (metrics.numBuckets * evictionThreshold);
      for (int i = 1; i <= actualSize + 1; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }

      assertNull(cache.get("string1".getBytes()));
    }
  }

  @Test
  public void testFifoConcurrentEvictions() throws InterruptedException {
    AtomicInteger evictionCount = new AtomicInteger(0);
    AtomicLong expected = new AtomicLong(0);
    AtomicBoolean monotonicallyIncreasingEvictions = new AtomicBoolean(true);
    FifoOffHeapLongCache.EvictionListener listener = new FifoOffHeapLongCache.EvictionListener() {
          @Override
          public void onEvict(long key, long value) {
            if (evictionCount.incrementAndGet() < 800 // <- size * evictionThreshold
                && expected.incrementAndGet() != value) {
              monotonicallyIncreasingEvictions.set(false);
            }
          }
        };
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(1000L)
        .setEvictionThreshold(0.80)
        .setEvictionListener(listener)
        .build()
    ) {

      CacheMetrics metrics = cache.getCacheMetrics();
      final long numBuckets = metrics.numBuckets;
      final List<Thread> threads = Lists.newArrayList();
      final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
      for (int i = 0; i < 8; i++) {
        threads.add(new Thread(() -> {
          try {
            for (int j = 1; j <= numBuckets; j++) {
              String key = String.format("string-%d", j);
              cache.put(key.getBytes(), j);
            }
          } catch (Exception e) {
            exceptions.add(e);
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
      double took = (double) (System.nanoTime() - start) / 1000000000;

      // ensure that the first (size * eviction threshold)
      // entries evicted are monotonically increasing
      assertTrue(monotonicallyIncreasingEvictions.get());
      assertEquals(exceptions.size(), 0);
    }
  }

  @Test
  public void testInduceDeadlock() {
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(1000L)
        .setNeighborhoodSize(64)
        .setProbeMax(1024)
        .setEvictionThreshold(0.80)
        .setDrainThreshold(128)
        .build()
    ) {
      CacheMetrics metrics = cache.getCacheMetrics();
      final long numBuckets = metrics.numBuckets;
      final List<Thread> threads = Lists.newArrayList();
      final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
      final AtomicInteger idGenerator = new AtomicInteger(0);
      for (int i = 0; i < 8; i++) {
        threads.add(new Thread(() -> {
          int id = idGenerator.incrementAndGet();
          try {
            for (int j = 1; j <= 1000; j++) {
              String key = String.format("string-%d-%d", id, j);
              cache.put(key.getBytes(), j);
            }
          } catch (Exception e) {
            exceptions.add(e);
          }
        }));
      }

      threads.forEach(t -> t.start());
      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
        }
      });
    }
  }

  @Test
  public void testPersistRestore() throws IOException {
    Path path = Paths.get(System.getProperty("java.io.tmpdir"));

    // write to cache and persist it
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(100L)
        .build()
    ) {
      for (int i = 0; i < 80; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
      }
      FifoOffHeapLongCache.saveTo(path, cache);
    }

    // load from persisted cache file and read back values
    try {
      FifoOffHeapLongCache cache = FifoOffHeapLongCache.loadFrom(path);
      assertEquals(cache.getOldestEntry().longValue(), 0L);
      try {
        Long value;
        for (int i = 0; i < 80; i++) {
          value = cache.get(String.format("string%d", i).getBytes());
          assertNotNull(value);
          assertEquals(value.longValue(), i);
        }
      } finally {
        cache.close();
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed deserializing cache");
    }
  }

  @Ignore @Test
  public void testPersistRestoreLargeCache() throws IOException {
    Path path = Paths.get(System.getProperty("java.io.tmpdir"));

    // write to cache and persist it
    try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
        .setSize(68000000L)
        .build()
    ) {
      long puts = 0L;
      for (long i = 0; i < 34000000L; i++) {
        cache.put(String.format("string%d", i).getBytes(), i);
        puts++;
      }

      long start = System.nanoTime();
      FifoOffHeapLongCache.saveTo(path, cache);
      System.out.println("Finished writing cache in " + ((System.nanoTime() - start) / (double) 1000000000) +  " seconds");
      System.out.println("Cache size (in bytes): " + cache.getCacheMetrics().sizeInBytes);
      System.out.println("Persisted " + puts + " entries (" + path.toFile().length() + ") to " + path.toAbsolutePath().toString());
    }

    // load from persisted cache file and read back values
    try {
      long start = System.nanoTime();
      FifoOffHeapLongCache cache = FifoOffHeapLongCache.loadFrom(path);
      System.out.println(
          "Finished loading cache in " + ((System.nanoTime() - start) / (double) 1000000000)
          + " seconds");
      System.out.println(
          "Loaded " + path.toFile().length() + " bytes from " + path.toAbsolutePath().toString());
      try {
        Long value;
        for (long i = 0; i < 34000000L; i++) {
          value = cache.get(String.format("string%d", i).getBytes());
          assertNotNull("null key: " + String.format("string%d", i), value);
          assertEquals(value.longValue(), i);
        }
      } finally {
        cache.close();
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed deserializing cache");
    }
  }
}
