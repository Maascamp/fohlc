package com.maascamp.folc;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import sun.misc.Unsafe;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

public class ByteToLongHashTable {

  /**
   * Buckets are 24 bytes wide.
   * 64 bit key + 64 bit value + 64 bit pointer to next bucket.
   */
  private static final int BUCKET_SIZE = 24;

  /**
   * Byte offset of the value within a bucket.
   */
  private static final int VALUE_OFFSET = 8;

  /**
   * Byte offset of the next bucket pointer within a bucket.
   */
  private static final int POINTER_OFFSET = 16;

  /**
   * Load factor threshold beyond which eviction begins.
   */
  private static final double EVICTION_THRESHOLD = 0.85;

  private static final long EMPTY = 0L;

  /**
   * Neighborhood for linear probing.
   * See <a href="https://en.wikipedia.org/wiki/Hopscotch_hashing">
   * Hopscotch Hashing</a>
   */
  private static final int NEIGHBORHOOD = 64;

  /**
   * Maximum number of probes for an empty bucket before we
   * declare failure. The chances of not finding an open slot
   * in {@link #NEIGHBORHOOD} probes is 1/{@link #NEIGHBORHOOD}! (factorial)
   */
  private static final int PROBE_MAX = 8192;

  // Bookkeeping properties for async bookkeeping.
  // see https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design
  private final Lock evictionLock;
  private final Queue<Runnable> taskBuffer;
  private final AtomicReference<DrainStatus> drainStatus;
  private static final int WRITE_BUFFER_DRAIN_THRESHOLD = 16;

  private final long sizeInBytes;
  private final long numBuckets;
  private final long memStart;
  private final Unsafe unsafe;
  private final HashFunction hashFunction;
  private final AtomicLong numEntries = new AtomicLong(0L);
  private final AtomicLong hits = new AtomicLong(0L);
  private final AtomicLong misses = new AtomicLong(0L);
  private final AtomicLong evictions = new AtomicLong(0L);
  private final AtomicLong fifoHead = new AtomicLong(-1L);
  private final AtomicLong fifoTail = new AtomicLong(-1L);

  // TODO: add the option to have bookeeping done in a separate thread.
  public ByteToLongHashTable(long numEntries) {
    this.evictionLock = new ReentrantLock();
    this.taskBuffer = new ConcurrentLinkedQueue<>();
    this.drainStatus = new AtomicReference<>(DrainStatus.IDLE);
    this.sizeInBytes = ceilingNextPowerOfTwo(numEntries) * BUCKET_SIZE;
    this.numBuckets = sizeInBytes / BUCKET_SIZE;

    this.unsafe = UnsafeAccess.getUnsafe();
    this.hashFunction = Hashing.murmur3_128(); // should we be using the seeded variant?

    this.memStart = this.unsafe.allocateMemory(this.sizeInBytes);
    this.unsafe.setMemory(this.memStart, this.sizeInBytes, (byte) 0); // zero out memory
  }

  /**
   * Release the off-heap memory associated with this cache.
   */
  public void destroy() {
    unsafe.freeMemory(memStart);
  }

  /**
   * Generate a CacheMetrics object representing the current state
   * of the cache.
   * @return {@link CacheMetrics}
   */
  public CacheMetrics getCacheMetrics() {
    return new CacheMetrics(
      sizeInBytes,
      numBuckets,
      numEntries.get(),
      getLoadFactor(),
      hits.get(),
      misses.get(),
      evictions.get()
    );
  }

  /**
   * Associate the specified value with the specified key in the cache
   * if the key does not currently exist in the cache.
   *
   * If the key already exists in the cache this method is a no-op.
   */
  public void put(byte[] key, long value) {
    // if hash table already contains value do nothing
    final Long val = _get(key);
    if (val == null) {
      _put(key, value);
    }
  }

  /**
   * Unconditionally associate the specified value with the specified key.
   */
  private void _put(byte[] key, long value) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    // keep trying until we get a stable write
    for (;;) {
      long address = addressFromIndex(findAvailableBucket(index));
      if (unsafe.compareAndSwapLong(null, address, EMPTY, hashCode)) {
        unsafe.putLongVolatile(null, address + VALUE_OFFSET, value);
        queueWrite(new WriteTask(address)); // queue the bookkeeping for async processing
        return;
      }
    }
  }

  /**
   * Return the value associated with the specified key.
   *
   * @return The value associated with the specified key or null
   * if it doesn't exist.
   */
  public Long get(byte[] key) {
    final Long val = _get(key);
    if (val == null) {
      misses.incrementAndGet();
    } else {
      hits.incrementAndGet();
    }
    return val;
  }

  /**
   * Return the value associated with the specified key if it exists.
   * If the key does not exist, execute a {@link #put(byte[], long)}.
   *
   * @return The associated value if it exists or null
   */
  public Long getAndPutIfEmpty(byte[] key, long value) {
    final Long val = _get(key);
    if (val == null) {
      _put(key, value);
      misses.incrementAndGet();
      return null;
    }

    hits.incrementAndGet();
    return val;
  }

  /**
   * Since we use hopscotch hashing, key is guaranteed to be within H steps of index.
   */
  private Long _get(byte[] key) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    // we start at the bucket index then step forward up to
    // NEIGHBORHOOD - 1 times
    for (int i=0; i< NEIGHBORHOOD; i++) {
      long bucketHashCode = unsafe.getLong(addressFromIndex((index + i) % numBuckets));
      if (bucketHashCode == hashCode) {
        // we've found it so return the corresponding value
        return unsafe.getLong(addressFromIndex((index + i) % numBuckets) + VALUE_OFFSET);
      }
    }

    // if we got here the value does not exist in the hash table
    return null;
  }

  private long findAvailableBucket(long index) {
    // see if we find a free bucket in the neighborhood
    for (int i=0; i < NEIGHBORHOOD; i++) {
      long candidateIndex = (index + i) % numBuckets;
      long bucketHashCode = unsafe.getLongVolatile(null, addressFromIndex(candidateIndex));
      if (bucketHashCode == EMPTY) {
        // we've found it so return the index
        return candidateIndex;
      }
    }

    // if we got here we were unable to find an empty bucket in the neighborhood
    long emptyIndex = -1L;
    long probeStart = index + NEIGHBORHOOD;
    for (int i=0; i < PROBE_MAX; i += BUCKET_SIZE) {
      long bucketHashCode = unsafe.getLongVolatile(null, addressFromIndex((probeStart + i) % numBuckets));
      if (bucketHashCode == EMPTY) {
        emptyIndex = (probeStart + i) % numBuckets;
        break;
      }
    }

    if (emptyIndex < 0) {
      throw new RuntimeException(String.format(
          "Unable to find a empty bucket after examining %d buckets", PROBE_MAX));
    }

    // now we swap back until the empty bucket is in the neighborhood
    long emptyAddress = addressFromIndex(emptyIndex);
    while (emptyIndex - index > NEIGHBORHOOD) {
      boolean foundSwap = false;
      long minBaseIndex = emptyIndex - (NEIGHBORHOOD - 1);
      for(int i = NEIGHBORHOOD - 1; i > 0; i--) {
        long candidateIndex = emptyIndex - i;
        if (candidateIndex < index) {
          // we can't use buckets less than our initial index
          continue;
        }

        long candidateAddress = addressFromIndex(candidateIndex);
        if (getIndex(unsafe.getLong(candidateAddress)) >= minBaseIndex) {
          // swap candidate vals to empty vals
          unsafe.putLong( // swap hashCode
              emptyAddress,
              unsafe.getLong(candidateAddress));
          unsafe.putLong( // swap value
              emptyAddress + VALUE_OFFSET,
              unsafe.getLong(candidateAddress + VALUE_OFFSET));
          unsafe.putLong( // swap pointer
              emptyAddress + POINTER_OFFSET,
              unsafe.getLong(candidateAddress + POINTER_OFFSET));

          // empty out candidate
          unsafe.setMemory(candidateAddress, BUCKET_SIZE, (byte) 0);

          emptyIndex = candidateIndex;
          foundSwap = true;
          break;
        }
      }

      if (!foundSwap) {
        // we were unable to find an open bucket to swap with, this is bad
        throw new RuntimeException(
            "No swap candidates in neighborhood, unable to move empty bucket");
      }
    }

    return emptyIndex % numBuckets;
  }

  private void queueWrite(WriteTask task) {
    taskBuffer.add(task);
    drainStatus.lazySet(DrainStatus.REQUIRED);
    tryDrainingBuffer();
  }

  private void tryDrainingBuffer() {
    if (evictionLock.tryLock()) {
      try {
        drainStatus.lazySet(DrainStatus.PROCESSING);
        drainBuffer();
        evict();
      } finally {
        drainStatus.compareAndSet(DrainStatus.PROCESSING, DrainStatus.IDLE);
        evictionLock.unlock();
      }
    }
  }

  @GuardedBy("evictionLock")
  private void drainBuffer() {
    for (int i = 0; i < WRITE_BUFFER_DRAIN_THRESHOLD; i++) {
      final Runnable task = taskBuffer.poll();
      if (task == null) {
        break;
      }
      task.run();
    }
  }

  @GuardedBy("evictionLock")
  private boolean hasOverflowed() {
    return getLoadFactor() > EVICTION_THRESHOLD;
  }

  @GuardedBy("evictionLock")
  private void deleteBucket(long address) {
    unsafe.setMemory(address, BUCKET_SIZE, (byte) 0);
  }

  @GuardedBy("evictionLock")
  private void evict() {
    while (hasOverflowed()) {
      for (;;) {
        long address = fifoHead.get();
        long hashCode = unsafe.getLongVolatile(null, address);
        if (unsafe.compareAndSwapLong(null, address, hashCode, EMPTY)) {
          fifoHead.set(unsafe.getLong(address + POINTER_OFFSET));
          deleteBucket(address);
          numEntries.decrementAndGet();
          evictions.incrementAndGet();
          return;
        }
      }
    }
  }

  /**
   * Maps a hash code into the bucket range.
   */
  private long getIndex(long hashCode) {
    return Math.abs(hashCode % numBuckets);
  }

  /**
   * Returns a pointer to the actual memory location referenced by the index.
   */
  private long addressFromIndex(long index) {
    return (index * BUCKET_SIZE) + memStart;
  }

  /**
   * Generate a 64 bit hash code from the specified byte array.
   */
  private long hash(byte[] val) {
    return hashFunction.newHasher(val.length)
        .putBytes(val)
        .hash()
        .asLong();
  }

  /**
   * Returns the load factor to 2 decimal places.
   */
  private double getLoadFactor() {
    return ((double) Math.round(((double) numEntries.get() / (double) numBuckets) * 100)) / 100;
  }

  private long ceilingNextPowerOfTwo(long x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << (Long.SIZE - Long.numberOfLeadingZeros(x - 1));
  }

  /**
   * Enum to track status of {@link #taskBuffer} processing.
   */
  private enum DrainStatus {
    IDLE,
    REQUIRED,
    PROCESSING
  }

  /**
   * Runnable that performs the necessary bookkeeping after
   * a write to the hash table.
   */
  private class WriteTask implements Runnable {

    public final long address;

    public WriteTask(long address) {
      this.address = address;
    }

    @Override
    @GuardedBy("evictionLock")
    public void run() {
      if (numEntries.getAndIncrement() == 0L) {
        // first entry into the hash table
        fifoHead.set(address);
        fifoTail.set(address);
      } else {
        // update the previous entry's pointer to  point to the new
        // entry and update the tail pointer
        unsafe.putLongVolatile(null, fifoTail.get() + POINTER_OFFSET, address);
        fifoTail.set(address);
      }

      // set the newly added entry's pointer to NOT_FOUND
      unsafe.putLongVolatile(null, address + POINTER_OFFSET, -1L);
    }
  }
}
