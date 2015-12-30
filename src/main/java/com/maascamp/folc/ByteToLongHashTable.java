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

  /* each bucket is 24 bytes wide (64 bit key + 64 bit val + 64 bit pointer) */
  private static final int BUCKET_SIZE = 24;
  private static final int BUCKET_VALUE_OFFSET = 8;
  private static final int BUCKET_POINTER_OFFSET = 16;
  private static final double EVICTION_THRESHOLD = 0.85;
  private static final long EMPTY_VAL = 0L;
  private static final long NOT_FOUND = -1L;

  // Hopscotch values
  private static final int NEIGHBORHOOD = 64;
  private static final int PROBE_MAX = 8192;

  // Write bookkeeping for lock amortization.
  // see https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design
  private final Lock evictionLock;
  private final Queue<Runnable> taskBuffer;
  private final AtomicReference<DrainStatus> drainStatus;
  private static final int WRITE_BUFFER_DRAIN_THRESHOLD = 16;

  private final Unsafe unsafe;
  private final HashFunction hashFunction;
  private final long sizeInBytes;
  private final long memStart;
  private final long numBuckets;

  private final AtomicLong numEntries = new AtomicLong(0L);
  private final AtomicLong fifoHead = new AtomicLong(-1L);
  private final AtomicLong fifoTail = new AtomicLong(-1L);

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

  public void destroy() {
    unsafe.freeMemory(memStart);
  }

  public long getSize() {
    return numEntries.get();
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }

  public double getLoadFactor() {
    return ((double) Math.round(((double) numEntries.get() / (double) numBuckets) * 100)) / 100;
  }

  /**
   *
   * NOTE: A zero value is assumed to be empty by the cache!
   *
   * @param val
   * @return
   */
  public void put(byte[] key, long val) {
    // if hash table already contains value do nothing
    final long current = _get(key);
    if (current == NOT_FOUND) {
      _put(key, val);
    }
  }

  /**
   * Insert using Hopscotch hashing for collision resolution.
   * @param key
   * @param value
   * @return
   */
  private void _put(byte[] key, long value) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    for (;;) {
      long address = addressFromIndex(findAvailableBucket(index));
      if (unsafe.compareAndSwapLong(null, address, EMPTY_VAL, hashCode)) {
        unsafe.putLongVolatile(null, address + BUCKET_VALUE_OFFSET, value);
        queueWrite(new WriteTask(address)); // queue the bookkeeping for async processing
        return;
      }
    }
  }

  public long get(byte[] key) {
    return _get(key);
  }

  public long getAndPutIfEmpty(byte[] key, long value) {
    long foundVal = _get(key);
    if (foundVal == NOT_FOUND) {
      _put(key, value);
      return NOT_FOUND;
    }

    return foundVal;
  }

  /**
   * Since we use hopscotch hashing, key is guaranteed to be within H steps of index.
   */
  private long _get(byte[] key) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    // we start at the bucket index then step forward up to
    // NEIGHBORHOOD - 1 times
    for (int i=0; i< NEIGHBORHOOD; i++) {
      long bucketHashCode = unsafe.getLong(addressFromIndex((index + i) % numBuckets));
      if (bucketHashCode == hashCode) {
        // we've found it so return the corresponding value
        return unsafe.getLong(addressFromIndex((index + i) % numBuckets) + BUCKET_VALUE_OFFSET);
      }
    }

    // if we got here the value does not exist in the hash table
    return NOT_FOUND;
  }

  private long findAvailableBucket(long index) {
    // see if we find a free bucket in the neighborhood
    for (int i=0; i < NEIGHBORHOOD; i++) {
      long bucketHashCode = unsafe.getLong(addressFromIndex((index + i) % numBuckets));
      if (bucketHashCode == EMPTY_VAL) {
        // we've found it so return the index
        return index + i;
      }
    }

    // if we got here we were unable to find an empty bucket in the neighborhood
    boolean found = false;
    long currentIndex = index + NEIGHBORHOOD;
    for (int i=0; i < PROBE_MAX; i += BUCKET_SIZE) {
      long bucketHashCode = unsafe.getLong(addressFromIndex((index + i) % numBuckets));
      if (bucketHashCode == EMPTY_VAL) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw new RuntimeException(String.format(
          "Unable to find a empty bucket after examining %d buckets", PROBE_MAX));
    }

    // now we swap back until the empty bucket is in the neighborhood
    int numSwaps = 0;
    long emptyIndex = currentIndex;
    long emptyAddress = addressFromIndex(emptyIndex % numBuckets);
    while (emptyIndex - index > NEIGHBORHOOD) {
      boolean foundSwap = false;
      long minBaseIndex = emptyIndex - (NEIGHBORHOOD - 1);
      for(int i = NEIGHBORHOOD - 1; i > 0; i--) {
        long candidate = emptyIndex - i;
        if (candidate < index) {
          // we can't use buckets less than our initial index
          continue;
        }

        long candidateAddress = addressFromIndex(candidate % numBuckets);
        if (getIndex(unsafe.getLong(candidateAddress)) >= minBaseIndex) {
          // swap candidate vals to empty vals
          unsafe.putLong( // swap hashCode
              emptyAddress,
              unsafe.getLong(candidateAddress));
          unsafe.putLong( // swap value
              emptyAddress + BUCKET_VALUE_OFFSET,
              unsafe.getLong(candidateAddress + BUCKET_VALUE_OFFSET));
          unsafe.putLong( // swap pointer
              emptyAddress + BUCKET_POINTER_OFFSET,
              unsafe.getLong(candidateAddress + BUCKET_POINTER_OFFSET));

          // empty out candidate
          unsafe.setMemory(candidateAddress, BUCKET_SIZE, (byte) 0);

          emptyIndex = candidate;
          foundSwap = true;
          numSwaps++;
          break;
        }
      }

      if (!foundSwap) {
        // we were unable to find an open bucket to swap with, this is bad
        throw new RuntimeException(String.format(
            "No swap candidates in neighborhood, unable to move empty bucket"));
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
  private void evict() {
    while (hasOverflowed()) {

    }
  }

  /**
   * Returns the index into the allocated memory by performing the following steps:
   * 1. Hash byte[] and mod it by getSize of memory
   * 2. Align to nearest multiple of BUCKET_SIZE by modding and adding/substracting the remainder
   */
  private long getIndex(long hashCode) {
    return Math.abs(hashCode % numBuckets);
  }

  private long addressFromIndex(long index) {
    return (index * BUCKET_SIZE) + memStart;
  }

  private long hash(byte[] val) {
    return hashFunction.newHasher(val.length)
        .putBytes(val)
        .hash()
        .asLong();
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

  private class WriteTask implements Runnable {

    public final long address;

    public WriteTask(long address) {
      this.address = address;
    }

    @Override
    @GuardedBy("evictionLock")
    public void run() {
      if (numEntries.getAndIncrement() == 0L) {
        unsafe.putLongVolatile(null, address + BUCKET_POINTER_OFFSET, -1L);
        fifoHead.set(address);
        fifoTail.set(address);
      } else {
        unsafe.putLongVolatile(null, address + BUCKET_POINTER_OFFSET, fifoTail.get());
        fifoTail.set(address);
      }
    }
  }
}
