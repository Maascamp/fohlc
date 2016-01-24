package com.maascamp.fohlc;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A lightweight off-heap cache with FIFO eviction semantics.
 *
 * As its name suggests, FifoOffHeapLongCache only stores long values.
 * THIS IS NOT A GENERAL PURPOSE Object CACHE. FifoOffHeapLongCache
 * uses Hopscotch hashing (a form of linear probing) for collision
 * resolution. Eviction begins when the load factor exceeds
 * {@link #evictionThreshold}.
 *
 * The bookkeeping strategy is modeled on the design used for
 * <a=href="https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design">
 * ConcurrentLinkedHashMap</a>. It amortizes bookkeeping and eviction across
 * threads. We aim to keep the load factor between 0.8 and 0.85.
 *
 * This cache explicitly does NOT offer delete/remove key functionality.
 */
public class FifoOffHeapLongCache implements AutoCloseable {

  /**
   * Load factor threshold beyond which eviction begins.
   */
  static final double DEFAULT_EVICTION_THRESHOLD = 0.80;

  /**
   * Neighborhood for linear probing.
   * See <a href="https://en.wikipedia.org/wiki/Hopscotch_hashing">
   * Hopscotch Hashing</a>
   */
  static final int DEFAULT_NEIGHBORHOOD_SIZE = 256;

  /**
   * Maximum number of probes for an empty bucket before we
   * declare failure. The chances of not finding an open slot
   * in {@link #neighborhoodSize} probes is 1/{@link #neighborhoodSize}! (factorial)
   * for a single neighborhood. However chances are actually much higher
   * since the above doesn't account for overlapping neighborhoods and so
   * doesn't necessarily prevent clustering.
   */
  static final int DEFAULT_PROBE_MAX = 8192;

  /**
   * Maximum number of tasks to process per call to {@link #drainBuffer()}.
   */
  static final int DEFAULT_DRAIN_THRESHOLD = 512;

  /**
   * Buckets are 32 bytes wide.
   * 8 byte key + 8 byte value + two 8 byte pointers to the next/prev buckets.
   */
  private static final int BUCKET_SIZE = 32;

  /**
   * Byte offset of the value within a bucket.
   */
  private static final int VALUE_OFFSET = 8;

  /**
   * Byte offset of the next bucket pointer within a bucket.
   */
  private static final int NEXT_POINTER_OFFSET = 16;

  /**
   * Byte offset of the next bucket pointer within a bucket.
   */
  private static final int PREV_POINTER_OFFSET = 24;

  /**
   * Special bucket values.
   */
  private static final long EMPTY = 0L;
  private static final long LOCK = -1L;

  private final Lock evictionLock;
  private final Queue<WriteTask> taskBuffer;

  private final long probeMax;
  private final long sizeInBytes;
  private final long numBuckets;
  private final long memStart;
  private final int neighborhoodSize;
  private final int drainThreshold;
  private final double evictionThreshold;
  private final Unsafe unsafe;
  private final HashFunction hashFunction;
  private final EvictionListener evictionListener;

  private final AtomicLong numEntries = new AtomicLong(0L);
  private final AtomicLong hits = new AtomicLong(0L);
  private final AtomicLong misses = new AtomicLong(0L);
  private final AtomicLong evictions = new AtomicLong(0L);
  private final AtomicLong fifoHead = new AtomicLong(-1L);
  private final AtomicLong fifoTail = new AtomicLong(-1L);

  private FifoOffHeapLongCache(final Builder builder) {
    this.sizeInBytes = ceilingNextPowerOfTwo(builder.size) * BUCKET_SIZE;
    this.numBuckets = sizeInBytes / BUCKET_SIZE;
    this.probeMax = Math.min(builder.probeMax, numBuckets);
    this.neighborhoodSize = builder.neighborhoodSize;
    this.drainThreshold = builder.drainThreshold;
    this.evictionThreshold = builder.evictionThreshold;
    this.evictionListener = builder.evictionListener;
    this.evictionLock = new ReentrantLock();
    this.taskBuffer = new ConcurrentLinkedQueue<>();

    this.unsafe = getUnsafe();
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
   * Proxies {@link #destroy()}. Implements AutoCloseable.
   */
  @Override
  public void close() {
    destroy();
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
   * Generate a CacheMetrics object representing the current state
   * of the cache.
   * @return {@link CacheMetrics}
   */
  public CacheMetrics getCacheMetrics() {
    return new CacheMetrics(
      sizeInBytes,
      numBuckets,
      numEntries.get(),
      getLoadFactorPretty(),
      hits.get(),
      misses.get(),
      evictions.get()
    );
  }

  /**
   * Return the oldest value in the cache.
   */
  public Long getOldestEntry() {
    return (numEntries.get() == 0) ? null : unsafe.getLong(fifoHead.get() + VALUE_OFFSET);
  }

  /**
   * Associate the specified value with the specified key unless
   * that key already exists in the hash table.
   */
  private void _put(byte[] key, long value) {
    final long hashCode = hash(key);

    long idx = findAvailableBucket(hashCode);
    if (idx < 0) {
      return; // we found our hashCode after all
    }

    long address = addressFromIndex(idx);
    unsafe.putLong(address + VALUE_OFFSET, value);
    queueWrite(new WriteTask(address));
    if (!unsafe.compareAndSwapLong(null, address, LOCK, hashCode)) {
      throw new ConcurrentModificationException("Concurrent modification of key: " + hashCode);
    }
    maybeEvict();
  }

  /**
   * Since we use hopscotch hashing, key is guaranteed to be
   * within {@link #neighborhoodSize} steps of index.
   */
  private Long _get(byte[] key) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    // start at `index` then step forward up to (NEIGHBORHOOD - 1) times
    for (int i=0; i<neighborhoodSize; i++) {
      long address = addressFromIndex((index + i) % numBuckets);
      if (unsafe.getLong(address) == LOCK) {
        // someone is already doing stuff so spin
        i--;
      } else if (unsafe.compareAndSwapLong(null, address, hashCode, LOCK)) {
        // we've found `hashCode` so return the corresponding value
        long val = unsafe.getLong(address + VALUE_OFFSET);
        if (!unsafe.compareAndSwapLong(null, address, LOCK, hashCode)) {
          throw new ConcurrentModificationException("Concurrent modification of key: " + hashCode);
        }
        return val;
      }
    }

    // if we got here the value does not exist in the hash table
    return null;
  }

  /**
   * Finds, locks, and returns an empty bucket for the caller's use, performing
   * any necessary swaps according to the Hopscotch algorithm.
   *
   * It is the responsibility of the caller to unlock the bucket when they're
   * done with it.
   */
  private long findAvailableBucket(long hashCode) {
    long baseIndex = getIndex(hashCode);
    long baseAddress = addressFromIndex(baseIndex);
    for(;;) {
      long baseHash = unsafe.getLong(baseAddress);
      if (baseHash == EMPTY) {
        if(unsafe.compareAndSwapLong(null, baseAddress, EMPTY, LOCK)) {
          // no collision so return baseIndex
          return baseIndex % numBuckets;
        }
      } else if (baseHash == hashCode) {
        // our hash code already exists so return
        return -1L;
      } else if (baseHash == LOCK) {
        // this bucket range is in use so wait till it's not
        continue;
      } else if (unsafe.compareAndSwapLong(null, baseAddress, baseHash, LOCK)) {
        // everything below is wrapped in a try/finally so we're
        // guaranteed to unlock the range when we're through with it
        try {
          long emptyIndex = -1L;
          long emptyAddress = -1L;
          for (int i = 1; i < probeMax; i++) {
            long candidateAddress = addressFromIndex((baseIndex + i) % numBuckets);
            if (emptyAddress > 0) {
              // we found an empty bucket within the neighborhood so keep
              // searching till we reach the end of the neighborhood to
              // ensure another thread didn't add the key at a later index
              if (i >= neighborhoodSize) {
                // hashCode doesn't already exist in the neighborhood
                // so return the bucket index
                return emptyIndex % numBuckets;
              }

              long candidateHash = unsafe.getLong(null, candidateAddress);
              if (candidateHash == LOCK) {
                // spin
                i--;
              } else if (candidateHash == hashCode) {
                // another thread added hashCode so unlock and return
                if (!unsafe.compareAndSwapLong(null, emptyAddress, LOCK, EMPTY)) {
                  throw new ConcurrentModificationException(
                      "Concurrent modification of while range locked");
                }
                return -1L;
              }
            } else {
              if (unsafe.compareAndSwapLong(null, candidateAddress, EMPTY, LOCK)) {
                // We found an empty bucket.
                // If we're still withing the neighborhood, finish checking to
                // make sure the value doesn't exist further on. If we're
                // already out of the neighborhood then just skip to swapping.
                emptyIndex = (baseIndex + i) % numBuckets;
                emptyAddress = candidateAddress;
                if (i < neighborhoodSize) {
                  continue;
                } else {
                  break;
                }
              }

              long candidateHash = unsafe.getLong(null, candidateAddress);
              if (candidateHash == LOCK) {
                // spin
                i--;
              } else if (candidateHash == hashCode) {
                // hash code was added so return (no need to unlock here)
                return -1L;
              }
            }
          }

          if (emptyIndex < 0) {
            throw new RuntimeException(String.format(
                "Unable to find a empty bucket after examining %d buckets", probeMax));
          }

          // now we swap back until the empty bucket is in the neighborhood
          while (emptyIndex - baseIndex > neighborhoodSize) {
            boolean foundSwap = false;
            long minBaseIndex = emptyIndex - (neighborhoodSize - 1);
            for (int i = neighborhoodSize - 1; i > 0; i--) {
              long candidateIndex = emptyIndex - i;
              if (candidateIndex < baseIndex) {
                // we can't use buckets less than our initial index
                continue;
              }

              // we've got a candidate so lock (if still necessary) and do swap
              long candidateAddress = addressFromIndex(candidateIndex % numBuckets);
              long candidateHash = unsafe.getLong(candidateAddress);
              if (candidateHash == LOCK) {
                // someone is already doing stuff so spin
                i++;
              } else if (candidateHash == hashCode) {
                // another thread already added our hash code so unlock and return
                if (!unsafe.compareAndSwapLong(null, emptyAddress, LOCK, EMPTY)) {
                  throw new ConcurrentModificationException(
                      "Concurrent modification of key while swapping: " + hashCode);
                }
                return -1L;
              } else if (getIndex(candidateHash) < minBaseIndex) {
                // can't swap to a index before minBaseIndex
                continue;
              } else if (unsafe.compareAndSwapLong(null, candidateAddress, candidateHash, LOCK)) {
                // got a lock on the candidate so do the swap
                swapEntries(emptyAddress, candidateAddress);

                long placeholder = emptyAddress;
                emptyAddress = candidateAddress;
                candidateAddress = placeholder;
                emptyIndex = candidateIndex;
                foundSwap = true;
                if (!unsafe.compareAndSwapLong(null, candidateAddress, LOCK, candidateHash)) {
                  throw new ConcurrentModificationException(
                      "Concurrent modification of key during swap !!!");
                }
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
        } finally {
          if (!unsafe.compareAndSwapLong(null, baseAddress, LOCK, baseHash)) {
            throw new ConcurrentModificationException(
                "Concurrent modification of locked range !!!");
          }
        }
      }
    }
  }

  /**
   * Helper method for swapping two entries.
   *
   * NOTE: this method does not touch the hashes associated with the
   * two entries because it is assumed they will be externally locked
   * for the duration of this operation. It's up to the caller to
   * lock/release each entry.
   */
  private void swapEntries(long a, long b) {
    // swap `a`/`b` values
    long placeholder = unsafe.getLong(b + VALUE_OFFSET);
    //unsafe.putLong(b + VALUE_OFFSET, unsafe.getLong(a + VALUE_OFFSET));
    unsafe.putLong(b + VALUE_OFFSET, EMPTY);
    unsafe.putLong(a + VALUE_OFFSET, placeholder);

    // update `b`'s predecessor to point to `a`
    // and update `a` to point back to `b`'s predecessor
    long predecessor = unsafe.getLong(b + PREV_POINTER_OFFSET);
    if (predecessor != -1L) {
      for (;;) {
        long hash = unsafe.getLong(predecessor);
        if (hash == LOCK) {
          continue;
        } else if (unsafe.compareAndSwapLong(null, predecessor, hash, LOCK)) {
          //long prev = unsafe.getLong(a + PREV_POINTER_OFFSET);
          unsafe.putLong(predecessor + NEXT_POINTER_OFFSET, a);
          unsafe.putLong(a + PREV_POINTER_OFFSET, predecessor);
          if (!unsafe.compareAndSwapLong(null, predecessor, LOCK, hash)) {
            throw new ConcurrentModificationException(
                "Concurrent modification during swap bookkeeping !!!");
          }
          break;
        }
      }
    }

    // update `b`'s successor to point back to `a`
    // and update `a` to point to `b`'s successor
    long successor = unsafe.getLong(b + NEXT_POINTER_OFFSET);
    if (successor != -1L) {
      for (;;) {
        long hash = unsafe.getLong(successor);
        if (hash == LOCK) {
          continue;
        } else if (unsafe.compareAndSwapLong(null, successor, hash, LOCK)) {
          unsafe.putLong(successor + PREV_POINTER_OFFSET, a);
          unsafe.putLong(a + NEXT_POINTER_OFFSET, successor);
          unsafe.compareAndSwapLong(null, successor, LOCK, hash);
          break;
        }
      }
    }

    // empty `b` and update fifo pointers if necessary
    unsafe.putLong(b + NEXT_POINTER_OFFSET, EMPTY);
    unsafe.putLong(b + PREV_POINTER_OFFSET, EMPTY);
    if (fifoHead.compareAndSet(b, a)){
      int s = 0;
    }
    if (fifoTail.compareAndSet(b, a)) {
      int s = 1;
    }
  }

  /**
   * Adds a WriteTask to the queue for later processing.
   */
  private void queueWrite(WriteTask task) {
    taskBuffer.add(task);
  }

  /**
   * Drains the {@link #taskBuffer} and performs any necessary
   * eviction iff the {@link #evictionLock} is available.
   *
   * This method will NOT block when attempting to acquire the lock.
   */
  private void maybeEvict() {
    if (evictionLock.tryLock()) {
      try {
        drainBuffer();
        while (hasOverflowed()) {
          evict();
        }
      } finally {
        evictionLock.unlock();
      }
    }
  }

  /**
   * Drains up to {@link #drainThreshold} WriteTasks.
   */
  @GuardedBy("evictionLock")
  private void drainBuffer() {
    for (int i = 0; i < drainThreshold; i++) {
      final WriteTask task = taskBuffer.poll();
      if (task == null) {
        break;
      }
      task.run();
    }
  }

  /**
   * Whether or not we've exceeded the {@link #evictionThreshold}.
   *
   * @return true if we have and false otherwise.
   */
  @GuardedBy("evictionLock")
  private boolean hasOverflowed() {
    return getLoadFactor() > evictionThreshold;
  }

  /**
   * Evicts the oldest entry in the hash table and updates
   * the list pointers.
   */
  @GuardedBy("evictionLock")
  private void evict() {
    for (;;) {
      // get oldest entry
      long address = fifoHead.get();
      long hashCode = unsafe.getLong(address);
      if (hashCode == LOCK) {
        // bucket is locked so spin
        continue;
      } else if (unsafe.compareAndSwapLong(null, fifoHead.get(), hashCode, LOCK)) {
        for (;;) {
          // we must also lock the next entry in the list so
          // we can do bookkeeping without risk of it being swapped
          // from under us
          long next = unsafe.getLong(address + NEXT_POINTER_OFFSET);
          long nextHash = unsafe.getLong(next);
          if (nextHash == LOCK) {
            continue;
          } else if (unsafe.compareAndSwapLong(null, next, nextHash, LOCK)) {
            unsafe.putLong(next + PREV_POINTER_OFFSET, -1L);
            fifoHead.set(next);
            numEntries.decrementAndGet();
            evictions.incrementAndGet();
            if (!unsafe.compareAndSwapLong(null, next, LOCK, nextHash)) {
              throw new ConcurrentModificationException("Concurrent modification of key during eviction !!!");
            }
            break;
          }
        }

        // we've updated the books so notify the listener and delete the entry
        evictionListener.onEvict(hashCode, unsafe.getLong(address + VALUE_OFFSET));
        unsafe.putLong(address + VALUE_OFFSET, EMPTY);
        unsafe.putLong(address + NEXT_POINTER_OFFSET, EMPTY);
        unsafe.putLong(address + PREV_POINTER_OFFSET, EMPTY);
        if (!unsafe.compareAndSwapLong(null, address, LOCK, EMPTY)) {
          throw new ConcurrentModificationException("Concurrent modification of key during delete !!!");
        }
        break;
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
   *
   * Since the {@link #LOCK} and {@link #EMPTY} values are special
   * some checks are added to ensure we don't return either from
   * this function.
   */
  private long hash(byte[] val) {
    long hash = hashFunction.newHasher(val.length)
        .putBytes(val)
        .hash()
        .asLong();

    if (hash == LOCK) {
      return hash - 1;
    } else if (hash == EMPTY) {
      return hash + 1;
    } else {
      return hash;
    }
  }

  /**
   * Returns the load factor to 2 decimal places.
   */
  private double getLoadFactorPretty() {
    return ((double) Math.round(getLoadFactor() * 100)) / 100;
  }

  /**
   * Returns the current load factor.
   */
  private double getLoadFactor() {
    return (double) numEntries.get() / (double) numBuckets;
  }

  /**
   * Helper method to return the next power of 2 >= the value passed in.
   */
  private long ceilingNextPowerOfTwo(long x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << (Long.SIZE - Long.numberOfLeadingZeros(x - 1));
  }

  /**
   * Returns the Unsafe singleton.
   * @return sun.misc.Unsafe
   */
  private static Unsafe getUnsafe() {
    try {
      return Unsafe.getUnsafe();
    } catch (SecurityException firstUse) {
      try {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        return (Unsafe) theUnsafe.get(null);
      } catch (Exception e) {
        throw new RuntimeException("Unable to acquire Unsafe", e);
      }
    }
  }

  /* ---------------- Write Bookkeeping -------------- */

  /**
   * Runnable that performs the necessary pointer
   * bookkeeping after a write to the hash table.
   */
  private class WriteTask implements Runnable {

    private final long newAddress;

    public WriteTask(long newAddress) {
      this.newAddress = newAddress;
    }

    @Override
    @GuardedBy("evictionLock")
    public void run() {
      if (numEntries.getAndIncrement() == 0L) {
        /* first entry into the hash table */
        // no need to worry about locking since we
        // can't be swapping entries at this point
        unsafe.putLong(newAddress + NEXT_POINTER_OFFSET, -1L);
        unsafe.putLong(newAddress + PREV_POINTER_OFFSET, -1L);
        fifoHead.set(newAddress);
        fifoTail.set(newAddress);
      } else {
        // lock the tail entry so no swaps happen while bookkeeping
        for (;;) {
          long currentAddress = fifoTail.get();
          long currentHash = unsafe.getLong(currentAddress);
          if (currentHash == LOCK) {
            continue; // spin until bucket is available
          } else if (unsafe.compareAndSwapLong(null, fifoTail.get(), currentHash, LOCK)) {
            // update the previous entry's pointer to point to the new
            if (!unsafe.compareAndSwapLong(null, currentAddress + NEXT_POINTER_OFFSET, -1L, newAddress)) {
              throw new RuntimeException("updating a tail that is not tail !!!");
            }

            // set the previous and next pointers on the new entry
            unsafe.putLong(newAddress + NEXT_POINTER_OFFSET, -1L);
            unsafe.putLong(newAddress + PREV_POINTER_OFFSET, currentAddress);
            fifoTail.set(newAddress);
            if (!unsafe.compareAndSwapLong(null, currentAddress, LOCK, currentHash)) {
              throw new ConcurrentModificationException("Concurrent modification during write bookkeeping !!!");
            }
            break;
          }
        }
      }
    }
  }

  /* ---------------- Eviction Listener -------------- */

  @ThreadSafe
  public static interface EvictionListener {

    /**
     * Called once for each evicted entry.
     *
     * Implementations should be thread-safe as this method may be called
     * by any thread writing to the cache.
     */
    public void onEvict(long key, long value);
  }

  /**
   * Default no-op {@link com.maascamp.fohlc.FifoOffHeapLongCache.EvictionListener} implementation.
   */
  private static class NoopEvictionListener implements EvictionListener {
    @Override
    public void onEvict(long key, long value) {
      return;
    }
  }

  /* ---------------- Builder -------------- */

  /**
   * A builder creating a {@link FifoOffHeapLongCache}.
   * Example:
   * <pre>{@code
   * FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
   *     .size(1000)
   *     .build();
   * }</pre>
   */
  public static class Builder {

    long size = 0;
    int probeMax = DEFAULT_PROBE_MAX;
    int neighborhoodSize = DEFAULT_NEIGHBORHOOD_SIZE;
    int drainThreshold = DEFAULT_DRAIN_THRESHOLD;
    double evictionThreshold = DEFAULT_EVICTION_THRESHOLD;
    EvictionListener evictionListener;

    public Builder() {}

    /**
     * Sets the maximum number of probes to execute when
     * looking for an empty bucket.
     */
    public Builder setProbeMax(int max) {
      this.probeMax = max;
      return this;
    }

    /**
     * Set the neighborhood size for hopscotch hashing.
     */
    public Builder setNeighborhoodSize(int size) {
      this.neighborhoodSize = size;
      return this;
    }

    /**
     * Set the maximum number of {@link com.maascamp.fohlc.FifoOffHeapLongCache.WriteTask}
     * to process when draining the write buffer.
     */
    public Builder setDrainThreshold(int threshold) {
      this.drainThreshold = threshold;
      return this;
    }

    /**
     * Set the load factor above which eviction occurs.
     */
    public Builder setEvictionThreshold(double threshold) {
      this.evictionThreshold = threshold;
      return this;
    }

    /**
     * Sets the number of buckets in the cache.
     * NOTE: actual size will be the closest power of two that
     * is >= the specified size.
     */
    public Builder setSize(long size) {
      this.size = size;
      return this;
    }

    /**
     * Specifiy an {@link com.maascamp.fohlc.FifoOffHeapLongCache.EvictionListener}
     * implementation to call on each eviction.
     */
    public Builder setEvictionListener(EvictionListener listener) {
      if (listener == null) {
        throw new NullPointerException();
      }
      this.evictionListener = listener;
      return this;
    }

    /**
     * Build and return an initialized FifoOffHeapLongCache instance.
     */
    public FifoOffHeapLongCache build() {
      if (size <= 0) {
        throw new IllegalStateException();
      }

      if (evictionListener == null) {
        evictionListener = new NoopEvictionListener();
      }

      return new FifoOffHeapLongCache(this);
    }
  }
}
