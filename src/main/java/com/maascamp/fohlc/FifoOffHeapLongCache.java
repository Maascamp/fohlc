package com.maascamp.fohlc;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * A lightweight off-heap cache with FIFO eviction semantics.
 *
 * As its name suggests, FifoOffHeapLongCache only stores long values.
 * THIS IS NOT A GENERAL PURPOSE Object CACHE. FifoOffHeapLongCache
 * uses Hopscotch hashing (a form of linear probing) for collision
 * resolution. Eviction begins when the load factor exceeds
 * {@link #EVICTION_THRESHOLD}.
 *
 * The bookkeeping strategy is modeled on the design used for
 * <a=href="https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design">
 * ConcurrentLinkedHashMap</a>. It amortizes bookkeeping and eviction across
 * threads. We aim to keep the load factor between 0.8 and 0.85.
 *
 * This cache explicitly does NOT offer delete/remove key functionality.
 */
public class FifoOffHeapLongCache {

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
   * Load factor threshold beyond which eviction begins.
   */
  private static final double EVICTION_THRESHOLD = 0.80;

  private static final long EMPTY = 0L;

  /**
   * Neighborhood for linear probing.
   * See <a href="https://en.wikipedia.org/wiki/Hopscotch_hashing">
   * Hopscotch Hashing</a>
   */
  private static final int NEIGHBORHOOD = 256;

  /**
   * Maximum number of probes for an empty bucket before we
   * declare failure. The chances of not finding an open slot
   * in {@link #NEIGHBORHOOD} probes is 1/{@link #NEIGHBORHOOD}! (factorial)
   * for a single neighborhood. However chances are actually much higher
   * since the above doesn't account for overlapping neighborhoods and so
   * doesn't necessarily prevent clustering.
   */
  private static final int PROBE_MAX = 8192;

  private final Lock evictionLock;
  private final Queue<WriteTask> taskBuffer;
  private static final int WRITE_BUFFER_DRAIN_THRESHOLD = 512;

  private final long probeMax;
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

  public FifoOffHeapLongCache(long numEntries) {
    this.evictionLock = new ReentrantLock();
    this.taskBuffer = new ConcurrentLinkedQueue<>();
    this.sizeInBytes = ceilingNextPowerOfTwo(numEntries) * BUCKET_SIZE;
    this.numBuckets = sizeInBytes / BUCKET_SIZE;
    this.probeMax = Math.min(PROBE_MAX, numBuckets);

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
   * that key alredy exists in the hashtable.
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
    if (!unsafe.compareAndSwapLong(null, address, -1L, hashCode)) {
      throw new RuntimeException("Concurrent modification of key: " + hashCode);
    }
    maybeEvict();
  }

  /**
   * Since we use hopscotch hashing, key is guaranteed to be
   * within {@link #NEIGHBORHOOD} steps of index.
   */
  private Long _get(byte[] key) {
    final long hashCode = hash(key);
    long index = getIndex(hashCode);

    // start at `index` then step forward up to (NEIGHBORHOOD - 1) times
    for (int i=0; i<NEIGHBORHOOD; i++) {
      long address = addressFromIndex((index + i) % numBuckets);
      if (unsafe.getLong(address) == -1L) {
        // someone is already doing stuff so spin
        i--;
      } else if (unsafe.compareAndSwapLong(null, address, hashCode, -1L)) {
        // we've found `hashCode` so return the corresponding value
        long val = unsafe.getLong(address + VALUE_OFFSET);
        if (!unsafe.compareAndSwapLong(null, address, -1L, hashCode)) {
          throw new RuntimeException("Concurrent modification of key: " + hashCode);
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
    long index = getIndex(hashCode);

    // see if we find a free bucket in the neighborhood
    for (int i=0; i < NEIGHBORHOOD; i++) {
      long candidateAddress = addressFromIndex((index + i) % numBuckets);
      long current = unsafe.getLong(candidateAddress);
      if (current == -1L) {
        // someone is already doing stuff so spin
        i--;
      } else if (current == hashCode) {
        // another thread already added our key/has so return
        return -1L;
      } else if (unsafe.compareAndSwapLong(null, candidateAddress, EMPTY, -1L)) {
        // we've found it so return the index
        return (index + i) % numBuckets;
      }
    }

    // if we got here we were unable to find an empty bucket in
    // the neighborhood so we start probing up to PROBE_MAX
    long emptyIndex = -1L;
    long emptyAddress = -1L;
    long probeStart = index + NEIGHBORHOOD;
    for (int i=0; i < (probeMax - NEIGHBORHOOD); i++) {
      emptyAddress = addressFromIndex((probeStart + i) % numBuckets);
      long current = unsafe.getLong(emptyAddress);
      if (current == -1L) {
        // someone is already doing stuff so spin
        i--;
      } else if (current == hashCode) {
        // another thread already added our key/hash so return
        return -1L;
      } else if (unsafe.compareAndSwapLong(null, emptyAddress, EMPTY, -1L)) {
        emptyIndex = probeStart + i;
        break;
      }
    }

    if (emptyIndex < 0) {
      throw new RuntimeException(String.format(
          "Unable to find a empty bucket after examining %d buckets", probeMax));
    }

    // now we swap back until the empty bucket is in the neighborhood
    while (emptyIndex - index > NEIGHBORHOOD) {
      boolean foundSwap = false;
      long minBaseIndex = emptyIndex - (NEIGHBORHOOD - 1);
      for(int i = NEIGHBORHOOD - 1; i > 0; i--) {
        long candidateIndex = emptyIndex - i;
        if (candidateIndex < index) {
          // we can't use buckets less than our initial index
          continue;
        }

        // we've got a candidate so lock (if still necessary) and do swap
        long candidateAddress = addressFromIndex(candidateIndex % numBuckets);
        long candidateHash = unsafe.getLong(candidateAddress);
        if (candidateHash == -1L) {
          // someone is already doing stuff so spin
          i++;
        } else if (candidateHash == hashCode) {
          // another thread already added our key/hash so reset and return
          if (!unsafe.compareAndSwapLong(null, emptyAddress, -1L, EMPTY)) {
            throw new RuntimeException("Concurrent modification of key while swapping: " + hashCode);
          }
          return -1L;
        } else if (getIndex(candidateHash) < minBaseIndex) {
          // can't swap to a index before minBaseIndex
          continue;
        } else if (unsafe.compareAndSwapLong(null, candidateAddress, candidateHash, -1L)) {
          // do the swap
          swapEntries(emptyAddress, candidateAddress);

          long placeholder = emptyAddress;
          emptyAddress = candidateAddress;
          candidateAddress = placeholder;
          emptyIndex = candidateIndex;
          foundSwap = true;
          if (!unsafe.compareAndSwapLong(null, candidateAddress, -1L, candidateHash)) {
            throw new RuntimeException("Concurrent modification of key while swapping: " + hashCode);
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
    unsafe.putLong(b + VALUE_OFFSET, EMPTY);
    unsafe.putLong(a + VALUE_OFFSET, placeholder);

    // update `b`'s predecessor to point to `a`
    // and update `a` to point back to `b`'s predecessor
    long predecessor = unsafe.getLong(b + PREV_POINTER_OFFSET);
    if (predecessor > 0L) {
      for (;;) {
        long hash = unsafe.getLong(predecessor);
        if (hash == -1L) {
          continue;
        } else if (unsafe.compareAndSwapLong(null, predecessor, hash, -1L)) {
          unsafe.putLong(predecessor + NEXT_POINTER_OFFSET, a);
          unsafe.compareAndSwapLong(null, predecessor, -1L, hash);
          unsafe.putLong(a + PREV_POINTER_OFFSET, predecessor);
          break;
        }
      }
    }

    // update `b`'s successor to point back to `a`
    // and update `a` to point to `b`'s successor
    long successor = unsafe.getLong(b + NEXT_POINTER_OFFSET);
    if (successor > 0L) {
      for (;;) {
        long hash = unsafe.getLong(successor);
        if (hash == -1L) {
          continue;
        } else if (unsafe.compareAndSwapLong(null, successor, hash, -1L)) {
          unsafe.putLong(successor + PREV_POINTER_OFFSET, a);
          unsafe.putLong(a + NEXT_POINTER_OFFSET, successor);
          unsafe.compareAndSwapLong(null, successor, -1L, hash);
          break;
        }
      }
    }

    // empty `b` and update fifo pointers if necessary
    unsafe.putLong(b + NEXT_POINTER_OFFSET, EMPTY);
    unsafe.putLong(b + PREV_POINTER_OFFSET, EMPTY);
    fifoHead.compareAndSet(b, a);
    fifoTail.compareAndSet(b, a);
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
   * Drains up to {@link #WRITE_BUFFER_DRAIN_THRESHOLD} WriteTasks.
   */
  @GuardedBy("evictionLock")
  private void drainBuffer() {
    for (int i = 0; i < WRITE_BUFFER_DRAIN_THRESHOLD; i++) {
      final WriteTask task = taskBuffer.poll();
      if (task == null) {
        break;
      }
      task.run();
    }
  }

  /**
   * Whether or not we've exceeded the {@link #EVICTION_THRESHOLD}.
   *
   * @return true if we have and false otherwise.
   */
  @GuardedBy("evictionLock")
  private boolean hasOverflowed() {
    return getLoadFactor() > EVICTION_THRESHOLD;
  }

  /**
   * Evicts the oldest entry in the hashtable and updates
   * the list pointers.
   */
  @GuardedBy("evictionLock")
  private void evict() {
    for (;;) {
      // get oldest entry
      long address = fifoHead.get();
      long hashCode = unsafe.getLong(address);
      if (hashCode == -1L) {
        // bucket is locked so spin
        continue;
      } else if (unsafe.compareAndSwapLong(null, fifoHead.get(), hashCode, -1L)) {
        for (;;) {
          // we must also lock the next entry in the list so
          // we can do bookkeeping without risk of it being swapped
          // from under us
          long next = unsafe.getLong(address + NEXT_POINTER_OFFSET);
          long nextHash = unsafe.getLong(next);
          if (nextHash == -1L) {
            continue;
          } else if (unsafe.compareAndSwapLong(null, next, nextHash, -1L)) {
            unsafe.putLong(next + PREV_POINTER_OFFSET, -1L);
            fifoHead.set(next);
            numEntries.decrementAndGet();
            evictions.incrementAndGet();
            if (!unsafe.compareAndSwapLong(null, next, -1L, nextHash)) {
              throw new RuntimeException("Concurrent modification of key during eviction !");
            }
            break;
          }
        }

        // we've updated the books so delete the entry
        unsafe.putLong(address + VALUE_OFFSET, EMPTY);
        unsafe.putLong(address + NEXT_POINTER_OFFSET, EMPTY);
        unsafe.putLong(address + PREV_POINTER_OFFSET, EMPTY);
        if (!unsafe.compareAndSwapLong(null, address, -1L, EMPTY)) {
          throw new RuntimeException("Concurrent modification of key during delete !");
        }
        break;
      }
    }
  }

  /**
   * Maps a hash code into the bucket range.
   */
  private long getIndex(long hashCode) {
    return Math.abs(hashCode) % numBuckets;
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
          if (currentHash == -1L) {
            continue; // spin until bucket is available
          } else if (unsafe.compareAndSwapLong(null, fifoTail.get(), currentHash, -1L)) {
            // update the previous entry's pointer to point to the new
            if (!unsafe.compareAndSwapLong(null, currentAddress + NEXT_POINTER_OFFSET, -1L, newAddress)) {
              throw new RuntimeException("updating a tail that is not tail !!!");
            }

            // set the previous and next pointers on the new entry
            unsafe.putLong(newAddress + NEXT_POINTER_OFFSET, -1L);
            unsafe.putLong(newAddress + PREV_POINTER_OFFSET, currentAddress);
            fifoTail.set(newAddress);
            if (!unsafe.compareAndSwapLong(null, currentAddress, -1L, currentHash)) {
              throw new RuntimeException("Concurrent modification during write bookkeeping !!!");
            }
            break;
          }
        }
      }
    }
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
}
