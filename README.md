#FifoOffHeapLongCache

###A lightweight concurrent off-heap cache with FIFO eviction semantics written in Java.

As its name suggests, FifoOffHeapLongCache maps byte array keys to long values and utilizes a FIFO eviction policy.
It is **not** a general purpose [Object](https://docs.oracle.com/javase/8/docs/api/java/lang/Object.html) cache. 
FifoOffHeapLongCache uses [Hopscotch hashing](https://en.wikipedia.org/wiki/Hopscotch_hashing) (a form of linear probing) for collision
resolution and uses the 128 bit version of [MurmurHash3](https://code.google.com/p/smhasher/wiki/MurmurHash3) as its hashing function
(note however that we only use the lower 64 bits of the generated hash). This class is thread-safe.

The bookkeeping strategy for managing linked list pointers and eviction is modeled on
the [ConcurrentLinkedHashMap design](https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design) which amortizes bookkeeping and 
eviction across threads. We aim to keep the load factor between 0.8 and 0.85.

Puts into FifoOffHeapLongCache are only written if the key does not currently exist in the cache. 
Note that FifoOffHeapLongCache explicitly does NOT offer delete/remove key functionality.

FifoOffHeapLongCache was built for large (>10GB) caches and stores all key, value, and pointer data 
off-heap in pre-allocated memory in order to minimize GC overhead. *NOTE: you __must__ call `destroy`
on the cache or use it in a try-with-resources block (in which case `destroy` will be called for you)
or any allocated memory will not be released.*

####Usage
```java
try (FifoOffHeapLongCache cache = new FifoOffHeapLongCache.Builder()
  .setSize(100000000L)
  .build()
) {
  String key = "key";
  long value = 1L;
  cache.put(key.getBytes(), value);
  assert 1 == cache.get(key.getBytes());
}
```
