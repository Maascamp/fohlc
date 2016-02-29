package com.maascamp.fohlc;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A ReadableByteChannel for use with FileChannel#transferFrom to persist the
 * Unsafe memory arena.
 */
public class UnsafeByteChannel implements ReadableByteChannel, WritableByteChannel {

  private final Unsafe unsafe;
  private long memStart;
  private long memSize;
  private long position;

  public UnsafeByteChannel(Unsafe unsafe, long memStart, long memSize) {
    this.unsafe = unsafe;
    this.memStart = memStart;
    this.memSize = memSize;
    this.position = 0;
  }

  @Override
  public int read(ByteBuffer dst) {
    if (position >= memSize) {
      return -1;
    }

    long posStart = position;
    while (dst.hasRemaining() && position < memSize) {
      dst.put(unsafe.getByte(memStart + position));
      position++;
    }
    return (int) (position - posStart);
  }

  @Override
  public int write(ByteBuffer src) {
    if (position >= memSize) {
      return 0;
    }

    int bytesToWrite = src.remaining();
    while (src.hasRemaining()) {
      unsafe.putByte(memStart + position, src.get());
      position++;
    }
    return bytesToWrite;
  }

  @Override
  public void close() {}

  @Override
  public boolean isOpen() {
    return true;
  }
}
