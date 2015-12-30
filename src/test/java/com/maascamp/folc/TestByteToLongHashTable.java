package com.maascamp.folc;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("all")
public class TestByteToLongHashTable {

  private ByteToLongHashTable cache;

  @Before
  public void setUp() {
    this.cache = new ByteToLongHashTable(100L);
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

    assertTrue("Expected 10", cache.getSize() == 10);
    assertTrue("Expected 3072 (128 * 24)", cache.getSizeInBytes() == 3072);
    assertTrue("Expected 0.8 (10 / 128)", cache.getLoadFactor() == 0.08);
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
}
