package com.maascamp.folc;

import java.lang.reflect.Field;

public class UnsafeAccess {

  public static sun.misc.Unsafe getUnsafe() {
    try {
      return sun.misc.Unsafe.getUnsafe();
    } catch (SecurityException firstUse) {
      try {
        Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        return (sun.misc.Unsafe) theUnsafe.get(null);
      } catch (Exception e) {
        throw new RuntimeException("Unable to acquire Unsafe", e);
      }
    }
  }
}
