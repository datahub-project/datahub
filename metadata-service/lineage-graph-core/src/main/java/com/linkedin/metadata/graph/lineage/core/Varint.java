package com.linkedin.metadata.graph.lineage.core;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Unsigned LEB128 helpers over byte arrays and memory segments. */
public final class Varint {

  private Varint() {}

  public static int size(long v) {
    int n = 1;
    while ((v & ~0x7FL) != 0) {
      v >>>= 7;
      n++;
    }
    return n;
  }

  /** Writes {@code v} at {@code pos} and returns the position after it. */
  public static int write(byte[] buf, int pos, long v) {
    while ((v & ~0x7FL) != 0) {
      buf[pos++] = (byte) ((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    buf[pos++] = (byte) v;
    return pos;
  }

  /** Writes {@code v} at {@code pos} and returns the position after it. */
  public static long write(MemorySegment seg, long pos, long v) {
    while ((v & ~0x7FL) != 0) {
      seg.set(ValueLayout.JAVA_BYTE, pos++, (byte) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    seg.set(ValueLayout.JAVA_BYTE, pos++, (byte) v);
    return pos;
  }

  /**
   * Reads an int varint at {@code pos[0]} and advances it. A one-element array keeps the cursor
   * without allocating per call.
   */
  public static int readInt(MemorySegment seg, long[] pos) {
    long p = pos[0];
    int result = 0;
    int shift = 0;
    byte b;
    do {
      b = seg.get(ValueLayout.JAVA_BYTE, p++);
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while (b < 0);
    pos[0] = p;
    return result;
  }

  public static long zigzag(long v) {
    return (v << 1) ^ (v >> 63);
  }

  public static long unzigzag(long v) {
    return (v >>> 1) ^ -(v & 1);
  }
}
