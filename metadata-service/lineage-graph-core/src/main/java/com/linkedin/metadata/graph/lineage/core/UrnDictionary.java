package com.linkedin.metadata.graph.lineage.core;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Immutable, off-heap URN dictionary. Node id {@code i} is the {@code i}-th URN in byte order.
 *
 * <p>URNs are front-coded in blocks of {@value #BLOCK}: the first URN of a block is stored whole
 * and each following one as {@code (shared prefix length, suffix)}. URNs in sorted order share most
 * of their bytes, so this is several times smaller than the raw strings. There is no hash index:
 * lookups binary-search the block heads, which costs no extra memory and is only needed for the
 * start URN of a walk and for writes.
 */
public final class UrnDictionary {

  static final int BLOCK = 16;

  private final int size;
  private final MemorySegment data;
  private final MemorySegment blockOffsets; // long per block

  private UrnDictionary(int size, MemorySegment data, MemorySegment blockOffsets) {
    this.size = size;
    this.data = data;
    this.blockOffsets = blockOffsets;
  }

  /** Builds the dictionary from interned URNs listed in byte order. */
  static UrnDictionary build(Arena arena, UrnInterner interner, int[] sortedIds) {
    int n = sortedIds.length;
    int blocks = (n + BLOCK - 1) / BLOCK;
    long total = 0;
    for (int i = 0; i < n; i++) {
      int len = interner.length(sortedIds[i]);
      if (i % BLOCK == 0) {
        total += Varint.size(len) + len;
      } else {
        int shared = sharedPrefix(interner, sortedIds[i - 1], sortedIds[i]);
        total += Varint.size(shared) + Varint.size(len - shared) + (len - shared);
      }
    }
    MemorySegment data = arena.allocate(Math.max(1, total));
    MemorySegment offsets = arena.allocate(Math.max(1, blocks) * 8L, 8);
    long pos = 0;
    for (int i = 0; i < n; i++) {
      int id = sortedIds[i];
      int len = interner.length(id);
      int from;
      if (i % BLOCK == 0) {
        offsets.setAtIndex(ValueLayout.JAVA_LONG, i / BLOCK, pos);
        pos = Varint.write(data, pos, len);
        from = 0;
      } else {
        from = sharedPrefix(interner, sortedIds[i - 1], id);
        pos = Varint.write(data, pos, from);
        pos = Varint.write(data, pos, len - from);
      }
      MemorySegment.copy(
          interner.page(id),
          interner.pageOffset(id) + from,
          data,
          ValueLayout.JAVA_BYTE,
          pos,
          len - from);
      pos += len - from;
    }
    return new UrnDictionary(n, data, offsets);
  }

  /** A dictionary over existing segments, for loading a snapshot. */
  static UrnDictionary of(int size, MemorySegment data, MemorySegment blockOffsets) {
    return new UrnDictionary(size, data, blockOffsets);
  }

  MemorySegment data() {
    return data;
  }

  MemorySegment blockOffsets() {
    return blockOffsets;
  }

  public int size() {
    return size;
  }

  public long bytes() {
    return data.byteSize() + blockOffsets.byteSize();
  }

  /** Returns the URN for {@code id}. */
  public String get(int id) {
    if (id < 0 || id >= size) {
      throw new IndexOutOfBoundsException(id);
    }
    Cursor c = new Cursor();
    c.seek(id / BLOCK);
    for (int i = id - id % BLOCK; i < id; i++) {
      c.next();
    }
    return c.next();
  }

  /** Returns the id of {@code urn}, or -1 if it is not in the dictionary. */
  public int find(String urn) {
    byte[] key = urn.getBytes(StandardCharsets.UTF_8);
    int blocks = (size + BLOCK - 1) / BLOCK;
    int lo = 0;
    int hi = blocks - 1;
    int block = -1;
    byte[] head = new byte[256];
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      long[] pos = {blockOffsets.getAtIndex(ValueLayout.JAVA_LONG, mid)};
      int len = Varint.readInt(data, pos);
      if (head.length < len) {
        head = new byte[len * 2];
      }
      MemorySegment.copy(data, ValueLayout.JAVA_BYTE, pos[0], head, 0, len);
      int c = Arrays.compareUnsigned(head, 0, len, key, 0, key.length);
      if (c <= 0) {
        block = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (block < 0) {
      return -1;
    }
    Cursor c = new Cursor();
    c.seek(block);
    int end = Math.min(size, (block + 1) * BLOCK);
    for (int id = block * BLOCK; id < end; id++) {
      c.advance();
      if (c.len == key.length && Arrays.equals(c.buf, 0, c.len, key, 0, key.length)) {
        return id;
      }
    }
    return -1;
  }

  /**
   * Sequential decoder. Decoding ids in ascending order touches each block once, which is how a
   * walk result's URNs are written out.
   */
  public final class Cursor {
    private final long[] pos = new long[1];
    private byte[] buf = new byte[256];
    private int len;
    private int nextId = -1;

    /** Positions the cursor at the start of {@code block}. */
    void seek(int block) {
      pos[0] = blockOffsets.getAtIndex(ValueLayout.JAVA_LONG, block);
      nextId = block * BLOCK;
    }

    /** Returns the URN for {@code id}, seeking only when it is not the next one in order. */
    public String get(int id) {
      if (id != nextId) {
        seek(id / BLOCK);
        for (int i = id - id % BLOCK; i < id; i++) {
          advance();
        }
      }
      return next();
    }

    String next() {
      advance();
      return new String(buf, 0, len, StandardCharsets.UTF_8);
    }

    void advance() {
      int shared = nextId % BLOCK == 0 ? 0 : Varint.readInt(data, pos);
      int suffix = Varint.readInt(data, pos);
      int total = shared + suffix;
      if (buf.length < total) {
        buf = Arrays.copyOf(buf, total * 2);
      }
      MemorySegment.copy(data, ValueLayout.JAVA_BYTE, pos[0], buf, shared, suffix);
      pos[0] += suffix;
      len = total;
      nextId++;
    }
  }

  public Cursor cursor() {
    return new Cursor();
  }

  private static int sharedPrefix(UrnInterner s, int a, int b) {
    byte[] pa = s.page(a);
    byte[] pb = s.page(b);
    int oa = s.pageOffset(a);
    int ob = s.pageOffset(b);
    int n = Math.min(s.length(a), s.length(b));
    int i = 0;
    while (i < n && pa[oa + i] == pb[ob + i]) {
      i++;
    }
    return i;
  }
}
