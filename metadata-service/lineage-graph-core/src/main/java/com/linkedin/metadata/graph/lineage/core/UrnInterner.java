package com.linkedin.metadata.graph.lineage.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Build-time URN interner: maps URN strings to dense temporary ids in first-seen order.
 *
 * <p>Strings are copied as UTF-8 bytes into 16 MiB pages and indexed by an open-addressing table of
 * {@code (hash << 32) | (id + 1)}. A {@code HashMap<String, Integer>} costs about 150 bytes per URN
 * on the heap; this costs the URN bytes plus about 20 bytes, which matters at 10M+ URNs.
 */
public final class UrnInterner {

  private static final int PAGE_BITS = 24;
  private static final int PAGE_SIZE = 1 << PAGE_BITS;

  private byte[][] pages = new byte[4][];
  private int pageCount;
  private int pagePos = PAGE_SIZE; // forces the first page allocation

  private long[] offsets = new long[1 << 16]; // id -> (page << PAGE_BITS) | pos
  private int[] lengths = new int[1 << 16];
  private int size;

  private long[] table = new long[1 << 17];
  private int mask = table.length - 1;

  public int size() {
    return size;
  }

  public int intern(String urn) {
    return intern(urn.getBytes(StandardCharsets.UTF_8));
  }

  public int intern(byte[] bytes) {
    return intern(bytes, 0, bytes.length);
  }

  /** Returns the id of the byte string, adding it if absent. */
  public int intern(byte[] buf, int off, int len) {
    int h = hash(buf, off, len);
    int slot = mix(h) & mask;
    while (true) {
      long e = table[slot];
      if (e == 0) {
        int id = add(buf, off, len);
        table[slot] = ((long) h << 32) | (id + 1L);
        if (size * 10L > table.length * 7L) {
          rehash();
        }
        return id;
      }
      if ((int) (e >>> 32) == h) {
        int id = (int) e - 1;
        if (equalsAt(id, buf, off, len)) {
          return id;
        }
      }
      slot = (slot + 1) & mask;
    }
  }

  /** Returns the id of the string, or -1 if it was never interned. */
  public int find(String urn) {
    byte[] b = urn.getBytes(StandardCharsets.UTF_8);
    int h = hash(b, 0, b.length);
    int slot = mix(h) & mask;
    while (true) {
      long e = table[slot];
      if (e == 0) {
        return -1;
      }
      if ((int) (e >>> 32) == h && equalsAt((int) e - 1, b, 0, b.length)) {
        return (int) e - 1;
      }
      slot = (slot + 1) & mask;
    }
  }

  public int length(int id) {
    return lengths[id];
  }

  /** Unsigned byte {@code i} of string {@code id}, or -1 past its end (for radix sorting). */
  public int byteAt(int id, int i) {
    if (i >= lengths[id]) {
      return -1;
    }
    long o = offsets[id];
    return pages[(int) (o >>> PAGE_BITS)][(int) (o & (PAGE_SIZE - 1)) + i] & 0xFF;
  }

  public byte[] page(int id) {
    return pages[(int) (offsets[id] >>> PAGE_BITS)];
  }

  public int pageOffset(int id) {
    return (int) (offsets[id] & (PAGE_SIZE - 1));
  }

  public String get(int id) {
    return new String(page(id), pageOffset(id), lengths[id], StandardCharsets.UTF_8);
  }

  /** Approximate bytes held, for build-time memory reporting. */
  public long bytes() {
    return (long) pageCount * PAGE_SIZE
        + offsets.length * 8L
        + lengths.length * 4L
        + table.length * 8L;
  }

  private int add(byte[] buf, int off, int len) {
    if (len > PAGE_SIZE) {
      throw new IllegalArgumentException("URN longer than " + PAGE_SIZE + " bytes");
    }
    if (pagePos + len > PAGE_SIZE) {
      if (pageCount == pages.length) {
        pages = Arrays.copyOf(pages, pages.length * 2);
      }
      pages[pageCount++] = new byte[PAGE_SIZE];
      pagePos = 0;
    }
    int page = pageCount - 1;
    System.arraycopy(buf, off, pages[page], pagePos, len);
    if (size == offsets.length) {
      offsets = Arrays.copyOf(offsets, size * 2);
      lengths = Arrays.copyOf(lengths, size * 2);
    }
    offsets[size] = ((long) page << PAGE_BITS) | pagePos;
    lengths[size] = len;
    pagePos += len;
    return size++;
  }

  private boolean equalsAt(int id, byte[] buf, int off, int len) {
    if (lengths[id] != len) {
      return false;
    }
    long o = offsets[id];
    byte[] p = pages[(int) (o >>> PAGE_BITS)];
    int po = (int) (o & (PAGE_SIZE - 1));
    return Arrays.equals(p, po, po + len, buf, off, off + len);
  }

  private void rehash() {
    long[] old = table;
    table = new long[old.length * 2];
    mask = table.length - 1;
    for (long e : old) {
      if (e != 0) {
        int slot = mix((int) (e >>> 32)) & mask;
        while (table[slot] != 0) {
          slot = (slot + 1) & mask;
        }
        table[slot] = e;
      }
    }
  }

  static int hash(byte[] buf, int off, int len) {
    // FNV-1a; URNs share long prefixes so every byte has to contribute.
    int h = 0x811C9DC5;
    for (int i = off; i < off + len; i++) {
      h ^= buf[i];
      h *= 0x01000193;
    }
    return h;
  }

  private static int mix(int h) {
    h ^= h >>> 16;
    h *= 0x85EBCA6B;
    h ^= h >>> 13;
    return h;
  }
}
