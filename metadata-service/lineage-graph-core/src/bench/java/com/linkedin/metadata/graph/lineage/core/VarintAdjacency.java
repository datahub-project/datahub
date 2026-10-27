package com.linkedin.metadata.graph.lineage.core;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Compression variant for the bench: the reverse (impact) adjacency stored as gap + varint bytes.
 * Each node's neighbours are sorted, so they are written as the first id then the gaps between
 * consecutive ids, each in LEB128. Offsets stay plain ints.
 */
final class VarintAdjacency {

  final MemorySegment off; // long per node + 1
  final MemorySegment data;

  private VarintAdjacency(MemorySegment off, MemorySegment data) {
    this.off = off;
    this.data = data;
  }

  static VarintAdjacency reverseOf(BaseGraph g, Arena arena) {
    int n = g.nodeCount();
    long total = 0;
    for (int u = 0; u < n; u++) {
      int prev = 0;
      for (int k = g.revStart(u); k < g.revEnd(u); k++) {
        int v = g.revNbr(k);
        total += Varint.size(k == g.revStart(u) ? v : v - prev);
        prev = v;
      }
    }
    MemorySegment off = arena.allocate((n + 1L) * 8, 8);
    MemorySegment data = arena.allocate(Math.max(1, total));
    long pos = 0;
    for (int u = 0; u < n; u++) {
      off.setAtIndex(ValueLayout.JAVA_LONG, u, pos);
      int prev = 0;
      for (int k = g.revStart(u); k < g.revEnd(u); k++) {
        int v = g.revNbr(k); // reverse rows are sorted by source id
        pos = Varint.write(data, pos, k == g.revStart(u) ? v : v - prev);
        prev = v;
      }
    }
    off.setAtIndex(ValueLayout.JAVA_LONG, n, pos);
    return new VarintAdjacency(off, data);
  }

  long dataBytes() {
    return data.byteSize();
  }

  long offsetBytes() {
    return off.byteSize();
  }

  /** Plain downstream BFS (no paths or filters) returning the number of nodes reached. */
  int bfs(int root, long[] visited, int[] queue) {
    visited[root >>> 6] |= 1L << (root & 63);
    int head = 0;
    int tail = 0;
    queue[tail++] = root;
    while (head < tail) {
      int u = queue[head++];
      long p = off.getAtIndex(ValueLayout.JAVA_LONG, u);
      long end = off.getAtIndex(ValueLayout.JAVA_LONG, u + 1);
      int v = 0;
      boolean first = true;
      while (p < end) {
        int x = 0;
        int shift = 0;
        byte b;
        do {
          b = data.get(ValueLayout.JAVA_BYTE, p++);
          x |= (b & 0x7F) << shift;
          shift += 7;
        } while (b < 0);
        v = first ? x : v + x;
        first = false;
        long bit = 1L << (v & 63);
        if ((visited[v >>> 6] & bit) == 0) {
          visited[v >>> 6] |= bit;
          queue[tail++] = v;
        }
      }
    }
    return tail - 1;
  }

  /** The same BFS over the plain int32 reverse index, for comparison. */
  static int bfsInt32(BaseGraph g, int root, long[] visited, int[] queue) {
    visited[root >>> 6] |= 1L << (root & 63);
    int head = 0;
    int tail = 0;
    queue[tail++] = root;
    while (head < tail) {
      int u = queue[head++];
      int end = g.revEnd(u);
      for (int k = g.revStart(u); k < end; k++) {
        int v = g.revNbr(k);
        long bit = 1L << (v & 63);
        if ((visited[v >>> 6] & bit) == 0) {
          visited[v >>> 6] |= bit;
          queue[tail++] = v;
        }
      }
    }
    return tail - 1;
  }
}
