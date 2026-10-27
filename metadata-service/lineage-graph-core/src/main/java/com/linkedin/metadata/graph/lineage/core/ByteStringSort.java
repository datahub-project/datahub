package com.linkedin.metadata.graph.lineage.core;

/**
 * Sorts interned URNs into byte order with multikey (three-way radix) quicksort.
 *
 * <p>URNs share long prefixes ({@code urn:li:dataset:(urn:li:dataPlatform:...}), so a comparison
 * sort re-reads those prefixes on every compare. Multikey quicksort reads each byte position about
 * once per partition level instead.
 */
final class ByteStringSort {

  private static final int INSERTION_THRESHOLD = 12;

  private ByteStringSort() {}

  /** Returns the interned ids ordered by their UTF-8 bytes. */
  static int[] sortedIds(UrnInterner interner) {
    int n = interner.size();
    int[] ids = new int[n];
    for (int i = 0; i < n; i++) {
      ids[i] = i;
    }
    sort(interner, ids, 0, n, 0);
    return ids;
  }

  private static void sort(UrnInterner s, int[] a, int lo, int hi, int depth) {
    while (hi - lo > INSERTION_THRESHOLD) {
      int pivot = s.byteAt(a[lo + (hi - lo) / 2], depth);
      int lt = lo;
      int gt = hi - 1;
      int i = lo;
      while (i <= gt) {
        int c = s.byteAt(a[i], depth);
        if (c < pivot) {
          swap(a, lt++, i++);
        } else if (c > pivot) {
          swap(a, i, gt--);
        } else {
          i++;
        }
      }
      // a[lo..lt) < pivot, a[lt..gt] == pivot, a(gt..hi) > pivot
      sort(s, a, lo, lt, depth);
      sort(s, a, gt + 1, hi, depth);
      if (pivot < 0) {
        return; // all equal strings that ended here
      }
      lo = lt;
      hi = gt + 1;
      depth++;
    }
    insertionSort(s, a, lo, hi, depth);
  }

  private static void insertionSort(UrnInterner s, int[] a, int lo, int hi, int depth) {
    for (int i = lo + 1; i < hi; i++) {
      for (int j = i; j > lo && compare(s, a[j], a[j - 1], depth) < 0; j--) {
        swap(a, j, j - 1);
      }
    }
  }

  static int compare(UrnInterner s, int x, int y, int from) {
    byte[] px = s.page(x);
    byte[] py = s.page(y);
    int ox = s.pageOffset(x);
    int oy = s.pageOffset(y);
    int lx = s.length(x);
    int ly = s.length(y);
    int n = Math.min(lx, ly);
    for (int i = from; i < n; i++) {
      int c = (px[ox + i] & 0xFF) - (py[oy + i] & 0xFF);
      if (c != 0) {
        return c;
      }
    }
    return lx - ly;
  }

  private static void swap(int[] a, int i, int j) {
    int t = a[i];
    a[i] = a[j];
    a[j] = t;
  }
}
