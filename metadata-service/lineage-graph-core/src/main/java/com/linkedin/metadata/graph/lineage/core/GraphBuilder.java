package com.linkedin.metadata.graph.lineage.core;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.Arena;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Accumulates edges and builds a {@link BaseGraph}.
 *
 * <p>Edges are kept as temporary int ids on the heap until {@link #build()}, which:
 *
 * <ol>
 *   <li>works out every node's facts with {@link NodeFacts}, adding each schema field's parent as a
 *       node;
 *   <li>sorts the URNs and assigns final ids in byte order;
 *   <li>keeps one edge per (source, relationship type, destination, via). OpenSearch keeps one
 *       document per lifecycle owner, so the same edge can appear more than once; copies differ
 *       only in fields the walk does not read. The copy that sorts first below represents them;
 *   <li>sorts each row, in both directions, in the order the OpenSearch visualization walk takes
 *       children ({@code createSearchAfterRequest}): via ascending with edges without a via last,
 *       updatedOn descending with missing first, createdOn descending with missing first, then the
 *       neighbour and relationship type. A per-hop limit then takes the first N edges of a row.
 * </ol>
 *
 * <p>Timestamps are used only for that order and are not stored. Self-edges are dropped, as the
 * OpenSearch walk skips them.
 */
public final class GraphBuilder {

  /** A missing timestamp. */
  public static final long NO_TIME = Long.MIN_VALUE;

  private final UrnInterner urns = new UrnInterner();
  private final TypeTable relTypes = new TypeTable("relationship type", BaseGraph.MAX_REL_TYPES);
  private final NodeFacts facts;

  private int size;
  private int[] src = new int[1 << 16];
  private int[] dst = new int[1 << 16];
  private int[] via = new int[1 << 16];
  private byte[] rel = new byte[1 << 16];
  private long[] createdOn = new long[1 << 16];
  private long[] updatedOn = new long[1 << 16];

  private final Map<String, Long> timingsMs = new LinkedHashMap<>();
  private int duplicates;
  private int unreadableNodes;

  public GraphBuilder() {
    this(NodeFacts.URN_PARTS);
  }

  public GraphBuilder(NodeFacts facts) {
    this.facts = facts;
  }

  /** Interns a URN given as bytes and returns its temporary id. */
  public int urn(byte[] buf, int off, int len) {
    return urns.intern(buf, off, len);
  }

  public int urn(String urn) {
    return urns.intern(urn);
  }

  public int relType(String name) {
    return relTypes.intern(name);
  }

  /** Adds an edge between temporary URN ids with no via and no timestamps. */
  public void addEdge(int source, int relType, int destination) {
    addEdge(source, relType, destination, BaseGraph.NO_ID, NO_TIME, NO_TIME);
  }

  /**
   * Adds an edge between temporary URN ids; pass {@link BaseGraph#NO_ID} and {@link #NO_TIME} for
   * missing values.
   */
  public void addEdge(
      int source, int relType, int destination, int viaUrn, long created, long updated) {
    if (source == destination) {
      return;
    }
    if (size == src.length) {
      grow();
    }
    src[size] = source;
    dst[size] = destination;
    via[size] = viaUrn;
    rel[size] = (byte) relType;
    createdOn[size] = created;
    updatedOn[size] = updated;
    size++;
  }

  /** Convenience overload taking URN strings; null optional values are allowed. */
  public void addEdge(
      String source,
      String relType,
      String destination,
      String viaUrn,
      Long created,
      Long updated) {
    addEdge(
        urn(source),
        relType(relType),
        urn(destination),
        viaUrn == null ? BaseGraph.NO_ID : urn(viaUrn),
        created == null ? NO_TIME : created,
        updated == null ? NO_TIME : updated);
  }

  public int edgeCount() {
    return size;
  }

  public int urnCount() {
    return urns.size();
  }

  /** Edges merged into another copy by the last {@link #build()}. */
  public int duplicates() {
    return duplicates;
  }

  /** Nodes whose facts could not be read in the last {@link #build()}. */
  public int unreadableNodes() {
    return unreadableNodes;
  }

  /** Approximate heap held by the builder before {@link #build()}. */
  public long heapBytes() {
    return urns.bytes() + src.length * 29L;
  }

  /** Milliseconds spent in each build step of the last {@link #build()}. */
  public Map<String, Long> timingsMs() {
    return timingsMs;
  }

  /** Builds into an automatic arena, freed by the garbage collector. */
  public BaseGraph build() {
    return build(Arena.ofAuto());
  }

  public BaseGraph build(Arena arena) {
    timingsMs.clear();
    duplicates = 0;
    unreadableNodes = 0;

    // Node facts. Parents are interned as they are found, so the loop also covers them.
    long t = System.nanoTime();
    TypeTable entityTypes = new TypeTable("entity type", BaseGraph.MAX_ENTITY_TYPES);
    TypeTable platforms = new TypeTable("platform", BaseGraph.MAX_PLATFORMS, false);
    TypeTable environments = new TypeTable("environment", BaseGraph.MAX_ENVIRONMENTS, false);
    int[] tmpMeta = new int[Math.max(16, urns.size())];
    int[] tmpParent = new int[tmpMeta.length];
    for (int id = 0; id < urns.size(); id++) {
      NodeFacts.Facts f = facts.of(urns.get(id));
      int parentId = f.parent() == null ? BaseGraph.NO_ID : urns.intern(f.parent());
      if (id >= tmpMeta.length) {
        tmpMeta = Arrays.copyOf(tmpMeta, tmpMeta.length * 2);
        tmpParent = Arrays.copyOf(tmpParent, tmpMeta.length);
      }
      if (f.unreadable()) {
        unreadableNodes++;
      }
      tmpMeta[id] =
          BaseGraph.packMeta(
              entityTypes.intern(f.entityType()),
              f.platform() == null ? -1 : platforms.intern(f.platform()),
              f.environment() == null ? -1 : environments.intern(f.environment()),
              f.unreadable());
      tmpParent[id] = parentId;
    }
    t = lap("nodeFacts", t);

    int[] sorted = ByteStringSort.sortedIds(urns);
    int nodes = sorted.length;
    int[] rank = new int[nodes];
    for (int i = 0; i < nodes; i++) {
      rank[sorted[i]] = i;
    }
    UrnDictionary dictionary = UrnDictionary.build(arena, urns, sorted);
    t = lap("urnDictionary", t);

    // Forward rows: counting sort by source, then dedupe and order each row.
    int[] fwdOff = new int[nodes + 1];
    int[] order = bySource(rank, nodes, fwdOff);
    int[] buf = new int[16];
    int kept = 0;
    for (int u = 0; u < nodes; u++) {
      int from = fwdOff[u];
      int to = fwdOff[u + 1];
      fwdOff[u] = kept;
      int n = to - from;
      if (n == 0) {
        continue;
      }
      if (buf.length < n) {
        buf = new int[Integer.highestOneBit(n) * 2];
      }
      sort(order, from, to, buf, (a, b) -> compareIdentity(a, b, rank));
      // Copies of one edge are adjacent; keep the one that sorts first.
      int w = from;
      for (int i = from; i < to; i++) {
        int e = order[i];
        if (w > from && compareIdentity(order[w - 1], e, rank) == 0) {
          duplicates++;
          if (compareTimes(e, order[w - 1]) < 0) {
            order[w - 1] = e;
          }
        } else {
          order[w++] = e;
        }
      }
      sort(order, from, w, buf, (a, b) -> comparePriority(a, b, dst, rank));
      System.arraycopy(order, from, order, kept, w - from);
      kept += w - from;
    }
    fwdOff[nodes] = kept;

    BaseGraph g =
        BaseGraph.allocate(
            arena, dictionary, entityTypes, relTypes, platforms, environments, nodes, kept);
    for (int id = 0; id < nodes; id++) {
      g.nodeMeta.setAtIndex(JAVA_INT, rank[id], tmpMeta[id]);
      g.parent.setAtIndex(JAVA_INT, rank[id], mapId(tmpParent[id], rank));
    }
    tmpMeta = null;
    tmpParent = null;
    for (int i = 0; i <= nodes; i++) {
      g.fwdOff.setAtIndex(JAVA_INT, i, fwdOff[i]);
    }
    for (int s = 0; s < kept; s++) {
      int e = order[s];
      g.fwdNbr.setAtIndex(JAVA_INT, s, rank[dst[e]]);
      g.fwdAttr.set(JAVA_BYTE, s, rel[e]);
      g.fwdVia.setAtIndex(JAVA_INT, s, mapId(via[e], rank));
    }
    t = lap("forwardIndex", t);

    // Reverse rows: counting sort of the kept edges by destination, then order each row.
    int[] revOff = new int[nodes + 1];
    for (int s = 0; s < kept; s++) {
      revOff[rank[dst[order[s]]] + 1]++;
    }
    for (int i = 0; i < nodes; i++) {
      revOff[i + 1] += revOff[i];
    }
    int[] rev = new int[kept];
    int[] fill = Arrays.copyOf(revOff, nodes);
    for (int s = 0; s < kept; s++) {
      int e = order[s];
      rev[fill[rank[dst[e]]]++] = e;
    }
    fill = null;
    order = null;
    for (int v = 0; v < nodes; v++) {
      int from = revOff[v];
      int to = revOff[v + 1];
      if (to - from > 1) {
        if (buf.length < to - from) {
          buf = new int[Integer.highestOneBit(to - from) * 2];
        }
        sort(rev, from, to, buf, (a, b) -> comparePriority(a, b, src, rank));
      }
    }
    for (int i = 0; i <= nodes; i++) {
      g.revOff.setAtIndex(JAVA_INT, i, revOff[i]);
    }
    for (int k = 0; k < kept; k++) {
      int e = rev[k];
      g.revNbr.setAtIndex(JAVA_INT, k, rank[src[e]]);
      g.revAttr.set(JAVA_BYTE, k, rel[e]);
      g.revVia.setAtIndex(JAVA_INT, k, mapId(via[e], rank));
    }
    lap("reverseIndex", t);
    return g;
  }

  /** Edge indexes grouped by source rank; fills {@code rowStart} with each group's start. */
  private int[] bySource(int[] rank, int nodes, int[] rowStart) {
    for (int e = 0; e < size; e++) {
      rowStart[rank[src[e]] + 1]++;
    }
    for (int i = 0; i < nodes; i++) {
      rowStart[i + 1] += rowStart[i];
    }
    int[] order = new int[size];
    int[] fill = Arrays.copyOf(rowStart, nodes);
    for (int e = 0; e < size; e++) {
      order[fill[rank[src[e]]]++] = e;
    }
    return order;
  }

  /** Orders edges of one row by (destination, relationship type, via). */
  private int compareIdentity(int a, int b, int[] rank) {
    int c = Integer.compare(rank[dst[a]], rank[dst[b]]);
    if (c == 0) {
      c = Integer.compare(rel[a], rel[b]);
    }
    if (c == 0) {
      c = Integer.compare(viaKey(a, rank), viaKey(b, rank));
    }
    return c;
  }

  /**
   * The visualization's order: via ascending (none last), timestamps as {@link #compareTimes}, then
   * the neighbour ({@code nbr} is {@code dst} for forward rows, {@code src} for reverse rows) and
   * relationship type.
   */
  private int comparePriority(int a, int b, int[] nbr, int[] rank) {
    int c = Integer.compare(viaKey(a, rank), viaKey(b, rank));
    if (c == 0) {
      c = compareTimes(a, b);
    }
    if (c == 0) {
      c = Integer.compare(rank[nbr[a]], rank[nbr[b]]);
    }
    if (c == 0) {
      c = Integer.compare(rel[a], rel[b]);
    }
    return c;
  }

  /** updatedOn descending, then createdOn descending, a missing value first. */
  private int compareTimes(int a, int b) {
    int c = compareDescMissingFirst(updatedOn[a], updatedOn[b]);
    return c != 0 ? c : compareDescMissingFirst(createdOn[a], createdOn[b]);
  }

  private static int compareDescMissingFirst(long x, long y) {
    if (x == y) {
      return 0;
    }
    if (x == NO_TIME) {
      return -1;
    }
    if (y == NO_TIME) {
      return 1;
    }
    return Long.compare(y, x);
  }

  private int viaKey(int e, int[] rank) {
    return via[e] == BaseGraph.NO_ID ? Integer.MAX_VALUE : rank[via[e]];
  }

  private static int mapId(int tmpId, int[] rank) {
    return tmpId == BaseGraph.NO_ID ? BaseGraph.NO_ID : rank[tmpId];
  }

  @FunctionalInterface
  private interface IntComparator {
    int compare(int a, int b);
  }

  /**
   * Stable merge sort of {@code a[from..to)} with a primitive comparator; {@code tmp} is scratch.
   */
  private static void sort(int[] a, int from, int to, int[] tmp, IntComparator cmp) {
    int n = to - from;
    if (n < 2) {
      return;
    }
    if (n <= 16) {
      for (int i = from + 1; i < to; i++) {
        int v = a[i];
        int j = i - 1;
        while (j >= from && cmp.compare(a[j], v) > 0) {
          a[j + 1] = a[j];
          j--;
        }
        a[j + 1] = v;
      }
      return;
    }
    int mid = (from + to) >>> 1;
    sort(a, from, mid, tmp, cmp);
    sort(a, mid, to, tmp, cmp);
    if (cmp.compare(a[mid - 1], a[mid]) <= 0) {
      return;
    }
    System.arraycopy(a, from, tmp, 0, n);
    int i = 0;
    int j = mid - from;
    for (int k = from; k < to; k++) {
      if (j >= n || (i < mid - from && cmp.compare(tmp[i], tmp[j]) <= 0)) {
        a[k] = tmp[i++];
      } else {
        a[k] = tmp[j++];
      }
    }
  }

  private void grow() {
    int n = src.length * 2;
    src = Arrays.copyOf(src, n);
    dst = Arrays.copyOf(dst, n);
    via = Arrays.copyOf(via, n);
    rel = Arrays.copyOf(rel, n);
    createdOn = Arrays.copyOf(createdOn, n);
    updatedOn = Arrays.copyOf(updatedOn, n);
  }

  private long lap(String step, long start) {
    long now = System.nanoTime();
    timingsMs.put(step, (now - start) / 1_000_000);
    return now;
  }
}
