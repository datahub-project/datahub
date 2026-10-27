package com.linkedin.metadata.graph.lineage.core;

import static com.linkedin.metadata.graph.lineage.core.WalkRecords.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Result of an impact walk, in compact form: one {@link WalkRecords} record per node that has a
 * relationship or a stored path, with paths kept as parent pointers rather than lists.
 *
 * <p>Paths follow {@code GraphQueryUtils.addEdgeToPaths}: a node's stored path is its parent's
 * stored path plus {@code [via?, node]}, or {@code [parent, via?, node]} when the parent had no
 * stored path at that moment (the root, or a via node). A via relationship's path is the path of
 * the child that introduced it, cut after the via. {@link #paths(int)} rebuilds them.
 *
 * <p>The arrays belong to the walker's pooled scratch space until {@link #close()}, so a walk
 * allocates nothing per node.
 */
public final class WalkResult implements AutoCloseable {

  public enum Status {
    COMPLETE,
    /** Stopped at {@code maxRelations}. */
    MAX_RELATIONS,
    /** Stopped at the time budget. */
    TIMEOUT,
    /** The start URN is not in the graph. */
    UNKNOWN_START
  }

  private final GraphSnapshot graph;
  private final Status status;
  private final boolean partial;
  private final int hops;
  private final long edgesScanned;
  private final int[] rec;
  // Records that are relationships, in the order they were first added.
  private final int[] results;
  private final int resultCount;
  private Runnable release;

  WalkResult(
      GraphSnapshot graph,
      Status status,
      boolean partial,
      int hops,
      long edgesScanned,
      int[] rec,
      int[] results,
      int resultCount,
      Runnable release) {
    this.graph = graph;
    this.status = status;
    this.partial = partial;
    this.hops = hops;
    this.edgesScanned = edgesScanned;
    this.rec = rec;
    this.results = results;
    this.resultCount = resultCount;
    this.release = release;
  }

  static WalkResult unknownStart(GraphSnapshot graph) {
    return new WalkResult(
        graph, Status.UNKNOWN_START, false, 0, 0, new int[0], new int[0], 0, null);
  }

  public Status status() {
    return status;
  }

  public boolean partial() {
    return partial;
  }

  public int hops() {
    return hops;
  }

  public long edgesScanned() {
    return edgesScanned;
  }

  public GraphSnapshot graph() {
    return graph;
  }

  /** Number of relationships. */
  public int size() {
    return resultCount;
  }

  /**
   * Returns the walker's pooled scratch space, which this result reads from. The result must not be
   * used afterwards. A result that is never closed is simply garbage collected, and the walker
   * allocates fresh scratch space for a later walk.
   */
  @Override
  public void close() {
    Runnable r = release;
    release = null;
    if (r != null) {
      r.run();
    }
  }

  /** Record index of the {@code i}-th relationship. */
  int record(int i) {
    return results[i];
  }

  /** Raw record field, see {@link WalkRecords}. */
  int field(int record, int field) {
    return rec[record * STRIDE + field];
  }

  private int base(int i) {
    return results[i] * STRIDE;
  }

  /** Node id of the {@code i}-th relationship. */
  public int node(int i) {
    return rec[base(i) + NODE];
  }

  public String urn(int i) {
    return graph.urn(node(i));
  }

  public String relationshipType(int i) {
    return graph.relTypes().name(WalkRecords.relType(rec[base(i) + PACKED]));
  }

  public int degree(int i) {
    return rec[base(i) + DEGREE];
  }

  /** All degrees the node was found at (one or two values). */
  public int[] degrees(int i) {
    int b = base(i);
    int d2 = rec[b + DEGREE2];
    return d2 < 0 ? new int[] {rec[b + DEGREE]} : new int[] {rec[b + DEGREE], d2};
  }

  public boolean explored(int i) {
    return (rec[base(i) + FLAGS] & F_EXPLORED) != 0;
  }

  /** Paths to the {@code i}-th relationship's node, as lists of node ids. */
  public List<int[]> paths(int i) {
    int b = base(i);
    int packed = rec[b + PACKED];
    List<int[]> out = new ArrayList<>(2);
    addPath(out, kind1(packed), rec[b + ARG1], rec[b + NODE]);
    addPath(out, kind2(packed), rec[b + ARG2], rec[b + NODE]);
    return out;
  }

  private void addPath(List<int[]> out, int kind, int arg, int self) {
    int[] p;
    if (kind == KIND_STORED) {
      p = storedPath(arg);
    } else if (kind == KIND_VIA_OF) {
      int[] childPath = storedPath(arg);
      int cut = 0;
      while (cut < childPath.length && childPath[cut] != self) {
        cut++;
      }
      p = Arrays.copyOf(childPath, Math.min(childPath.length, cut + 1));
    } else {
      return;
    }
    for (int[] q : out) {
      if (Arrays.equals(q, p)) {
        return;
      }
    }
    out.add(p);
  }

  /** The stored path of record {@code r}, root first. */
  int[] storedPath(int r) {
    int len = 0;
    for (int cur = r; ; ) {
      int b = cur * STRIDE;
      len += rec[b + S_VIA] >= 0 ? 2 : 1;
      if ((rec[b + FLAGS] & F_PARENT_HAD_STORED) == 0) {
        len++;
        break;
      }
      cur = rec[b + S_PARENT_REC];
    }
    int[] p = new int[len];
    int pos = len;
    for (int cur = r; ; ) {
      int b = cur * STRIDE;
      p[--pos] = rec[b + NODE];
      if (rec[b + S_VIA] >= 0) {
        p[--pos] = rec[b + S_VIA];
      }
      if ((rec[b + FLAGS] & F_PARENT_HAD_STORED) == 0) {
        p[--pos] = rec[b + S_PARENT_NODE];
        break;
      }
      cur = rec[b + S_PARENT_REC];
    }
    return p;
  }
}
