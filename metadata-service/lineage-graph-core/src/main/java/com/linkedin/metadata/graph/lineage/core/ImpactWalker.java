package com.linkedin.metadata.graph.lineage.core;

import static com.linkedin.metadata.graph.lineage.core.WalkRecords.*;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Level-by-level impact walk over a {@link GraphSnapshot}, reproducing the OpenSearch walk in
 * {@code GraphQueryBaseDAO.getImpactLineage} with {@code exploreMultiplePaths = false}:
 *
 * <ul>
 *   <li>Each hop expands every frontier node's outgoing and incoming edges and keeps an edge if
 *       {@link EdgeFilter} allows it, the other end is not yet visited, and (when set) it passes
 *       the lineage time window.
 *   <li>A child is marked visited when first reached. Its path is its parent's stored path plus
 *       {@code [via?, child]}. If the parent's stored path has a cycle the child is visited but
 *       gets no relationship ({@code addEdgeToPaths} returns false).
 *   <li>An edge's {@code via} entity, the first time it is seen, becomes a relationship of the same
 *       hop whose path is the child's path cut at the via. Via entities join the next frontier but
 *       are not marked visited.
 *   <li>Within a hop the last relationship put for a node wins; across hops a node found again
 *       keeps its first relationship, the smaller degree, and the union of degrees and paths.
 *   <li>At the start of a hop, frontier nodes already in the result are marked explored.
 * </ul>
 *
 * <p>Only one difference is deliberate: OpenSearch processes a hop's edges across parallel slices,
 * so which parent claims a child first varies between runs; here it is deterministic.
 *
 * <p>Scratch space is pooled and lent to the {@link WalkResult} until it is closed, so a walk
 * allocates nothing per node. The pool keeps at most as many scratch spaces as walks may run at
 * once; a scratch space sized for another snapshot (after a reload) is dropped.
 */
public final class ImpactWalker {

  private static final int OUT = EdgeFilter.Direction.OUTGOING.ordinal();
  private static final int IN = EdgeFilter.Direction.INCOMING.ordinal();
  private static final int DEADLINE_CHECK_EVERY = 4096;

  private final ArrayBlockingQueue<Scratch> pool;

  /**
   * @param maxPooled scratch spaces kept for reuse: the number of walks that may run at once
   */
  public ImpactWalker(int maxPooled) {
    this.pool = new ArrayBlockingQueue<>(Math.max(1, maxPooled));
  }

  public ImpactWalker() {
    this(2);
  }

  /** Walks {@code snapshot}, which the caller holds for as long as it uses the result. */
  public WalkResult walk(GraphSnapshot snapshot, WalkRequest request) {
    return walk(snapshot, request, EdgeFilter.compile(snapshot, request.rules()));
  }

  public WalkResult walk(GraphSnapshot snapshot, WalkRequest request, EdgeFilter filter) {
    int root = snapshot.find(request.startUrn());
    if (root < 0) {
      return WalkResult.unknownStart(snapshot);
    }
    Scratch s = pool.poll();
    if (s == null || s.capacity != snapshot.nodeCount()) {
      s = new Scratch(snapshot.nodeCount());
    }
    try {
      return s.run(snapshot, root, request, filter);
    } catch (RuntimeException | Error e) {
      s.release();
      throw e;
    }
  }

  private final class Scratch {
    final int capacity;
    final long[] visited;
    final long[] viaSet;
    final int[] recOf; // node -> record + 1
    final IntList touched = new IntList(); // visited nodes, for reset

    int recs;
    int[] rec = new int[1024 * STRIDE];

    final IntList results = new IntList();
    final IntList hopList = new IntList();
    IntList frontier = new IntList();
    IntList nextFrontier = new IntList();
    final Runnable release = this::release;

    GraphSnapshot graph;

    Scratch(int nodes) {
      capacity = nodes;
      visited = new long[(nodes + 63) >>> 6];
      viaSet = new long[(nodes + 63) >>> 6];
      recOf = new int[nodes];
    }

    WalkResult run(GraphSnapshot snapshot, int root, WalkRequest req, EdgeFilter filter) {
      graph = snapshot;
      BaseGraph base = snapshot.base;
      long deadline =
          req.timeoutMillis() > 0 ? System.nanoTime() + req.timeoutMillis() * 1_000_000 : 0;
      int cap = req.maxRelations();
      WalkResult.Status status = WalkResult.Status.COMPLETE;
      boolean partial = false;
      long scanned = 0;
      int hops = 0;

      markVisited(root);
      frontier.add(root);

      hopLoop:
      for (int h = 1; h <= req.maxHops() && frontier.size > 0; h++) {
        if (deadline != 0 && System.nanoTime() > deadline) {
          status = WalkResult.Status.TIMEOUT;
          partial = true;
          break;
        }
        hops = h;
        for (int i = 0; i < frontier.size; i++) {
          int r = recOf[frontier.data[i]] - 1;
          if (r >= 0 && (rec[r * STRIDE + FLAGS] & F_IN_RESULT) != 0) {
            rec[r * STRIDE + FLAGS] |= F_EXPLORED;
          }
        }
        int hopNew = 0;
        for (int i = 0; i < frontier.size; i++) {
          if (deadline != 0
              && (i % DEADLINE_CHECK_EVERY) == DEADLINE_CHECK_EVERY - 1
              && System.nanoTime() > deadline) {
            status = WalkResult.Status.TIMEOUT;
            partial = true;
            mergeHop(h);
            break hopLoop;
          }
          int x = frontier.data[i];
          int t = base.entityType(x);
          int end = base.fwdEnd(x);
          for (int e = base.fwdStart(x); e < end; e++) {
            scanned++;
            int y = base.fwdNbr(e);
            if (isVisited(y)) {
              continue;
            }
            int rel = base.fwdAttr(e) & BaseGraph.REL_MASK;
            if (!filter.allows(t, rel, OUT, base.entityType(y))) {
              continue;
            }
            hopNew += addChild(x, y, rel, base.fwdVia(e), h);
          }
          end = base.revEnd(x);
          for (int k = base.revStart(x); k < end; k++) {
            scanned++;
            int y = base.revNbr(k);
            if (isVisited(y)) {
              continue;
            }
            int rel = base.revAttr(k) & BaseGraph.REL_MASK;
            if (!filter.allows(t, rel, IN, base.entityType(y))) {
              continue;
            }
            hopNew += addChild(x, y, rel, base.revVia(k), h);
          }
          if (cap > 0 && results.size + hopNew >= cap) {
            break; // the hop's relationship budget is spent
          }
        }
        mergeHop(h);
        if (cap > 0 && results.size >= cap) {
          status = WalkResult.Status.MAX_RELATIONS;
          partial = true;
          break;
        }
      }
      return new WalkResult(
          snapshot,
          status,
          partial && req.partialResults(),
          hops,
          scanned,
          rec,
          results.data,
          results.size,
          release);
    }

    /** Processes one accepted edge; returns how many new relationships the hop gained. */
    private int addChild(int parent, int child, int rel, int via, int h) {
      int pr = recOf[parent] - 1;
      int parentFlags = pr >= 0 ? rec[pr * STRIDE + FLAGS] : 0;
      boolean parentHas = (parentFlags & F_HAS_STORED) != 0;
      markVisited(child);
      if (parentHas && (parentFlags & F_CYCLIC) != 0) {
        return 0; // every path to the parent has a cycle: visited, but no relationship
      }
      // A path can only repeat a node through a via entity: every other path node is visited.
      boolean cyclic =
          (via >= 0 || isVia(child))
              && (via == child
                  || (via >= 0 && pathContains(parent, pr, parentHas, via))
                  || pathContains(parent, pr, parentHas, child));
      int c = record(child);
      int cb = c * STRIDE;
      rec[cb + FLAGS] |=
          F_HAS_STORED | (parentHas ? F_PARENT_HAD_STORED : 0) | (cyclic ? F_CYCLIC : 0);
      rec[cb + S_PARENT_REC] = pr;
      rec[cb + S_PARENT_NODE] = parent;
      rec[cb + S_VIA] = via;
      int added = hopPut(c, rel, KIND_STORED, c, h);
      if (via >= 0 && !isVia(via)) {
        viaSet[via >>> 6] |= 1L << (via & 63);
        added += hopPut(record(via), rel, KIND_VIA_OF, c, h);
      }
      return added;
    }

    private int hopPut(int r, int rel, int kind, int arg, int h) {
      int b = r * STRIDE;
      int added = 0;
      if (rec[b + HOP_STAMP] != h) {
        rec[b + HOP_STAMP] = h;
        hopList.add(r);
        added = (rec[b + FLAGS] & F_IN_RESULT) != 0 ? 0 : 1;
      }
      rec[b + PACKED] = withHop(rec[b + PACKED], rel, kind);
      rec[b + HOP_ARG] = arg;
      return added;
    }

    private void mergeHop(int h) {
      IntList next = nextFrontier;
      next.size = 0;
      for (int i = 0; i < hopList.size; i++) {
        int r = hopList.data[i];
        int b = r * STRIDE;
        int packed = rec[b + PACKED];
        int hopKind = WalkRecords.hopKind(packed);
        if ((rec[b + FLAGS] & F_IN_RESULT) == 0) {
          rec[b + FLAGS] |= F_IN_RESULT;
          results.add(r);
          rec[b + PACKED] = withRelationship(packed, WalkRecords.hopRelType(packed), hopKind);
          rec[b + DEGREE] = h;
          rec[b + ARG1] = rec[b + HOP_ARG];
        } else {
          if (h != rec[b + DEGREE] && rec[b + DEGREE2] < 0) {
            rec[b + DEGREE2] = h;
          }
          if (kind2(packed) == KIND_NONE
              && (hopKind != kind1(packed) || rec[b + HOP_ARG] != rec[b + ARG1])) {
            rec[b + PACKED] = withKind2(packed, hopKind);
            rec[b + ARG2] = rec[b + HOP_ARG];
          }
        }
        next.add(rec[b + NODE]);
      }
      hopList.size = 0;
      nextFrontier = frontier;
      frontier = next;
    }

    private boolean pathContains(int parent, int pr, boolean parentHas, int x) {
      if (parent == x) {
        return true;
      }
      if (!parentHas) {
        return false;
      }
      for (int cur = pr; ; ) {
        int b = cur * STRIDE;
        if (rec[b + S_VIA] == x || rec[b + S_PARENT_NODE] == x) {
          return true;
        }
        if ((rec[b + FLAGS] & F_PARENT_HAD_STORED) == 0) {
          return false;
        }
        cur = rec[b + S_PARENT_REC];
      }
    }

    private int record(int n) {
      int r = recOf[n] - 1;
      if (r >= 0) {
        return r;
      }
      if ((recs + 1) * STRIDE > rec.length) {
        rec = Arrays.copyOf(rec, rec.length * 2);
      }
      r = recs++;
      recOf[n] = r + 1;
      int b = r * STRIDE;
      rec[b + NODE] = n;
      rec[b + FLAGS] = 0;
      rec[b + S_PARENT_REC] = -1;
      rec[b + S_PARENT_NODE] = -1;
      rec[b + S_VIA] = -1;
      rec[b + PACKED] = 0;
      rec[b + DEGREE2] = -1;
      rec[b + HOP_STAMP] = 0;
      return r;
    }

    private void markVisited(int n) {
      long bit = 1L << (n & 63);
      if ((visited[n >>> 6] & bit) == 0) {
        visited[n >>> 6] |= bit;
        touched.add(n);
      }
    }

    private boolean isVisited(int n) {
      return (visited[n >>> 6] & (1L << (n & 63))) != 0;
    }

    private boolean isVia(int n) {
      return (viaSet[n >>> 6] & (1L << (n & 63))) != 0;
    }

    void release() {
      reset();
      pool.offer(this); // dropped when the pool is full
    }

    void reset() {
      graph = null;
      for (int i = 0; i < touched.size; i++) {
        visited[touched.data[i] >>> 6] = 0;
      }
      for (int r = 0; r < recs; r++) {
        int n = rec[r * STRIDE + NODE];
        recOf[n] = 0;
        viaSet[n >>> 6] = 0;
      }
      touched.size = 0;
      recs = 0;
      results.size = 0;
      hopList.size = 0;
      frontier.size = 0;
      nextFrontier.size = 0;
    }
  }

  /** Growable int array without boxing. */
  static final class IntList {
    int[] data = new int[256];
    int size;

    void add(int v) {
      if (size == data.length) {
        data = Arrays.copyOf(data, size * 2);
      }
      data[size++] = v;
    }
  }
}
