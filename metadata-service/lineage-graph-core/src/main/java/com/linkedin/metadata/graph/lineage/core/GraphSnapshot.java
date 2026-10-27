package com.linkedin.metadata.graph.lineage.core;

import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One loaded snapshot of the graph, as walks see it, and the memory behind it.
 *
 * <p>A walk takes the current snapshot with {@link #tryAcquire()} and keeps it to the end, then
 * calls {@link #release()}. When a newer snapshot is published the old one is {@link #retire()}d:
 * its arena is closed, unmapping the snapshot's files, as soon as no walk holds it. Nothing is left
 * to the garbage collector, so at most one old snapshot stays mapped, and only while walks that
 * started on it finish.
 *
 * <p>A snapshot built in memory (tests, benches) can use an automatic arena; it is then never
 * closed explicitly.
 */
public final class GraphSnapshot {

  // Bit 0 set once retired; the rest counts holders, plus one for the publisher until retired.
  private final AtomicInteger state = new AtomicInteger(2);

  final BaseGraph base;
  private final long builtAtMillis;
  private final long pointInTimeMillis;
  private final Arena arena;
  private volatile Runnable onClosed;

  public GraphSnapshot(BaseGraph base, long builtAtMillis, long pointInTimeMillis, Arena arena) {
    this.base = base;
    this.builtAtMillis = builtAtMillis;
    this.pointInTimeMillis = pointInTimeMillis;
    this.arena = arena;
  }

  /** A snapshot of a graph built in memory, for tests and benches. */
  public static GraphSnapshot of(BaseGraph base) {
    long now = System.currentTimeMillis();
    return new GraphSnapshot(base, now, now, null);
  }

  /** Takes a reference for a walk; false if the snapshot was retired and may be unmapped. */
  public boolean tryAcquire() {
    while (true) {
      int s = state.get();
      if ((s & 1) != 0) {
        return false;
      }
      if (state.compareAndSet(s, s + 2)) {
        return true;
      }
    }
  }

  public void release() {
    int s = state.addAndGet(-2);
    if (s == 1) {
      close();
    }
  }

  /**
   * Marks the snapshot replaced and drops the publisher's reference; the arena closes when the last
   * walk releases it. {@code onClosed} runs after that, on the releasing thread.
   */
  public void retire(Runnable onClosed) {
    this.onClosed = onClosed;
    int s = state.updateAndGet(v -> (v | 1) - 2);
    if (s == 1) {
      close();
    }
  }

  /** Walks currently holding the snapshot. */
  public int holders() {
    int s = state.get();
    return (s >> 1) - ((s & 1) == 0 ? 1 : 0);
  }

  public boolean retired() {
    return (state.get() & 1) != 0;
  }

  private void close() {
    if (arena != null && arena.scope().isAlive()) {
      arena.close();
    }
    Runnable r = onClosed;
    if (r != null) {
      r.run();
    }
  }

  public BaseGraph base() {
    return base;
  }

  /** When the builder finished this snapshot. */
  public long builtAtMillis() {
    return builtAtMillis;
  }

  /** When the scan's point in time was opened: the graph reflects writes up to here. */
  public long pointInTimeMillis() {
    return pointInTimeMillis;
  }

  public int nodeCount() {
    return base.nodes;
  }

  public TypeTable entityTypes() {
    return base.entityTypes;
  }

  public TypeTable relTypes() {
    return base.relTypes;
  }

  /** Node id of {@code urn}, or -1. */
  public int find(String urn) {
    return base.urns.find(urn);
  }

  public String urn(int node) {
    return base.urns.get(node);
  }

  public int entityType(int node) {
    return base.entityType(node);
  }
}
