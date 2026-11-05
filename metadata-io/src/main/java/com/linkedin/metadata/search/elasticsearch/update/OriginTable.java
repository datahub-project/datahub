package com.linkedin.metadata.search.elasticsearch.update;

import io.opentelemetry.api.trace.SpanContext;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Origin spans of write actions, keyed by the action instance, for {@link BulkTelemetry}.
 *
 * <p>Built for the add path, which runs once per queued write action: a {@link ConcurrentHashMap},
 * so recording an origin takes no table-wide lock (an insert into an empty bin is a CAS; a
 * collision locks one bin) and allocates one map node. Keys are held strongly: the bulk processor
 * holds every queued action until its batch is sent, so this table does not extend an action's life
 * on the normal path, and the callers remove an action's origin on every path that drops the action
 * (batch start, rejected add, requeue, give-up, close).
 *
 * <p>Keys compare by identity because the OpenSearch write requests ({@code IndexRequest}, {@code
 * UpdateRequest}, {@code DeleteRequest}) do not override {@code equals} or {@code hashCode}; a key
 * type that did would make two equal actions share one origin.
 *
 * <p>Size is bounded by {@code capacity}. Reaching it means origins are leaking (normal use holds
 * at most the processors' unflushed actions), so instead of refusing new origins forever, which is
 * how attribution used to stop silently, the table is cleared: every origin it held is dropped and
 * counted, and recording resumes. The clear is the rare path and takes a lock; the size check is
 * approximate under concurrency, so the table can briefly exceed {@code capacity} by the number of
 * concurrent writers.
 */
final class OriginTable {

  private final ConcurrentHashMap<Object, SpanContext> origins = new ConcurrentHashMap<>();
  private final int capacity;
  private final AtomicLong dropped = new AtomicLong();
  private final ReentrantLock clearing = new ReentrantLock();

  OriginTable(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be positive: " + capacity);
    }
    this.capacity = capacity;
  }

  /**
   * Records {@code origin} for {@code action}. Returns how many origins were dropped to make room:
   * zero, or the whole table's content when it had reached capacity.
   */
  long put(@Nonnull Object action, @Nonnull SpanContext origin) {
    long cleared = 0;
    if (origins.size() >= capacity) {
      cleared = clearIfFull();
    }
    origins.put(action, origin);
    return cleared;
  }

  private long clearIfFull() {
    if (!clearing.tryLock()) {
      return 0; // another writer is clearing right now
    }
    try {
      if (origins.size() < capacity) {
        return 0;
      }
      long n = origins.size();
      origins.clear();
      dropped.addAndGet(n);
      return n;
    } finally {
      clearing.unlock();
    }
  }

  @Nullable
  SpanContext remove(@Nullable Object action) {
    return action == null ? null : origins.remove(action);
  }

  /** Empties the table; not counted as drops (the caller is releasing everything on purpose). */
  void clear() {
    origins.clear();
  }

  int size() {
    return origins.size();
  }

  /** Origins dropped so far because the table reached capacity. */
  long dropped() {
    return dropped.get();
  }
}
