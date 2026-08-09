package com.linkedin.metadata.buffer;

import com.datahub.util.RecordUtils;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.buffer.offload.HazelcastOffloadBuffer;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.entity.hooks.buffer.HookKey;
import com.linkedin.metadata.entity.hooks.buffer.HookPayload;
import com.linkedin.metadata.entity.hooks.buffer.PostCommitHookBuffer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link PostCommitHookBuffer} — a thin adapter over the framework {@link
 * HazelcastOffloadBuffer} that fixes the hook feature bits ({@link MergePolicy#NO_COALESCE} +
 * {@link SizingPolicy#REJECT_AT_CAP}) and translates the {@link PostCommitHookBuffer} MCL-typed
 * {@code enqueue} into the framework's {@code (K, V)} {@code enqueue} (MCL → {@link HookPayload}
 * JSON). All infra (drain lock, paging drain, CAS clear, sequence CAS, size-check reject) lives in
 * the framework; this class adds only the hook-specific key/value binding and the FIFO drain
 * comparator.
 *
 * <p><b>No coalescing:</b> every enqueue uses a globally-unique monotonic sequence so {@link
 * HookKey}s never collide — each committed MCL is replayed exactly once. This is the deliberate
 * departure from the retention buffer: hooks read the MCL's previous-aspect to compute a
 * per-transition delta, so collapsing two MCLs into one entry would lose intermediate side effects.
 *
 * <p><b>Sizing:</b> {@link SizingPolicy#REJECT_AT_CAP} — the framework's {@code enqueue} does a
 * cluster-wide {@code IMap.size()} check and returns {@code false} at cap, so the caller falls back
 * to synchronous hook execution (bounded memory, no data loss). No {@code EvictionConfig} (eviction
 * would silently drop a distinct committed MCL = lost side effect).
 *
 * <p>Construction is done by {@code PostCommitHookBufferFactory} via the shared {@code
 * OffloadBufferFactory}; this class is public only so the factory can build the delegate and wrap
 * it, and so {@link #drainOrder()} can supply the framework's drain comparator.
 */
@Slf4j
public class HazelcastPostCommitHookBuffer implements PostCommitHookBuffer {

  private final HazelcastOffloadBuffer<HookKey, HookPayload> delegate;

  public HazelcastPostCommitHookBuffer(
      @Nonnull HazelcastOffloadBuffer<HookKey, HookPayload> delegate) {
    this.delegate = delegate;
  }

  /**
   * The framework buffer this adapter wraps. The drainer is wired against the framework {@link
   * com.linkedin.metadata.buffer.offload.OffloadBuffer} type (not the hook-specific {@link
   * PostCommitHookBuffer}), so the factory passes this to {@code
   * OffloadBufferFactory.createDrainer}.
   */
  @Nonnull
  public HazelcastOffloadBuffer<HookKey, HookPayload> getDelegate() {
    return delegate;
  }

  /** Convenience constructor for tests: builds the delegate from the raw Hazelcast instance. */
  public HazelcastPostCommitHookBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String name,
      @Nonnull String lockMapName,
      int maxPendingEntries,
      @Nullable MetricUtils metricUtils) {
    this.delegate =
        new HazelcastOffloadBuffer<>(
            hazelcastInstance,
            name,
            lockMapName,
            name + ".seq",
            maxPendingEntries,
            MergePolicy.NO_COALESCE,
            SizingPolicy.REJECT_AT_CAP,
            new HookDrainOrder(),
            "post_commit_hook",
            metricUtils);
  }

  @Override
  public long nextSequence() {
    return delegate.nextSequence();
  }

  @Override
  public long nextSequence(int count) {
    return delegate.nextSequence(count);
  }

  @Override
  public boolean enqueue(@Nonnull HookKey key, @Nonnull MetadataChangeLog mcl) {
    HookPayload payload = new HookPayload(RecordUtils.toJsonString(mcl));
    return delegate.enqueue(key, payload);
  }

  @Override
  public boolean enqueueBatch(@Nonnull List<Map.Entry<HookKey, MetadataChangeLog>> entries) {
    // MCL → HookPayload (JSON), then one putAll on the framework buffer. Keys are unique
    // (sequence), so the map preserves every entry.
    List<Map.Entry<HookKey, HookPayload>> batch = new ArrayList<>(entries.size());
    for (Map.Entry<HookKey, MetadataChangeLog> e : entries) {
      batch.add(Map.entry(e.getKey(), new HookPayload(RecordUtils.toJsonString(e.getValue()))));
    }
    return delegate.enqueueBatch(batch);
  }

  @Override
  public boolean defersApply() {
    return true;
  }

  @Override
  @Nonnull
  public List<Map.Entry<HookKey, HookPayload>> drain(int limit) {
    return delegate.drain(limit);
  }

  @Override
  public boolean removeIfSame(@Nonnull HookKey key, @Nonnull HookPayload expected) {
    return delegate.removeIfSame(key, expected);
  }

  @Override
  public void requeue(@Nonnull HookKey key, @Nonnull HookPayload payload) {
    delegate.requeue(key, payload);
  }

  @Override
  @Nullable
  public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    return delegate.tryAcquireDrainLock(lockName, lease);
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token) {
    delegate.releaseDrainLock(lockName, token);
  }

  /** The framework drain comparator for the hook buffer, ordered by enqueue sequence (FIFO). */
  @Nonnull
  public static Comparator<Map.Entry<HookKey, HookPayload>> drainOrder() {
    return new HookDrainOrder();
  }

  /** Serializable FIFO comparator over {@link HookKey#getSequence()}. */
  public static final class HookDrainOrder
      implements Comparator<Map.Entry<HookKey, HookPayload>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Map.Entry<HookKey, HookPayload> a, Map.Entry<HookKey, HookPayload> b) {
      int bySeq = Long.compare(a.getKey().getSequence(), b.getKey().getSequence());
      if (bySeq != 0) {
        return bySeq;
      }
      return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
    }
  }
}
