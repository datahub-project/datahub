package com.linkedin.metadata.entity.hooks.buffer;

import com.linkedin.mxe.MetadataChangeLog;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Async dispatch for {@link
 * com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect#postMCPSideEffect} post-commit hooks.
 * When wired and {@link #defersApply()} true, the ingest thread does not run post-commit hooks
 * inline; instead it enqueues the committed MCL (under a routing-aware {@link HookKey} built by a
 * {@link HookContextResolver}) and a background {@link PostCommitHookDrainer} replays each MCL
 * through its hook off the request thread, under the correct (route-aware) {@link
 * io.datahubproject.metadata.context.OperationContext}.
 *
 * <p><b>No coalescing.</b> Every enqueue uses a globally-unique sequence (see {@link
 * #nextSequence()}) so {@link HookKey}s never collide — each committed MCL is replayed exactly
 * once, in FIFO order, so no intermediate transition is ever dropped (the DB ledger is never
 * touched by this buffer; only the timing of derived side-effect MCP generation changes). This is
 * the deliberate departure from the retention buffer: hooks read the MCL's previous-aspect to
 * compute a per-transition delta, so collapsing two MCLs into one entry would lose intermediate
 * side effects.
 *
 * <p><b>Delivery = at-least-once.</b> The backing map is in-memory (Hazelcast {@code
 * backupCount=1}, survives a pod loss, not a full cluster restart). {@link #enqueue} returns {@code
 * false} on buffer-write failure so the caller falls back to synchronous hook execution — no side
 * effect is ever silently dropped at enqueue. A failed replay leaves the key in the buffer for the
 * next tick; a poison key that never succeeds is moved to a bounded DLQ after N retries. A full
 * cluster restart loses pending work; recovery is via a documented re-sync (re-fire hooks for an
 * affected URN), since hooks re-derive from current state and a re-fire converges to the correct
 * end state.
 *
 * <p>The drain/lock surface mirrors {@link com.linkedin.metadata.buffer.offload.OffloadBuffer} so a
 * Hazelcast-backed implementation can share the same non-reentrant, token-fenced drain lock and
 * {@link com.hazelcast.query.PagingPredicate} batched drain.
 */
public interface PostCommitHookBuffer {

  /**
   * Allocate the next globally-unique monotonic enqueue sequence. Called by the ingest thread
   * before {@link HookContextResolver#enrichKey} so the enriched {@link HookKey} carries a distinct
   * sequence (no coalescing). Only meaningful when {@link #defersApply()} is true.
   */
  long nextSequence();

  /**
   * Enqueue a committed MCL (under its enriched {@link HookKey}) for deferred replay.
   *
   * @return {@code true} if the entry was durably written to the buffer; {@code false} on
   *     buffer-write failure (partition / serialization / CP error). On {@code false} the caller
   *     MUST fall back to running the hook synchronously so no side effect is silently dropped.
   */
  boolean enqueue(@Nonnull HookKey key, @Nonnull MetadataChangeLog mcl);

  /**
   * @return true if callers must NOT run post-commit hooks synchronously; a background drainer will
   *     replay them instead. False means there is no buffer backing this instance and callers
   *     should run hooks inline (legacy behavior).
   */
  boolean defersApply();

  /**
   * Drain up to {@code limit} pending entries in a stable order for replay. Only called by the
   * background drainer (when {@link #defersApply()} is true).
   */
  @Nonnull
  List<Map.Entry<HookKey, HookPayload>> drain(int limit);

  /**
   * Remove {@code key} only if its current value is still {@code expected} (CAS clear after a
   * successful replay; guards against a re-enqueue having replaced the value mid-drain).
   */
  boolean removeIfSame(@Nonnull HookKey key, @Nonnull HookPayload expected);

  /**
   * Re-put an existing pending entry (same unique key) with an updated payload — used by the
   * drainer to bump a failed entry's retry count and leave it for the next tick. Unlike {@link
   * #enqueue}, this does NOT allocate a new sequence: the key is the original enqueue's key, so the
   * entry keeps its FIFO position and is not duplicated.
   */
  void requeue(@Nonnull HookKey key, @Nonnull HookPayload payload);

  /**
   * Non-reentrant, token-fenced cluster-wide drain lock. Returns a fencing token on acquire (then
   * {@link #releaseDrainLock} clears it) or {@code null} if another pod already holds the lock.
   */
  @Nullable
  Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease);

  /** Release the drain lock only if the stored token is still ours (lease-expired → no-op). */
  void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token);

  /** No-op buffer (default): hooks run synchronously inline whenever the buffer is not wired. */
  PostCommitHookBuffer NO_OP =
      new PostCommitHookBuffer() {
        @Override
        public long nextSequence() {
          return 0L;
        }

        @Override
        public boolean enqueue(HookKey key, MetadataChangeLog mcl) {
          return false;
        }

        @Override
        public boolean defersApply() {
          return false;
        }

        @Nonnull
        @Override
        public List<Map.Entry<HookKey, HookPayload>> drain(int limit) {
          return List.of();
        }

        @Override
        public boolean removeIfSame(HookKey key, HookPayload expected) {
          return false;
        }

        @Override
        public void requeue(HookKey key, HookPayload payload) {}

        @Nullable
        @Override
        public Object tryAcquireDrainLock(String lockName, Duration lease) {
          return null;
        }

        @Override
        public void releaseDrainLock(String lockName, Object token) {}
      };
}
