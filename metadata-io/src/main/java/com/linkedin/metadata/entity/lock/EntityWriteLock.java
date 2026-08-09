package com.linkedin.metadata.entity.lock;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * Serializes concurrent entity writers on the same URN(s) <b>before</b> the write transaction
 * opens, so waiters queue OFF the DB connection — unlike a DB advisory lock, which pins a pooled
 * connection while blocked. This matters when DB connections are the bottleneck.
 *
 * <p><b>Liveness-only, best-effort.</b> Optimistic-locking CAS on {@code SystemMetadata.version}
 * remains the correctness guard, so a lock that cannot be taken (timeout, backend outage,
 * split-brain, lease expiry) NEVER blocks a write — the writer just proceeds without serialization
 * and CAS still prevents lost updates. Implementations therefore MUST NOT throw on acquire failure,
 * and MUST acquire in a deterministic (sorted) order to stay deadlock-free.
 *
 * <p>Acquire and the matching {@link LockHandle#close()} run on the same thread.
 */
public interface EntityWriteLock {

  /**
   * Best-effort acquire of per-URN write locks (sorted internally for deadlock-freedom). Returns a
   * handle that releases exactly what was acquired. Never throws for acquisition failure.
   */
  @Nonnull
  LockHandle acquire(@Nonnull OperationContext opContext, @Nonnull Collection<String> urns);

  /** Releases the locks taken by one {@link #acquire} call. {@link #close()} never throws. */
  interface LockHandle extends AutoCloseable {
    @Override
    void close();
  }
}
