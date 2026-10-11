package com.linkedin.metadata.entity.lock;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * No pre-transaction serialization: concurrent writers rely purely on optimistic-locking CAS. Used
 * when {@code entityWriteLockBackend=none}, or when {@code =hazelcast} but no Hazelcast instance is
 * available.
 */
public final class NoOpEntityWriteLock implements EntityWriteLock {

  private static final LockHandle NOOP_HANDLE = () -> {};

  @Nonnull
  @Override
  public LockHandle acquire(@Nonnull OperationContext opContext, @Nonnull Collection<String> urns) {
    return NOOP_HANDLE;
  }
}
