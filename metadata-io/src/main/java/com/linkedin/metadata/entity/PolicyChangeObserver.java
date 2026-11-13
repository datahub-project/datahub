package com.linkedin.metadata.entity;

/**
 * Called by {@link EntityServiceImpl} after a write or delete that changed a dataHubPolicy has been
 * committed. The authorizer uses it to reload its policy cache before the write returns.
 *
 * <p>Runs on the writing thread. Implementations may block for a bounded time; exceptions are
 * logged and never fail the write.
 */
@FunctionalInterface
public interface PolicyChangeObserver {
  void onPolicyChange();
}
