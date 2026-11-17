package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Called by {@link EntityServiceImpl} after a write or delete of a dataHubPolicy has been
 * committed. With POLICY_CACHE_IMMEDIATE_UPDATE_ENABLED, GMS registers an observer here that reads
 * the changed policies into its cache and, with POLICY_CACHE_BROADCAST_ENABLED, signals them to the
 * other GMS members.
 *
 * <p>{@code clientWrite} is true when the write has a request context. A client write may wait for
 * the cache read before it returns. A queued write that the MCE consumer embedded in GMS commits
 * has no request context: it must not wait, so the consumer is never slowed by a cache read. The
 * standalone MCE consumer registers no observer. A consumer set to send writes to GMS ({@code
 * entityClient.impl=restli}) does so through a request, so GMS sees those as client writes.
 *
 * <p>Runs on the writing thread. Implementations may block for a bounded time on a client write
 * only; exceptions are logged and never fail the write.
 */
@FunctionalInterface
public interface PolicyChangeObserver {
  void onPolicyChange(@Nonnull Set<Urn> policyUrns, boolean clientWrite);
}
