package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Called by {@link EntityServiceImpl} after a client's write or delete of a dataHubPolicy has been
 * committed. With POLICY_CACHE_IMMEDIATE_UPDATE_ENABLED, GMS registers its authorizer here to read
 * the changed policies into its cache before the write returns.
 *
 * <p>Only writes with a request context call it: the MCE consumer, which commits queued writes,
 * does not.
 *
 * <p>Runs on the writing thread. Implementations may block for a bounded time; exceptions are
 * logged and never fail the write.
 */
@FunctionalInterface
public interface PolicyChangeObserver {
  void onPolicyChange(@Nonnull Set<Urn> policyUrns);
}
