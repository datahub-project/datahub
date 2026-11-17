package com.linkedin.gms.factory.policycache;

import com.datahub.authorization.DataHubAuthorizer;
import com.hazelcast.map.IMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.PolicyChangeObserver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * After a committed policy write, signals the change to the other GMS members through {@link
 * com.datahub.authorization.PolicyChangeMap} (with POLICY_CACHE_BROADCAST_ENABLED), then updates
 * this GMS's policy cache.
 */
@Slf4j
public class ClusterPolicyChangeObserver implements PolicyChangeObserver {

  private final DataHubAuthorizer authorizer;
  @Nullable private final IMap<String, Long> changes;
  @Nullable private final MetricUtils metricUtils;

  /**
   * @param changes the cluster change map, or null when the broadcast is off
   */
  public ClusterPolicyChangeObserver(
      @Nonnull DataHubAuthorizer authorizer,
      @Nullable IMap<String, Long> changes,
      @Nullable MetricUtils metricUtils) {
    this.authorizer = authorizer;
    this.changes = changes;
    this.metricUtils = metricUtils;
  }

  @Override
  public void onPolicyChange(@Nonnull Set<Urn> policyUrns, boolean clientWrite) {
    // Signal first: the write is committed, so other members can read it now instead of after this
    // member's wait below.
    if (changes != null) {
      publish(changes, policyUrns);
    }
    if (clientWrite) {
      authorizer.onPolicyChange(policyUrns);
    } else {
      authorizer.requestUpdate(policyUrns);
    }
  }

  /**
   * Asynchronous, so an unreachable partition owner cannot hold the writing thread (or the MCE
   * consumer in GMS) for the Hazelcast operation timeout. The write is committed either way; a
   * failed signal leaves other members on their refresh timer.
   */
  private void publish(@Nonnull IMap<String, Long> map, @Nonnull Set<Urn> policyUrns) {
    final long now = System.currentTimeMillis();
    for (Urn urn : policyUrns) {
      try {
        map.setAsync(urn.toString(), now)
            .whenComplete(
                (ignored, error) -> {
                  if (error != null) {
                    publishFailed(urn, error);
                  }
                });
      } catch (RuntimeException e) {
        publishFailed(urn, e);
      }
    }
  }

  private void publishFailed(@Nonnull Urn urn, @Nonnull Throwable error) {
    if (metricUtils != null) {
      metricUtils.increment(ClusterPolicyChangeObserver.class, "policyChange.publish_failed", 1);
    }
    log.warn(
        "Failed to signal the policy change for {} to other GMS members; they read it on their"
            + " refresh timer.",
        urn,
        error);
  }
}
