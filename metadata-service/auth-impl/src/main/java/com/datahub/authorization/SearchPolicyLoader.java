package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.google.common.collect.Iterables;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Reads policies without scanning the aspect table: the search index lists the policy URNs, and the
 * primary store is read only by primary key.
 */
public class SearchPolicyLoader implements PolicyLoader {

  private final OperationContext systemOpContext;
  private final SystemEntityClient entityClient;
  private final PolicyFetcher policyFetcher;
  private final int fetchSize;

  public SearchPolicyLoader(
      @Nonnull OperationContext systemOpContext,
      @Nonnull SystemEntityClient entityClient,
      int fetchSize) {
    this.systemOpContext = Objects.requireNonNull(systemOpContext);
    this.entityClient = Objects.requireNonNull(entityClient);
    this.policyFetcher = new PolicyFetcher(entityClient);
    this.fetchSize = fetchSize;
  }

  @Nonnull
  @Override
  public List<PolicyFetcher.Policy> loadAll() {
    final List<PolicyFetcher.Policy> policies = new ArrayList<>();
    String scrollId = null;
    do {
      final PolicyFetcher.PolicyFetchResult page;
      try {
        page = policyFetcher.fetchPolicies(systemOpContext, fetchSize, scrollId, null);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to list policies from the search index", e);
      }
      policies.addAll(page.getPolicies());
      scrollId = page.getScrollId();
    } while (scrollId != null);
    return policies;
  }

  /**
   * Reads by primary key, in chunks of the fetch size. A policy with {@code status.removed=true} is
   * left out, as the search listing leaves it out. The dataHubPolicy entity has no status aspect
   * today, so this only matters if one is added.
   */
  @Nonnull
  @Override
  public Map<Urn, PolicyFetcher.Policy> loadByUrns(@Nonnull Set<Urn> urns) {
    final Map<Urn, PolicyFetcher.Policy> result = new HashMap<>();
    for (List<Urn> chunk : Iterables.partition(urns, Math.max(1, fetchSize))) {
      final Map<Urn, EntityResponse> responses;
      try {
        // No cache: a cached read could return the value from before the change being applied.
        responses =
            entityClient.batchGetV2NoCache(
                systemOpContext,
                POLICY_ENTITY_NAME,
                new HashSet<>(chunk),
                Set.of(DATAHUB_POLICY_INFO_ASPECT_NAME, STATUS_ASPECT_NAME));
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to read "
                + chunk.size()
                + " policies, starting with "
                + chunk.subList(0, Math.min(chunk.size(), 5)),
            e);
      }
      for (Map.Entry<Urn, EntityResponse> entry : responses.entrySet()) {
        final EnvelopedAspect info =
            entry.getValue().getAspects().get(DATAHUB_POLICY_INFO_ASPECT_NAME);
        final EnvelopedAspect status = entry.getValue().getAspects().get(STATUS_ASPECT_NAME);
        if (info == null || (status != null && new Status(status.getValue().data()).isRemoved())) {
          continue;
        }
        result.put(
            entry.getKey(),
            new PolicyFetcher.Policy(
                entry.getKey(), new DataHubPolicyInfo(info.getValue().data())));
      }
    }
    return result;
  }
}
