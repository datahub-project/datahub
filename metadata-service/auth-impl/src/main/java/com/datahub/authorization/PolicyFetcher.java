package com.datahub.authorization;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Wrapper around entity client to fetch policies in a paged manner */
@Slf4j
@RequiredArgsConstructor
public class PolicyFetcher {
  private final EntityClient _entityClient;

  private static final SortCriterion POLICY_SORT_CRITERION =
      new SortCriterion().setField("lastUpdatedTimestamp").setOrder(SortOrder.DESCENDING);

  /**
   * This is to provide a scroll implementation using the start/count api. It is not efficient and
   * the scroll native functions should be used instead. This does fix a failure to fetch policies
   * when deep pagination happens where there are >10k policies. Exists primarily to prevent
   * breaking change to the graphql api.
   */
  @Deprecated
  public CompletableFuture<PolicyFetchResult> fetchPolicies(
      int start, String query, int count, Authentication authentication) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            PolicyFetchResult result = PolicyFetchResult.EMPTY;
            String scrollId = "";
            int fetchedResults = 0;

            while (PolicyFetchResult.EMPTY.equals(result) && scrollId != null) {
              PolicyFetchResult tmpResult =
                  fetchPolicies(query, count, scrollId.isEmpty() ? null : scrollId, authentication);
              fetchedResults += tmpResult.getPolicies().size();
              scrollId = tmpResult.getScrollId();
              if (fetchedResults > start) {
                result = tmpResult;
              }
            }

            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list policies", e);
          }
        });
  }

  public PolicyFetchResult fetchPolicies(
      int count, @Nullable String scrollId, Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    return fetchPolicies("", count, scrollId, authentication);
  }

  public PolicyFetchResult fetchPolicies(
      String query, int count, @Nullable String scrollId, Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    log.debug(String.format("Batch fetching policies. count: %s, scroll: %s", count, scrollId));

    // First fetch all policy urns
    ScrollResult result =
        _entityClient.scrollAcrossEntities(
            List.of(POLICY_ENTITY_NAME),
            query,
            null,
            scrollId,
            null,
            count,
            new SearchFlags()
                .setSkipCache(true)
                .setSkipAggregates(true)
                .setSkipHighlighting(true)
                .setFulltext(true),
            authentication);
    List<Urn> policyUrns =
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());

    if (policyUrns.isEmpty()) {
      return PolicyFetchResult.EMPTY;
    }

    // Fetch DataHubPolicyInfo aspects for each urn
    final Map<Urn, EntityResponse> policyEntities =
        _entityClient.batchGetV2(
            POLICY_ENTITY_NAME, new HashSet<>(policyUrns), null, authentication);
    return new PolicyFetchResult(
        policyUrns.stream()
            .map(policyEntities::get)
            .filter(Objects::nonNull)
            .map(this::extractPolicy)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()),
        result.getNumEntities(),
        result.getScrollId());
  }

  private Policy extractPolicy(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      // Right after deleting the policy, there could be a small time frame where search and local
      // db is not consistent.
      // Simply return null in that case
      return null;
    }
    return new Policy(
        entityResponse.getUrn(),
        new DataHubPolicyInfo(aspectMap.get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data()));
  }

  @Value
  public static class PolicyFetchResult {
    List<Policy> policies;
    int total;
    @Nullable String scrollId;

    public static final PolicyFetchResult EMPTY =
        new PolicyFetchResult(Collections.emptyList(), 0, null);
  }

  @Value
  public static class Policy {
    Urn urn;
    DataHubPolicyInfo policyInfo;
  }
}
