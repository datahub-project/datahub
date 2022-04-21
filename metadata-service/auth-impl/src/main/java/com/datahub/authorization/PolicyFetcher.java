package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;


/**
 * Wrapper around entity client to fetch policies in a paged manner
 */
@Slf4j
@RequiredArgsConstructor
public class PolicyFetcher {
  private final EntityClient _entityClient;

  private static final SortCriterion POLICY_SORT_CRITERION =
      new SortCriterion().setField("lastUpdatedTimestamp").setOrder(SortOrder.DESCENDING);

  public PolicyFetchResult fetchPolicies(int start, int count, Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    log.debug(String.format("Batch fetching policies. start: %s, count: %s ", start, count));
    // First fetch all policy urns from start - start + count
    SearchResult result =
        _entityClient.search(POLICY_ENTITY_NAME, "*", null, POLICY_SORT_CRITERION, start, count, authentication);
    List<Urn> policyUrns = result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());

    if (policyUrns.isEmpty()) {
      return new PolicyFetchResult(Collections.emptyList(), 0);
    }

    // Fetch DataHubPolicyInfo aspects for each urn
    final Map<Urn, EntityResponse> policyEntities =
        _entityClient.batchGetV2(POLICY_ENTITY_NAME, new HashSet<>(policyUrns), null, authentication);
    return new PolicyFetchResult(policyUrns.stream()
        .map(policyEntities::get)
        .filter(Objects::nonNull)
        .map(this::extractPolicy)
        .filter(Objects::nonNull)
        .collect(Collectors.toList()), result.getNumEntities());
  }

  private Policy extractPolicy(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      // Right after deleting the policy, there could be a small time frame where search and local db is not consistent.
      // Simply return null in that case
      return null;
    }
    return new Policy(entityResponse.getUrn(),
        new DataHubPolicyInfo(aspectMap.get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data()));
  }

  @Value
  public static class PolicyFetchResult {
    List<Policy> policies;
    int total;
  }

  @Value
  public static class Policy {
    Urn urn;
    DataHubPolicyInfo policyInfo;
  }
}
