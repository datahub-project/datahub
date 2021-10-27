package com.linkedin.datahub.graphql.resolvers.policy;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListPoliciesInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesResult;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.aspect.DataHubPolicyAspect;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.snapshot.DataHubPolicySnapshot;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class ListPoliciesResolver implements DataFetcher<CompletableFuture<ListPoliciesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String POLICY_ENTITY_NAME = "dataHubPolicy";

  private final RestliEntityClient _entityClient;

  public ListPoliciesResolver(final RestliEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListPoliciesResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (PolicyAuthUtils.canManagePolicies(context)) {
      final ListPoliciesInput input = bindArgument(environment.getArgument("input"), ListPoliciesInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all policy Urns.
          final ListUrnsResult gmsResult = _entityClient.listUrns(POLICY_ENTITY_NAME, start, count, context.getActor());

          // Then, get all policies. TODO: Migrate batchGet to return GenericAspects, to avoid requiring a snapshot.
          final Map<Urn, Entity> entities = _entityClient.batchGet(new HashSet<>(gmsResult.getEntities()), context.getActor());

          // Now that we have entities we can bind this to a result.
          final ListPoliciesResult result = new ListPoliciesResult();
          result.setStart(gmsResult.getStart());
          result.setCount(gmsResult.getCount());
          result.setTotal(gmsResult.getTotal());
          result.setPolicies(mapEntities(entities.values()));
          return result;

        } catch (Exception e) {
          throw new RuntimeException("Failed to list policies", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<Policy> mapEntities(final Collection<Entity> entities) {
    final List<Policy> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final DataHubPolicySnapshot snapshot = entity.getValue().getDataHubPolicySnapshot();
      results.add(mapPolicySnapshot(snapshot));
    }
    return results;
  }

  private Policy mapPolicySnapshot(final DataHubPolicySnapshot snapshot) {
    for (DataHubPolicyAspect aspect : snapshot.getAspects()) {
      if (aspect.isDataHubPolicyInfo()) {
        // TODO: Consider wrapping into a standard "info" field.
        Policy policy = PolicyInfoPolicyMapper.map(aspect.getDataHubPolicyInfo());
        policy.setUrn(snapshot.getUrn().toString());
        return policy;
      }
    }
    // If the policy exists, it should always have DataHubPolicyInfo.
    throw new IllegalArgumentException(
        String.format("Failed to find DataHubPolicyInfo aspect in DataHubPolicySnapshot data %s. Invalid state.", snapshot.data()));
  }
}
