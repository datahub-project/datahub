package com.linkedin.datahub.graphql.resolvers.policy;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListPoliciesInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesResult;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.types.policy.mappers.PolicyMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.ListUrnsResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


public class ListPoliciesResolver implements DataFetcher<CompletableFuture<ListPoliciesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListPoliciesResolver(final EntityClient entityClient) {
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
          final ListUrnsResult gmsResult = _entityClient.listUrns(POLICY_ENTITY_NAME, start, count, context.getAuthentication());

          // Then, get all policies. TODO: Migrate batchGet to return GenericAspects, to avoid requiring a snapshot.
          final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(POLICY_ENTITY_NAME,
              new HashSet<>(gmsResult.getEntities()), null, context.getAuthentication());

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

  private List<Policy> mapEntities(final Collection<EntityResponse> entities) {
    return entities.stream()
        .map(PolicyMapper::map)
        .collect(Collectors.toList());
  }
}
