package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.authorization.PolicyFetcher;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListPoliciesInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesResult;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


public class ListPoliciesResolver implements DataFetcher<CompletableFuture<ListPoliciesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final PolicyFetcher _policyFetcher;

  public ListPoliciesResolver(final EntityClient entityClient) {
    _policyFetcher = new PolicyFetcher(entityClient);
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
          final PolicyFetcher.PolicyFetchResult policyFetchResult =
              _policyFetcher.fetchPolicies(start, count, context.getAuthentication());

          // Now that we have entities we can bind this to a result.
          final ListPoliciesResult result = new ListPoliciesResult();
          result.setStart(start);
          result.setCount(count);
          result.setTotal(policyFetchResult.getTotal());
          result.setPolicies(mapEntities(policyFetchResult.getPolicies()));
          return result;
        } catch (Exception e) {
          throw new RuntimeException("Failed to list policies", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<Policy> mapEntities(final List<PolicyFetcher.Policy> policies) {
    return policies.stream().map(policy -> {
      Policy mappedPolicy = PolicyInfoPolicyMapper.map(policy.getPolicyInfo());
      mappedPolicy.setUrn(policy.getUrn().toString());
      return mappedPolicy;
    }).collect(Collectors.toList());
  }
}
