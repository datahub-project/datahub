package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authorization.PolicyFetcher;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesInput;
import com.linkedin.datahub.graphql.generated.ListPoliciesResult;
import com.linkedin.datahub.graphql.generated.Policy;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.policy.mappers.PolicyInfoPolicyMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ListPoliciesResolver implements DataFetcher<CompletableFuture<ListPoliciesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final PolicyFetcher _policyFetcher;

  public ListPoliciesResolver(final EntityClient entityClient) {
    _policyFetcher = new PolicyFetcher(entityClient);
  }

  @Override
  public CompletableFuture<ListPoliciesResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (PolicyAuthUtils.canManagePolicies(context)) {
      final ListPoliciesInput input =
          bindArgument(environment.getArgument("input"), ListPoliciesInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
      final List<AndFilterInput> filters =
          input.getOrFilters() != null ? input.getOrFilters() : new ArrayList<>();
      final List<FacetFilterInput> facetFilters =
          filters.stream()
              .map(AndFilterInput::getAnd)
              .flatMap(List::stream)
              .collect(Collectors.toList());
      log.debug(
          "User {} listing policies with filters {}", context.getActorUrn(), filters.toString());

      final Filter filter = ResolverUtils.buildFilter(facetFilters, Collections.emptyList());

      return _policyFetcher
          .fetchPolicies(context.getOperationContext(), start, query, count, filter)
          .thenApply(
              policyFetchResult -> {
                final ListPoliciesResult result = new ListPoliciesResult();
                result.setStart(start);
                result.setCount(count);
                result.setTotal(policyFetchResult.getTotal());
                result.setPolicies(mapEntities(context, policyFetchResult.getPolicies()));
                return result;
              });
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private static List<Policy> mapEntities(
      @Nullable QueryContext context, final List<PolicyFetcher.Policy> policies) {
    return policies.stream()
        .map(
            policy -> {
              Policy mappedPolicy = PolicyInfoPolicyMapper.map(context, policy.getPolicyInfo());
              mappedPolicy.setUrn(policy.getUrn().toString());
              return mappedPolicy;
            })
        .collect(Collectors.toList());
  }
}
