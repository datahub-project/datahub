package com.linkedin.datahub.graphql.resolvers.marketplace;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetRootDataProductsResult;
import com.linkedin.datahub.graphql.generated.GetRootEntitiesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GetRootDataProductsResolver
    implements DataFetcher<CompletableFuture<GetRootDataProductsResult>> {

  static final String HAS_PARENT_DATA_PRODUCT_FIELD_NAME = "hasParentDataProduct";
  private static final String DEFAULT_QUERY = "*";

  private final EntityClient _entityClient;

  public GetRootDataProductsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetRootDataProductsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = getQueryContext(environment);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final GetRootEntitiesInput input =
              bindArgument(environment.getArgument("input"), GetRootEntitiesInput.class);
          final Integer start = input.getStart() == null ? 0 : input.getStart();
          final Integer count = input.getCount() == null ? 25 : input.getCount();
          final String query =
              input.getQuery() == null || input.getQuery().isEmpty()
                  ? DEFAULT_QUERY
                  : input.getQuery();
          final Filter filter = buildRootDataProductsFilter();

          try {
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.DATA_PRODUCT_ENTITY_NAME,
                    query,
                    filter,
                    Collections.singletonList(
                        new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            final GetRootDataProductsResult result = new GetRootDataProductsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setDataProducts(
                mapUnresolvedDataProducts(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve root data products from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildRootDataProductsFilter() {
    final CriterionArray array =
        new CriterionArray(
            ImmutableList.of(
                buildCriterion(HAS_PARENT_DATA_PRODUCT_FIELD_NAME, Condition.EQUAL, "false")));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }

  private List<DataProduct> mapUnresolvedDataProducts(final List<Urn> entityUrns) {
    final List<DataProduct> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final DataProduct stub = new DataProduct();
      stub.setUrn(urn.toString());
      stub.setType(EntityType.DATA_PRODUCT);
      results.add(stub);
    }
    return results;
  }
}
