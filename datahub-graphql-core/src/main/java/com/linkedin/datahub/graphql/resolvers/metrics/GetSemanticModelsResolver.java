package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetRootEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetSemanticModelsResult;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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

public class GetSemanticModelsResolver
    implements DataFetcher<CompletableFuture<GetSemanticModelsResult>> {

  private static final String DEFAULT_QUERY = "*";

  private final EntityClient _entityClient;

  public GetSemanticModelsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetSemanticModelsResult> get(final DataFetchingEnvironment environment)
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

          try {
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.SEMANTIC_MODEL_ENTITY_NAME,
                    query,
                    null,
                    Collections.singletonList(
                        new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            final GetSemanticModelsResult result = new GetSemanticModelsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setSemanticModels(
                mapUnresolvedSemanticModels(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve semantic models from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<SemanticModel> mapUnresolvedSemanticModels(final List<Urn> entityUrns) {
    final List<SemanticModel> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final SemanticModel stub = new SemanticModel();
      stub.setUrn(urn.toString());
      stub.setType(EntityType.SEMANTIC_MODEL);
      results.add(stub);
    }
    return results;
  }
}
