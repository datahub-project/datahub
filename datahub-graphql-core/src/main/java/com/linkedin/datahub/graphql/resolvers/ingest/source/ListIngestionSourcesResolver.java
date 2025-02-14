package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesResult;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Lists all ingestion sources stored within DataHub. Requires the MANAGE_INGESTION privilege. */
@Slf4j
public class ListIngestionSourcesResolver
    implements DataFetcher<CompletableFuture<ListIngestionSourcesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListIngestionSourcesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListIngestionSourcesResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {
      final ListIngestionSourcesInput input =
          bindArgument(environment.getArgument("input"), ListIngestionSourcesInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
      final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
      final List<FacetFilterInput> filters =
          input.getFilters() == null ? Collections.emptyList() : input.getFilters();

      // construct sort criteria, defaulting to systemCreated
      final SortCriterion sortCriterion;

      // if query is expecting to sort by something, use that
      final com.linkedin.datahub.graphql.generated.SortCriterion sortCriterionInput =
          input.getSort();
      if (sortCriterionInput != null) {
        sortCriterion =
            new SortCriterion()
                .setField(sortCriterionInput.getField())
                .setOrder(SortOrder.valueOf(sortCriterionInput.getSortOrder().name()));
      } else {
        // TODO: default to last executed
        sortCriterion = null;
      }

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // First, get all ingestion sources Urns.
              final SearchResult gmsResult =
                  _entityClient.search(
                      context
                          .getOperationContext()
                          .withSearchFlags(flags -> flags.setFulltext(true)),
                      Constants.INGESTION_SOURCE_ENTITY_NAME,
                      query,
                      buildFilter(filters, Collections.emptyList()),
                      sortCriterion != null ? List.of(sortCriterion) : null,
                      start,
                      count);

              final List<Urn> entitiesUrnList =
                  gmsResult.getEntities().stream().map(SearchEntity::getEntity).toList();
              // Then, resolve all ingestion sources
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      Constants.INGESTION_SOURCE_ENTITY_NAME,
                      new HashSet<>(entitiesUrnList),
                      ImmutableSet.of(
                          Constants.INGESTION_INFO_ASPECT_NAME,
                          Constants.INGESTION_SOURCE_KEY_ASPECT_NAME));

              final List<EntityResponse> entitiesOrdered =
                  entitiesUrnList.stream().map(entities::get).filter(Objects::nonNull).toList();

              // Now that we have entities we can bind this to a result.
              final ListIngestionSourcesResult result = new ListIngestionSourcesResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setIngestionSources(
                  IngestionResolverUtils.mapIngestionSources(entitiesOrdered));
              return result;

            } catch (Exception e) {
              throw new RuntimeException("Failed to list ingestion sources", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
