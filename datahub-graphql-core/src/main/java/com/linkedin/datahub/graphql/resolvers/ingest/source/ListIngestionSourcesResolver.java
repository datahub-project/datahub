package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesResult;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Lists all ingestion sources stored within DataHub. Requires the MANAGE_INGESTION privilege. */
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

      return CompletableFuture.supplyAsync(
          () -> {
            try {
              // First, get all ingestion sources Urns.
              final SearchResult gmsResult =
                  _entityClient.search(
                      Constants.INGESTION_SOURCE_ENTITY_NAME,
                      query,
                      buildFilter(filters, Collections.emptyList()),
                      null,
                      start,
                      count,
                      context.getAuthentication(),
                      new SearchFlags().setFulltext(true));

              // Then, resolve all ingestion sources
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      Constants.INGESTION_SOURCE_ENTITY_NAME,
                      new HashSet<>(
                          gmsResult.getEntities().stream()
                              .map(SearchEntity::getEntity)
                              .collect(Collectors.toList())),
                      ImmutableSet.of(
                          Constants.INGESTION_INFO_ASPECT_NAME,
                          Constants.INGESTION_SOURCE_KEY_ASPECT_NAME),
                      context.getAuthentication());

              final Collection<EntityResponse> sortedEntities =
                  entities.values().stream()
                      .sorted(
                          Comparator.comparingLong(
                              s ->
                                  -s.getAspects()
                                      .get(Constants.INGESTION_SOURCE_KEY_ASPECT_NAME)
                                      .getCreated()
                                      .getTime()))
                      .collect(Collectors.toList());

              // Now that we have entities we can bind this to a result.
              final ListIngestionSourcesResult result = new ListIngestionSourcesResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setIngestionSources(
                  IngestionResolverUtils.mapIngestionSources(sortedEntities));
              return result;

            } catch (Exception e) {
              throw new RuntimeException("Failed to list ingestion sources", e);
            }
          });
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
