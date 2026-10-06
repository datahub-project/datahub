package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesResult;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.util.SelectionSetUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Lists all ingestion sources stored within DataHub. Requires the MANAGE_INGESTION privilege. */
@Slf4j
public class ListIngestionSourcesResolver
    implements DataFetcher<CompletableFuture<ListIngestionSourcesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";
  private static final String FACETS_FIELD_NAME = "facets";
  private static final List<String> FACET_FIELDS = List.of("type");

  private final EntityClient _entityClient;

  public ListIngestionSourcesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListIngestionSourcesResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (!IngestionAuthUtils.canManageIngestion(context)) {
      throw new AuthorizationException(
          "You are not authorized to list ingestion sources. Please contact your DataHub administrator.");
    }
    final ListIngestionSourcesInput input =
        bindArgument(environment.getArgument("input"), ListIngestionSourcesInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
    final List<FacetFilterInput> filters =
        input.getFilters() == null ? Collections.emptyList() : input.getFilters();

    // construct sort criteria, defaulting to systemCreated
    List<SortCriterion> sortCriteria = buildSortCriteria(input.getSort());

    // Only compute the facet aggregation when the caller actually selects it, to avoid an
    // unnecessary terms aggregation on every list/count call (e.g. getNoOfIngestionSources).
    final List<String> facetFields =
        SelectionSetUtils.selectedSubFieldNames(environment).contains(FACETS_FIELD_NAME)
            ? FACET_FIELDS
            : Collections.emptyList();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // First, get all ingestion sources Urns.
            final SearchResult gmsResult =
                _entityClient.searchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    List.of(Constants.INGESTION_SOURCE_ENTITY_NAME),
                    query,
                    buildFilter(filters, Collections.emptyList()),
                    start,
                    count,
                    sortCriteria,
                    facetFields);

            // Now that we have entities we can bind this to a result.
            final ListIngestionSourcesResult result = new ListIngestionSourcesResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setIngestionSources(mapUnresolvedIngestionSources(gmsResult.getEntities()));
            if (gmsResult.getMetadata() != null
                && gmsResult.getMetadata().getAggregations() != null) {
              result.setFacets(
                  gmsResult.getMetadata().getAggregations().stream()
                      .map(facet -> MapperUtils.mapFacet(context, facet))
                      .collect(Collectors.toList()));
            } else {
              result.setFacets(Collections.emptyList());
            }
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list ingestion sources", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  // This method maps urns returned from the list endpoint into Partial Ingestion source objects
  // which will be
  // resolved be a separate Batch resolver.
  private List<IngestionSource> mapUnresolvedIngestionSources(final SearchEntityArray entityArray) {
    final List<IngestionSource> results = new ArrayList<>();
    for (final SearchEntity entity : entityArray) {
      final Urn urn = entity.getEntity();
      final IngestionSource unresolvedTest = new IngestionSource();
      unresolvedTest.setUrn(urn.toString());
      results.add(unresolvedTest);
    }
    return results;
  }

  List<SortCriterion> buildSortCriteria(
      com.linkedin.datahub.graphql.generated.SortCriterion sortCriterionInput) {
    if (sortCriterionInput == null) {
      return List.of();
    }

    SortOrder order = SortOrder.valueOf(sortCriterionInput.getSortOrder().name());
    return List.of(new SortCriterion().setField(sortCriterionInput.getField()).setOrder(order));
  }
}
