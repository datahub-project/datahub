package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesResult;
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

/**
 * Lists ingestion sources stored within DataHub.
 *
 * <p>Listing all sources requires the MANAGE_INGESTION privilege. When the {@code
 * viewIngestionSourcePrivilegesEnabled} feature flag is on, actors without it may list the sources
 * they are authorized to read, for example through a policy granting View Entity Page on specific
 * ingestion sources; every other source is left out of the result.
 */
@Slf4j
public class ListIngestionSourcesResolver
    implements DataFetcher<CompletableFuture<ListIngestionSourcesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";
  private static final String FACETS_FIELD_NAME = "facets";
  private static final List<String> FACET_FIELDS = List.of("type");

  /**
   * Read authorization is evaluated per source URN, so the page for an actor without
   * MANAGE_INGESTION is assembled in memory from a bounded scan of the matching sources rather than
   * from the search engine's own pagination.
   */
  static final int AUTHORIZED_SOURCES_SCAN_LIMIT = 1000;

  private final EntityClient _entityClient;
  private final FeatureFlags _featureFlags;

  public ListIngestionSourcesResolver(
      final EntityClient entityClient, final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<ListIngestionSourcesResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    final boolean canManageIngestion = AuthorizationUtils.canManageIngestion(context);
    if (!canManageIngestion && !_featureFlags.isViewIngestionSourcePrivilegesEnabled()) {
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
            if (canManageIngestion) {
              return listAllSources(
                  context, query, filters, start, count, sortCriteria, facetFields);
            }
            return listAuthorizedSources(context, query, filters, start, count, sortCriteria);
          } catch (Exception e) {
            throw new RuntimeException("Failed to list ingestion sources", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private ListIngestionSourcesResult listAllSources(
      final QueryContext context,
      final String query,
      final List<FacetFilterInput> filters,
      final int start,
      final int count,
      final List<SortCriterion> sortCriteria,
      final List<String> facetFields)
      throws Exception {
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
    if (gmsResult.getMetadata() != null && gmsResult.getMetadata().getAggregations() != null) {
      result.setFacets(
          gmsResult.getMetadata().getAggregations().stream()
              .map(facet -> MapperUtils.mapFacet(context, facet))
              .collect(Collectors.toList()));
    } else {
      result.setFacets(Collections.emptyList());
    }
    return result;
  }

  /**
   * Lists only the sources the actor is authorized to read. Facets are not returned: aggregations
   * are computed by the search engine over every matching source and would reveal sources the actor
   * cannot see.
   */
  private ListIngestionSourcesResult listAuthorizedSources(
      final QueryContext context,
      final String query,
      final List<FacetFilterInput> filters,
      final int start,
      final int count,
      final List<SortCriterion> sortCriteria)
      throws Exception {
    final SearchResult gmsResult =
        _entityClient.searchAcrossEntities(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(Constants.INGESTION_SOURCE_ENTITY_NAME),
            query,
            buildFilter(filters, Collections.emptyList()),
            0,
            AUTHORIZED_SOURCES_SCAN_LIMIT,
            sortCriteria,
            Collections.emptyList());
    if (gmsResult.getNumEntities() > AUTHORIZED_SOURCES_SCAN_LIMIT) {
      log.warn(
          "Actor {} listed ingestion sources without MANAGE_INGESTION; only the first {} of {} matching sources were checked for read authorization",
          context.getActorUrn(),
          AUTHORIZED_SOURCES_SCAN_LIMIT,
          gmsResult.getNumEntities());
    }

    final List<SearchEntity> authorizedEntities =
        gmsResult.getEntities().stream()
            .filter(
                entity -> AuthUtil.canViewEntity(context.getOperationContext(), entity.getEntity()))
            .collect(Collectors.toList());

    final int from = Math.min(start, authorizedEntities.size());
    final int to = Math.min(start + count, authorizedEntities.size());

    final ListIngestionSourcesResult result = new ListIngestionSourcesResult();
    result.setStart(start);
    result.setCount(count);
    result.setTotal(authorizedEntities.size());
    result.setIngestionSources(
        mapUnresolvedIngestionSources(new SearchEntityArray(authorizedEntities.subList(from, to))));
    result.setFacets(Collections.emptyList());
    return result;
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
