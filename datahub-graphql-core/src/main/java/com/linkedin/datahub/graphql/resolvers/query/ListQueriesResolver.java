package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListQueriesInput;
import com.linkedin.datahub.graphql.generated.ListQueriesResult;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListQueriesResolver implements DataFetcher<CompletableFuture<ListQueriesResult>> {

  // Visible for Testing
  static final Integer DEFAULT_START = 0;
  static final Integer DEFAULT_COUNT = 100;
  static final String DEFAULT_QUERY = "";
  static final String CREATED_AT_FIELD = "createdAt";
  static final String QUERY_SOURCE_FIELD = "source";
  static final String QUERY_ENTITIES_FIELD = "entities";
  static final String URN_SORT_FIELD = "urn";

  /** Batch size for each page scanned while authorizing the requested page. */
  static final int AUTHORIZATION_SCROLL_BATCH_SIZE = 100;

  static final String AUTHORIZATION_SCROLL_KEEP_ALIVE = "1m";

  /**
   * Upper bound on raw search candidates a single request will fetch while overfetching past the
   * requested page to authorize it exactly. Protects an unscoped call (no dataset/source filter)
   * from scrolling the entire Query index one batch at a time. Real callers today always scope
   * this call to one dataset, so this should never trigger in practice; if it does, the request is
   * rejected outright rather than returning a partial page or an inexact total.
   */
  static final int MAX_QUERY_OVERFETCH_CANDIDATES = 10_000;

  /**
   * Wall-clock budget for that same overfetch, independent of the candidate cap above: bounds
   * against slow individual batches (a degraded search cluster, an unusually large policy set)
   * that a fixed candidate count can't anticipate. Comfortably under
   * {@code DATAHUB_GMS_ASYNC_REQUEST_TIMEOUT_MS} (55s default) so this fails with a clear,
   * specific error before the servlet's own request timeout would.
   */
  static final long MAX_QUERY_OVERFETCH_MILLIS = 30_000L;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListQueriesResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListQueriesInput input =
        bindArgument(environment.getArgument("input"), ListQueriesInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
    final Filter inputFilter =
        input.getOrFilters() != null
            ? buildFilter(Collections.emptyList(), input.getOrFilters())
            : null;
    final Filter finalFilter =
        inputFilter != null
            ? SearchUtils.combineFilters(inputFilter, buildFilters(input))
            : buildFilters(input);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<SortCriterion> sortCriteria =
                input.getSortInput() != null
                    ? Collections.singletonList(
                        mapSortCriterion(input.getSortInput().getSortCriterion()))
                    : Collections.singletonList(
                        new SortCriterion()
                            .setField(CREATED_AT_FIELD)
                            .setOrder(SortOrder.DESCENDING));

            // Active when query-read authorization is enabled (dedicated flag or the legacy
            // view-authorization switch); disabled means no subject lookups at all, and the raw
            // search page can be returned as-is.
            final boolean authorizationActive =
                EntityAspectAuthorizationUtils.isQueryViewAuthorizationEnabled(
                        context.getOperationContext())
                    && !context.getOperationContext().isSystemAuth();

            if (!authorizationActive) {
              return searchUnauthorized(context, query, finalFilter, sortCriteria, start, count);
            }
            return searchAuthorized(context, query, finalFilter, sortCriteria, start, count);
          } catch (Exception e) {
            throw new RuntimeException("Failed to list Queries", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private ListQueriesResult searchUnauthorized(
      QueryContext context,
      String query,
      Filter finalFilter,
      List<SortCriterion> sortCriteria,
      Integer start,
      Integer count)
      throws Exception {
    final SearchResult gmsResult =
        _entityClient.search(
            context
                .getOperationContext()
                .withSearchFlags(flags -> flags.setFulltext(true).setSkipHighlighting(true)),
            QUERY_ENTITY_NAME,
            query,
            finalFilter,
            sortCriteria,
            start,
            count);

    final List<Urn> queryUrns =
        gmsResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());

    final ListQueriesResult result = new ListQueriesResult();
    result.setStart(gmsResult.getFrom());
    result.setCount(queryUrns.size());
    result.setTotal(gmsResult.getNumEntities());
    result.setQueries(mapUnresolvedQueries(queryUrns));
    return result;
  }

  /**
   * Applies the requested query/filters/sort to obtain one stable ordered stream via {@link
   * EntityClient#scrollAcrossEntities}, authorizes it batch by batch in that same order, and only
   * then applies start/count to the authorized stream — so start, count, and total all describe
   * the actor's authorized view rather than the raw search result. Pagination has to happen after
   * authorization, not before: filtering an already-paginated raw page (as a single search call
   * would) leaks the existence/count of denied queries through a shifting total and can strand
   * authorized queries behind a raw-page boundary.
   */
  private ListQueriesResult searchAuthorized(
      QueryContext context,
      String query,
      Filter finalFilter,
      List<SortCriterion> sortCriteria,
      Integer start,
      Integer count)
      throws Exception {
    List<SortCriterion> stableSort = new ArrayList<>(sortCriteria);
    stableSort.add(new SortCriterion().setField(URN_SORT_FIELD).setOrder(SortOrder.ASCENDING));

    AuthorizedQueryPage page =
        scanAuthorizedPage(context, query, finalFilter, stableSort, start, count);

    final ListQueriesResult result = new ListQueriesResult();
    result.setStart(start);
    result.setCount(page.urns().size());
    result.setTotal(page.total());
    result.setQueries(mapUnresolvedQueries(page.urns()));
    return result;
  }

  private AuthorizedQueryPage scanAuthorizedPage(
      QueryContext context,
      String query,
      Filter finalFilter,
      List<SortCriterion> stableSort,
      int start,
      int count)
      throws Exception {
    final long deadline = System.currentTimeMillis() + MAX_QUERY_OVERFETCH_MILLIS;
    int authorizedSeen = 0;
    int candidatesScanned = 0;
    List<Urn> page = new ArrayList<>();
    String scrollId = null;

    do {
      if (System.currentTimeMillis() > deadline) {
        log.warn(
            "listQueries overfetch timed out ({} candidates scanned) for actor {}; rejecting"
                + " request. Narrow the query with a dataset or source filter.",
            candidatesScanned,
            context.getOperationContext().getActorContext().getActorUrn());
        throw new DataHubGraphQLException(
            "Timed out authorizing listQueries results; narrow the query with a dataset or"
                + " source filter and try again.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      final ScrollResult scrollResult =
          _entityClient.scrollAcrossEntities(
              context
                  .getOperationContext()
                  .withSearchFlags(flags -> flags.setFulltext(true).setSkipHighlighting(true)),
              ImmutableList.of(QUERY_ENTITY_NAME),
              query,
              finalFilter,
              scrollId,
              AUTHORIZATION_SCROLL_KEEP_ALIVE,
              stableSort,
              AUTHORIZATION_SCROLL_BATCH_SIZE);

      final List<Urn> orderedUrns =
          scrollResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList());

      if (orderedUrns.isEmpty()) {
        break;
      }

      candidatesScanned += orderedUrns.size();
      if (candidatesScanned > MAX_QUERY_OVERFETCH_CANDIDATES) {
        log.warn(
            "listQueries overfetch cap reached ({} candidates scanned) for actor {}; rejecting"
                + " request. Narrow the query with a dataset or source filter.",
            candidatesScanned,
            context.getOperationContext().getActorContext().getActorUrn());
        throw new DataHubGraphQLException(
            "listQueries matched too many candidates to authorize exactly; narrow the query with"
                + " a dataset or source filter and try again.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      final Set<Urn> allowed =
          EntityAspectAuthorizationUtils.filterViewableQueryEntities(
              context.getOperationContext(),
              context.getOperationContext(),
              context.getOperationContext().getAspectRetriever(),
              orderedUrns,
              EntityAspectAuthorizationUtils.requireAllQuerySubjects(
                  context.getOperationContext()));

      for (Urn urn : orderedUrns) {
        if (!allowed.contains(urn)) {
          continue;
        }
        if (authorizedSeen >= start && page.size() < count) {
          page.add(urn);
        }
        authorizedSeen++;
      }

      scrollId = scrollResult.getScrollId();
    } while (scrollId != null);

    return new AuthorizedQueryPage(page, authorizedSeen);
  }

  /** One page of authorized Query urns plus the exact authorized total across the full scan. */
  private record AuthorizedQueryPage(List<Urn> urns, int total) {}

  // This method maps urns returned from the list endpoint into Partial Query objects which will be
  // resolved be a separate Batch resolver.
  private List<QueryEntity> mapUnresolvedQueries(final List<Urn> queryUrns) {
    final List<QueryEntity> results = new ArrayList<>();
    for (final Urn urn : queryUrns) {
      final QueryEntity unresolvedQuery = new QueryEntity();
      unresolvedQuery.setUrn(urn.toString());
      unresolvedQuery.setType(EntityType.QUERY);
      results.add(unresolvedQuery);
    }
    return results;
  }

  @Nullable
  private Filter buildFilters(@Nonnull final ListQueriesInput input) {
    final AndFilterInput criteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();

    // Optionally add a source filter.
    if (input.getSource() != null) {
      andConditions.add(
          new FacetFilterInput(
              QUERY_SOURCE_FIELD,
              ImmutableList.of(input.getSource().toString()),
              false,
              FilterOperator.EQUAL));
    }

    // Optionally add an entity type filter.
    if (input.getDatasetUrn() != null) {
      andConditions.add(
          new FacetFilterInput(
              QUERY_ENTITIES_FIELD,
              ImmutableList.of(input.getDatasetUrn()),
              false,
              FilterOperator.EQUAL));
    }

    criteria.setAnd(andConditions);
    return buildFilter(Collections.emptyList(), ImmutableList.of(criteria));
  }
}
