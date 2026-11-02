package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.ContainerEntitiesInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.loaders.ContainerEntityCountsBatchLoader;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.util.SelectionSetUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;

/** Retrieves a list of historical executions for a particular source. */
@Slf4j
public class ContainerEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  public static final List<String> CONTAINABLE_ENTITY_NAMES =
      ImmutableList.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME);
  private static final String CONTAINER_FIELD_NAME = "container";
  private static final String INPUT_ARG_NAME = "input";
  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final ContainerEntitiesInput DEFAULT_ENTITIES_INPUT = new ContainerEntitiesInput();

  // Selections answerable from an aggregation alone. Deliberately excludes start/count: those are
  // echoed from the request rather than reported by the search layer on this path, so we only take
  // it when the caller cannot observe the difference.
  private static final Set<String> COUNT_ONLY_FIELDS = ImmutableSet.of("total", "__typename");

  static {
    DEFAULT_ENTITIES_INPUT.setQuery(DEFAULT_QUERY);
    DEFAULT_ENTITIES_INPUT.setStart(DEFAULT_START);
    DEFAULT_ENTITIES_INPUT.setCount(DEFAULT_COUNT);
  }

  private final EntityClient _entityClient;

  public ContainerEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((Container) environment.getSource()).getUrn();

    final ContainerEntitiesInput input =
        environment.getArgument(INPUT_ARG_NAME) != null
            ? bindArgument(environment.getArgument(INPUT_ARG_NAME), ContainerEntitiesInput.class)
            : DEFAULT_ENTITIES_INPUT;

    final String query = input.getQuery() != null ? input.getQuery() : DEFAULT_QUERY;
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    // Fast path: every UI call site selects only `total`, so the hits the search would return are
    // discarded. Serve those from a batched, request-scoped aggregation instead, so a page of N
    // containers costs a fixed number of searches rather than N searches plus N primary-store
    // existence checks. See ContainerEntityCountsBatchLoader.
    if (canServeFromCounts(environment, query, input.getFilters())) {
      return resolveCountFromLoader(environment, urn, start, count);
    }

    return resolveDirect(context, urn, input, query, start, count);
  }

  private static boolean canServeFromCounts(
      final DataFetchingEnvironment environment,
      final String query,
      @Nullable final List<FacetFilterInput> filters) {
    // The batched loader forces query "*" and applies no facet filters, so anything relying on a
    // real query or filters must take the direct path to preserve exact behavior. `count` is not
    // consulted: the gate below already establishes that no hits are read, and `total` is
    // independent of paging.
    if (!DEFAULT_QUERY.equals(query) || (filters != null && !filters.isEmpty())) {
      return false;
    }
    return isCountOnlySelection(environment);
  }

  /**
   * True when the caller reads nothing but the total.
   *
   * <p>Read from the query AST rather than {@code environment.getSelectionSet()}, which would
   * normalize the whole operation and can trip graphql-java's 100k field cap on the production
   * search query — see {@link SelectionSetUtils}. The check over-approximates what is selected,
   * which errs toward the direct path; that is the safe direction, since under-approximating would
   * serve counts to a caller that asked for hits.
   */
  private static boolean isCountOnlySelection(final DataFetchingEnvironment environment) {
    return SelectionSetUtils.selectsOnly(environment, COUNT_ONLY_FIELDS);
  }

  private CompletableFuture<SearchResults> resolveCountFromLoader(
      final DataFetchingEnvironment environment,
      final String containerUrn,
      final int start,
      final int count) {
    final DataLoader<String, Long> loader =
        environment.getDataLoader(ContainerEntityCountsBatchLoader.LOADER_NAME);
    return loader.load(containerUrn).thenApply(total -> toCountOnlyResults(total, start, count));
  }

  private static SearchResults toCountOnlyResults(
      final Long total, final int start, final int count) {
    final SearchResults results = new SearchResults();
    results.setStart(start);
    results.setCount(count);
    results.setTotal(total != null ? total.intValue() : 0);
    results.setSearchResults(Collections.emptyList());
    return results;
  }

  /** The original, unbatched behavior: one search per invocation. */
  private CompletableFuture<SearchResults> resolveDirect(
      final QueryContext context,
      final String urn,
      final ContainerEntitiesInput input,
      final String query,
      final int start,
      final int count) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            final CriterionArray criteria = new CriterionArray();
            final Criterion filterCriterion =
                buildCriterion(CONTAINER_FIELD_NAME + ".keyword", Condition.EQUAL, urn);
            criteria.add(filterCriterion);
            if (input.getFilters() != null) {
              input.getFilters().forEach(filter -> criteria.add(criterionFromFilter(filter)));
            }

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    CONTAINABLE_ENTITY_NAMES,
                    query,
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion().setAnd(criteria))),
                    start,
                    count,
                    Collections.emptyList()));

          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to resolve entities associated with container with urn %s", urn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
