package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainEntitiesInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.loaders.DomainEntityCountsBatchLoader;
import com.linkedin.datahub.graphql.loaders.DomainEntityCountsBatchLoader.DomainCountKey;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;

/** Resolves the entities in a particular Domain. */
@Slf4j
public class DomainEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String DOMAINS_FIELD_NAME = "domains";
  private static final String INPUT_ARG_NAME = "input";
  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final DomainEntitiesInput DEFAULT_ENTITIES_INPUT = new DomainEntitiesInput();

  static {
    DEFAULT_ENTITIES_INPUT.setQuery(DEFAULT_QUERY);
    DEFAULT_ENTITIES_INPUT.setStart(DEFAULT_START);
    DEFAULT_ENTITIES_INPUT.setCount(DEFAULT_COUNT);
  }

  private final EntityClient _entityClient;

  public DomainEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((Domain) environment.getSource()).getUrn();

    final DomainEntitiesInput input =
        environment.getArgument(INPUT_ARG_NAME) != null
            ? bindArgument(environment.getArgument(INPUT_ARG_NAME), DomainEntitiesInput.class)
            : DEFAULT_ENTITIES_INPUT;

    final String query = input.getQuery() != null ? input.getQuery() : DEFAULT_QUERY;
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    // Fast path: the common count-only selections (Domain.entities/dataProducts/
    // applicationsInDomain in the domainEntitiesFields fragment) request no hits, no query, and at
    // most a single _entityType filter. Serve these from a batched, request-scoped aggregation so a
    // page of N domains costs a fixed number of searches instead of 3N. See
    // DomainEntityCountsBatchLoader.
    if (canServeFromCounts(query, count, input.getFilters())) {
      return resolveCountFromLoader(environment, urn, start, input.getFilters());
    }

    return resolveDirect(context, urn, input, query, start, count);
  }

  private static boolean canServeFromCounts(
      final String query, final int count, @Nullable final List<FacetFilterInput> filters) {
    // The batched loader forces query "*", so anything relying on a real query or hit-returning
    // pagination must take the direct path to preserve exact behavior.
    if (count != 0 || !DEFAULT_QUERY.equals(query)) {
      return false;
    }
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    // Only a single _entityType filter with an explicit value list and the default (EQUAL)
    // condition is derivable from the domains aggregation. Anything else — extra fields, multiple
    // criteria, a non-default condition, or absent values — falls back to the direct search, which
    // handles it exactly as before.
    if (filters.size() != 1) {
      return false;
    }
    final FacetFilterInput filter = filters.get(0);
    return INDEX_VIRTUAL_FIELD.equals(filter.getField())
        && filter.getCondition() == null
        && filter.getValues() != null
        && !filter.getValues().isEmpty();
  }

  private CompletableFuture<SearchResults> resolveCountFromLoader(
      final DataFetchingEnvironment environment,
      final String domainUrn,
      final int start,
      @Nullable final List<FacetFilterInput> filters) {
    final DataLoader<DomainCountKey, Long> loader =
        environment.getDataLoader(DomainEntityCountsBatchLoader.LOADER_NAME);
    return loader
        .load(toKey(domainUrn, filters))
        .thenApply(total -> toCountOnlyResults(total, start));
  }

  private static DomainCountKey toKey(
      final String domainUrn, @Nullable final List<FacetFilterInput> filters) {
    if (filters == null || filters.isEmpty()) {
      return new DomainCountKey(domainUrn);
    }
    // canServeFromCounts guarantees a single _entityType filter with non-null values here. Its
    // values are GraphQL EntityType enum names, applied directly to the _entityType search field
    // (no token mapping). The null-guard is defensive so this stays crash-safe if the gate changes.
    final FacetFilterInput entityTypeFilter = filters.get(0);
    final List<String> values =
        entityTypeFilter.getValues() != null
            ? entityTypeFilter.getValues()
            : Collections.emptyList();
    return new DomainCountKey(
        domainUrn, values, Boolean.TRUE.equals(entityTypeFilter.getNegated()));
  }

  private static SearchResults toCountOnlyResults(final Long total, final int start) {
    final SearchResults results = new SearchResults();
    results.setStart(start);
    results.setCount(0);
    results.setTotal(total != null ? total.intValue() : 0);
    results.setSearchResults(Collections.emptyList());
    return results;
  }

  /** The original, unbatched behavior: one search per invocation. */
  private CompletableFuture<SearchResults> resolveDirect(
      final QueryContext context,
      final String urn,
      final DomainEntitiesInput input,
      final String query,
      final int start,
      final int count) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final CriterionArray criteria = new CriterionArray();
            final Criterion filterCriterion =
                buildCriterion(DOMAINS_FIELD_NAME + ".keyword", Condition.EQUAL, urn);
            criteria.add(filterCriterion);
            if (input.getFilters() != null) {
              input
                  .getFilters()
                  .forEach(
                      filter -> {
                        criteria.add(criterionFromFilter(filter));
                      });
            }

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList()),
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
                String.format("Failed to resolve entities associated with Domain with urn %s", urn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
