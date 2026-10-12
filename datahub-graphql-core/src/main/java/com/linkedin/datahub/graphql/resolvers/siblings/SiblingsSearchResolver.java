package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.loaders.SiblingsSearchBatchLoader;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.util.SelectionSetUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dataloader.DataLoader;

/** Resolver that executes a searchAcrossEntities only on an entity's siblings */
@Slf4j
public class SiblingsSearchResolver implements DataFetcher<CompletableFuture<ScrollResults>> {

  private static final String SIBLINGS_FIELD_NAME = "siblings";
  private static final String MATCH_ALL_QUERY = "*";
  private static final String FACETS_FIELD_NAME = "facets";

  // Mirrors SearchUtils#DEFAULT_SCROLL_COUNT, which the unbatched path applies.
  private static final int DEFAULT_COUNT = 10;

  // Only dataset carries the siblings aspect (entity-registry.yml), so no other index can match
  // SIBLINGS_FIELD_NAME. Callers omit types, which would otherwise widen to every default search
  // index — one query per index, per entity this resolver runs on.
  private static final List<EntityType> SIBLING_CAPABLE_ENTITY_TYPES = List.of(EntityType.DATASET);

  private final EntityClient _entityClient;
  private final ViewService _viewService;

  // Null when constructed without feature flags (legacy/test path) — treated as "batch disabled".
  @Nullable private final FeatureFlags _featureFlags;

  /** Test-only: no feature flags means the batch path stays off. */
  SiblingsSearchResolver(final EntityClient entityClient, final ViewService viewService) {
    this(entityClient, viewService, null);
  }

  public SiblingsSearchResolver(
      final EntityClient entityClient,
      final ViewService viewService,
      @Nullable final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _viewService = viewService;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<ScrollResults> get(DataFetchingEnvironment environment) {
    final Entity entity = environment.getSource();
    final QueryContext context = environment.getContext();
    final ScrollAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), ScrollAcrossEntitiesInput.class);

    final List<EntityType> entityTypes = resolveEntityTypes(input);

    if (canBatch(input, environment)) {
      final DataLoader<SiblingsSearchBatchLoader.Key, ScrollResults> loader =
          environment.getDataLoaderRegistry().getDataLoader(SiblingsSearchBatchLoader.LOADER_NAME);
      return loader.load(toKey(context, entity, input, entityTypes));
    }

    final Criterion siblingsFilter =
        CriterionUtils.buildCriterion(SIBLINGS_FIELD_NAME, Condition.EQUAL, entity.getUrn());
    final Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(siblingsFilter))));
    final Filter inputFilter = ResolverUtils.buildFilter(null, input.getOrFilters());

    return SearchUtils.scrollAcrossEntities(
        context,
        _entityClient,
        _viewService,
        entityTypes,
        input.getQuery(),
        SearchUtils.combineFilters(inputFilter, baseFilter),
        input.getViewUrn(),
        input.getSearchFlags(),
        input.getCount(),
        input.getScrollId(),
        input.getKeepAlive(),
        List.of(),
        List.of(),
        this.getClass().getSimpleName());
  }

  /** Callers omit types; supply the scope rather than widening to every search index. */
  private static List<EntityType> resolveEntityTypes(final ScrollAcrossEntitiesInput input) {
    final List<EntityType> inputTypes = input.getTypes();
    return inputTypes == null || inputTypes.isEmpty() ? SIBLING_CAPABLE_ENTITY_TYPES : inputTypes;
  }

  /**
   * A scroll cursor is per-query state that a grouped search cannot produce, so any request that
   * carries one continues on the unbatched path.
   */
  private boolean canBatch(
      final ScrollAcrossEntitiesInput input, final DataFetchingEnvironment environment) {
    return _featureFlags != null
        && _featureFlags.isSiblingsSearchBatchLoadEnabled()
        && input.getScrollId() == null
        && !selectsFacets(environment);
  }

  /**
   * A batched chunk's aggregations describe every urn in the chunk, not one key, so they cannot be
   * attributed per dataset. Callers that ask for facets take the unbatched path, which aggregates
   * over that dataset's siblings alone.
   */
  private static boolean selectsFacets(final DataFetchingEnvironment environment) {
    return SelectionSetUtils.selectedSubFieldNames(environment).contains(FACETS_FIELD_NAME);
  }

  private static SiblingsSearchBatchLoader.Key toKey(
      final QueryContext context,
      final Entity entity,
      final ScrollAcrossEntitiesInput input,
      final List<EntityType> entityTypes) {
    // Mirrors SearchUtils#scrollAcrossEntities so a batched request resolves the same query as the
    // unbatched one.
    final String query =
        StringUtils.isNotBlank(input.getQuery())
            ? ResolverUtils.escapeForwardSlash(input.getQuery())
            : MATCH_ALL_QUERY;

    return new SiblingsSearchBatchLoader.Key(
        entity.getUrn(),
        SearchUtils.getSearchEntityNames(context.getOperationContext(), entityTypes),
        query,
        ResolverUtils.buildFilter(null, input.getOrFilters()),
        input.getSearchFlags() == null
            ? null
            : SearchFlagsInputMapper.map(context, input.getSearchFlags()),
        input.getViewUrn(),
        input.getCount() != null ? input.getCount() : DEFAULT_COUNT);
  }
}
