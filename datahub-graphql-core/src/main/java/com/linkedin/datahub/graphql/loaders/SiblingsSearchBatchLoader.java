package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader for {@code Dataset.siblingsSearch}, collapsing the previous one search per
 * dataset row into a fixed number of searches per request.
 *
 * <p>The field asks the reverse question to the {@code siblings} aspect — "which entities index me
 * as their sibling" — so it cannot be served from the aspect, which only records the forward
 * direction. Siblings written through {@code SiblingsPatchBuilder} are not guaranteed symmetric.
 *
 * <p>Unlike the other GMS batch loaders the key is not a bare urn: {@code siblingsSearch} takes an
 * {@code input} argument, so two rows in one request can ask different questions about the same
 * urn. {@link Key} therefore carries the query shape alongside the urn. The GraphQL-generated input
 * classes have identity equality only, so the key holds the Pegasus equivalents ({@link Filter},
 * {@link SearchFlags}), which compare by value.
 *
 * <p>Each chunk issues one search filtered to the chunk's urns and faceted on {@code siblings}. The
 * facet gives an exact per-urn total; the hits are attributed back to the urn that matched them by
 * reading their own {@code siblings} aspect, which the search response does not carry.
 *
 * <p><b>Window caveat.</b> One urn with many siblings can crowd others out of the shared hit
 * window. Any key that ends up with fewer hits than its total warrants is re-queried on its own, so
 * a crowded window costs extra searches rather than returning short results.
 */
@Slf4j
public final class SiblingsSearchBatchLoader {

  public static final String LOADER_NAME = "SiblingsSearch";

  private static final String SIBLINGS_FIELD = "siblings";
  private static final String MATCH_ALL_QUERY = "*";

  // Keep chunks under the search-layer terms-bucket cap (min(maxAggValues, maxTermBucketSize),
  // maxTermBucketSize defaults to 60). Unlike the container equivalent an entity may list several
  // siblings, so a matched document can contribute buckets outside the chunk — hence the wide
  // margin rather than sitting near the ceiling.
  private static final int MAX_URNS_PER_AGG = 25;
  private static final int MAX_AGG_VALUES = 1000;

  // Hits are shared across the chunk, so ask for more than the per-key count to reduce how often
  // the per-key fallback has to run.
  private static final int WINDOW_HEADROOM_FACTOR = 3;
  private static final int MAX_WINDOW = 500;

  private SiblingsSearchBatchLoader() {}

  /**
   * Identifies one {@code siblingsSearch} call. Everything except {@link #urn} describes the query
   * shape and is what keys sharing a batch must agree on.
   */
  public static final class Key {
    private final String urn;
    private final List<String> entityNames;
    private final String query;
    @Nullable private final Filter inputFilter;
    @Nullable private final SearchFlags searchFlags;
    @Nullable private final String viewUrn;
    private final int count;

    public Key(
        final String urn,
        final List<String> entityNames,
        final String query,
        @Nullable final Filter inputFilter,
        @Nullable final SearchFlags searchFlags,
        @Nullable final String viewUrn,
        final int count) {
      this.urn = urn;
      this.entityNames = List.copyOf(entityNames);
      this.query = query;
      this.inputFilter = inputFilter;
      this.searchFlags = searchFlags;
      this.viewUrn = viewUrn;
      this.count = count;
    }

    public String getUrn() {
      return urn;
    }

    private GroupKey groupKey() {
      return new GroupKey(entityNames, query, inputFilter, searchFlags, viewUrn, count);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      final Key other = (Key) o;
      return urn.equals(other.urn) && groupKey().equals(other.groupKey());
    }

    @Override
    public int hashCode() {
      return Objects.hash(urn, groupKey());
    }
  }

  /** The query shape shared by every key in one batched search. */
  private static final class GroupKey {
    private final List<String> entityNames;
    private final String query;
    @Nullable private final Filter inputFilter;
    @Nullable private final SearchFlags searchFlags;
    @Nullable private final String viewUrn;
    private final int count;

    private GroupKey(
        final List<String> entityNames,
        final String query,
        @Nullable final Filter inputFilter,
        @Nullable final SearchFlags searchFlags,
        @Nullable final String viewUrn,
        final int count) {
      this.entityNames = entityNames;
      this.query = query;
      this.inputFilter = inputFilter;
      this.searchFlags = searchFlags;
      this.viewUrn = viewUrn;
      this.count = count;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof GroupKey)) {
        return false;
      }
      final GroupKey other = (GroupKey) o;
      return count == other.count
          && entityNames.equals(other.entityNames)
          && query.equals(other.query)
          && Objects.equals(inputFilter, other.inputFilter)
          && Objects.equals(searchFlags, other.searchFlags)
          && Objects.equals(viewUrn, other.viewUrn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entityNames, query, inputFilter, searchFlags, viewUrn, count);
    }
  }

  public static DataLoader<Key, ScrollResults> create(
      final EntityClient entityClient,
      final ViewService viewService,
      final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Parent the batchLoad span under the operation, not the executor thread (see
    // GmsGraphQLEngine#createDataLoader).
    final Context batchContext = Context.current();

    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try (Scope ignored = batchContext.makeCurrent()) {
                    return batchLoad(
                        keys, (QueryContext) env.getContext(), entityClient, viewService);
                  }
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public static List<ScrollResults> batchLoad(
      final List<Key> keys,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final ViewService viewService) {

    final Map<Key, ScrollResults> resultByKey = new HashMap<>(keys.size());

    final Map<GroupKey, List<Key>> byGroup = new LinkedHashMap<>();
    for (Key key : keys) {
      byGroup.computeIfAbsent(key.groupKey(), g -> new ArrayList<>()).add(key);
    }

    for (Map.Entry<GroupKey, List<Key>> group : byGroup.entrySet()) {
      final List<Key> distinctKeys =
          group.getValue().stream().distinct().collect(Collectors.toList());
      for (List<Key> chunk : partition(distinctKeys, MAX_URNS_PER_AGG)) {
        try {
          resultByKey.putAll(
              loadChunk(group.getKey(), chunk, queryContext, entityClient, viewService));
        } catch (Exception e) {
          // Surface rather than swallow: an empty ScrollResults is indistinguishable from "this
          // dataset has no siblings" and would silently stop the UI merging sibling rows. Matches
          // the throw-on-failure contract of the unbatched resolver path.
          throw new RuntimeException(
              String.format("Failed to resolve siblings search for %d entities", chunk.size()), e);
        }
      }
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    final List<ScrollResults> ordered = new ArrayList<>(keys.size());
    for (Key key : keys) {
      ordered.add(resultByKey.getOrDefault(key, emptyResults()));
    }
    return ordered;
  }

  private static Map<Key, ScrollResults> loadChunk(
      final GroupKey group,
      final List<Key> chunk,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final ViewService viewService)
      throws Exception {

    final ResolvedQuery resolved = resolveQuery(group, queryContext, viewService);
    if (resolved.entityNames.isEmpty()) {
      return chunk.stream().collect(Collectors.toMap(k -> k, k -> emptyResults()));
    }

    final List<String> urns = chunk.stream().map(Key::getUrn).collect(Collectors.toList());
    final int window = Math.min(MAX_WINDOW, urns.size() * group.count * WINDOW_HEADROOM_FACTOR);

    final OperationContext opContext =
        queryContext
            .getOperationContext()
            .withSearchFlags(
                flags -> {
                  // `flags` arrives as a copy (SearchContext#withFlagDefaults), but the group's
                  // flags are part of the DataLoader key — mutating them would corrupt its hash.
                  final SearchFlags base =
                      group.searchFlags != null ? copyFlags(group.searchFlags) : flags;
                  return base.setMaxAggValues(MAX_AGG_VALUES);
                });

    final SearchResult searchResult =
        entityClient.searchAcrossEntities(
            opContext,
            resolved.entityNames,
            group.query,
            combineWithSiblingsFilter(resolved.baseFilter, urns),
            0,
            window,
            Collections.emptyList(),
            Collections.singletonList(SIBLINGS_FIELD));

    final Map<String, Long> totalsByUrn = extractTotals(searchResult, urns);
    final Map<String, List<SearchEntity>> hitsByUrn =
        attributeHits(searchResult, urns, opContext, entityClient);

    final Map<Key, ScrollResults> results = new HashMap<>(chunk.size());
    for (Key key : chunk) {
      final long total = totalsByUrn.getOrDefault(key.urn, 0L);
      final List<SearchEntity> hits =
          hitsByUrn.getOrDefault(key.urn, Collections.emptyList()).stream()
              .limit(group.count)
              .collect(Collectors.toList());

      // The window is shared, so a urn with many siblings can starve its neighbours. Re-query any
      // key that came back with fewer hits than its own total warrants.
      if (hits.size() < Math.min(group.count, total)) {
        results.put(key, loadSingle(key, group, resolved, queryContext, entityClient));
        continue;
      }
      results.put(key, toScrollResults(queryContext, hits, total));
    }
    return results;
  }

  private static ScrollResults loadSingle(
      final Key key,
      final GroupKey group,
      final ResolvedQuery resolved,
      final QueryContext queryContext,
      final EntityClient entityClient)
      throws Exception {

    final OperationContext opContext =
        group.searchFlags != null
            ? queryContext
                .getOperationContext()
                .withSearchFlags(flags -> copyFlags(group.searchFlags))
            : queryContext.getOperationContext();

    final SearchResult result =
        entityClient.searchAcrossEntities(
            opContext,
            resolved.entityNames,
            group.query,
            combineWithSiblingsFilter(resolved.baseFilter, List.of(key.urn)),
            0,
            group.count,
            Collections.emptyList(),
            Collections.emptyList());

    return toScrollResults(
        queryContext, result.getEntities(), result.hasNumEntities() ? result.getNumEntities() : 0);
  }

  /**
   * Maps each hit back to the chunk urns it is a sibling of. The search response carries only urns,
   * so the edge direction has to be read from the hits' own {@code siblings} aspect.
   */
  private static Map<String, List<SearchEntity>> attributeHits(
      @Nullable final SearchResult searchResult,
      final List<String> chunkUrns,
      final OperationContext opContext,
      final EntityClient entityClient)
      throws Exception {

    final Map<String, List<SearchEntity>> hitsByUrn = new HashMap<>();
    if (searchResult == null || searchResult.getEntities().isEmpty()) {
      return hitsByUrn;
    }

    final Map<Urn, SearchEntity> hitByUrn = new LinkedHashMap<>();
    for (SearchEntity hit : searchResult.getEntities()) {
      hitByUrn.put(hit.getEntity(), hit);
    }

    final Set<String> chunkUrnSet = new HashSet<>(chunkUrns);

    // batchGetV2 is per entity type; siblings are datasets today but group defensively.
    final Map<String, Set<Urn>> byEntityType = new LinkedHashMap<>();
    for (Urn urn : hitByUrn.keySet()) {
      byEntityType.computeIfAbsent(urn.getEntityType(), t -> new LinkedHashSet<>()).add(urn);
    }

    for (Map.Entry<String, Set<Urn>> entry : byEntityType.entrySet()) {
      // alwaysIncludeKeyAspect=false: only the siblings edge is read, so the key aspect is dead
      // weight on the response.
      final Map<Urn, EntityResponse> responses =
          entityClient.batchGetV2(
              opContext, entry.getKey(), entry.getValue(), Set.of(SIBLINGS_ASPECT_NAME), false);

      for (Map.Entry<Urn, EntityResponse> response : responses.entrySet()) {
        final SearchEntity hit = hitByUrn.get(response.getKey());
        if (hit == null) {
          continue;
        }
        for (String siblingUrn : readSiblingUrns(response.getValue())) {
          if (chunkUrnSet.contains(siblingUrn)) {
            hitsByUrn.computeIfAbsent(siblingUrn, u -> new ArrayList<>()).add(hit);
          }
        }
      }
    }
    return hitsByUrn;
  }

  private static List<String> readSiblingUrns(@Nullable final EntityResponse response) {
    if (response == null || !response.getAspects().containsKey(SIBLINGS_ASPECT_NAME)) {
      return Collections.emptyList();
    }
    final EnvelopedAspect aspect = response.getAspects().get(SIBLINGS_ASPECT_NAME);
    final Siblings siblings = new Siblings(aspect.getValue().data());
    if (!siblings.hasSiblings()) {
      return Collections.emptyList();
    }
    return siblings.getSiblings().stream().map(Urn::toString).collect(Collectors.toList());
  }

  private static Map<String, Long> extractTotals(
      @Nullable final SearchResult searchResult, final List<String> chunkUrns) {
    final Map<String, Long> totals = new HashMap<>();
    if (searchResult == null) {
      return totals;
    }
    final Set<String> chunkUrnSet = new HashSet<>(chunkUrns);
    for (AggregationMetadata agg : searchResult.getMetadata().getAggregations()) {
      if (!SIBLINGS_FIELD.equals(agg.getName())) {
        continue;
      }
      agg.getAggregations()
          .forEach(
              (siblingUrn, count) -> {
                if (chunkUrnSet.contains(siblingUrn)) {
                  totals.merge(siblingUrn, count, Long::sum);
                }
              });
    }
    return totals;
  }

  private static ResolvedQuery resolveQuery(
      final GroupKey group, final QueryContext queryContext, final ViewService viewService) {

    if (group.viewUrn == null) {
      return new ResolvedQuery(group.entityNames, group.inputFilter);
    }

    final DataHubViewInfo view =
        SearchUtils.resolveView(
            queryContext.getOperationContext(), viewService, UrnUtils.getUrn(group.viewUrn));
    if (view == null) {
      return new ResolvedQuery(group.entityNames, group.inputFilter);
    }

    return new ResolvedQuery(
        SearchUtils.intersectEntityTypes(group.entityNames, view.getDefinition().getEntityTypes()),
        SearchUtils.combineFilters(group.inputFilter, view.getDefinition().getFilter()));
  }

  private static Filter combineWithSiblingsFilter(
      @Nullable final Filter baseFilter, final List<String> urns) {
    final StringArray values = new StringArray();
    for (String urn : urns) {
      values.add(urn);
    }
    final CriterionArray criteria = new CriterionArray();
    criteria.add(buildCriterion(SIBLINGS_FIELD, Condition.EQUAL, values));
    final Filter siblingsFilter =
        new Filter()
            .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criteria)));

    return baseFilter == null
        ? siblingsFilter
        : SearchUtils.combineFilters(baseFilter, siblingsFilter);
  }

  private static ScrollResults toScrollResults(
      final QueryContext queryContext, final List<SearchEntity> hits, final long total) {
    final ScrollResults results = new ScrollResults();
    // A grouped query cannot produce a per-key scroll cursor. Callers that pass a scrollId take the
    // unbatched path, so this only affects starting a scroll.
    results.setNextScrollId(null);
    // `count` is how many entities this result set carries, not the requested page size — the
    // unbatched path derives it from the response (UrnScrollResultsMapper#apply), so a key with no
    // siblings reports 0 rather than the count it asked for.
    results.setCount(hits.size());
    results.setTotal((int) Math.min(total, Integer.MAX_VALUE));
    results.setSearchResults(
        hits.stream()
            .map(hit -> MapperUtils.mapResult(queryContext, hit))
            .collect(Collectors.toList()));
    results.setFacets(Collections.emptyList());
    return results;
  }

  private static ScrollResults emptyResults() {
    return toScrollResults(null, Collections.emptyList(), 0);
  }

  private static SearchFlags copyFlags(final SearchFlags flags) {
    try {
      return flags.copy();
    } catch (CloneNotSupportedException e) {
      // Unreachable: DataTemplate extends Cloneable. Required by Object.clone()'s signature.
      throw new IllegalStateException("Failed to clone SearchFlags", e);
    }
  }

  private static <T> List<List<T>> partition(final List<T> list, final int size) {
    final List<List<T>> chunks = new ArrayList<>();
    for (int i = 0; i < list.size(); i += size) {
      chunks.add(list.subList(i, Math.min(i + size, list.size())));
    }
    return chunks;
  }

  private static final class ResolvedQuery {
    private final List<String> entityNames;
    @Nullable private final Filter baseFilter;

    private ResolvedQuery(final List<String> entityNames, @Nullable final Filter baseFilter) {
      this.entityNames = entityNames;
      this.baseFilter = baseFilter;
    }
  }
}
