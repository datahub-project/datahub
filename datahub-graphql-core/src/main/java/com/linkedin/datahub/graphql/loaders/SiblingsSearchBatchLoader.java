package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
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
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader for {@code Dataset.siblingsSearch}, replacing one search per dataset row
 * with one search per chunk.
 *
 * <p>The field asks which entities list this urn as a sibling. That is the reverse of the {@code
 * siblings} aspect, which records only the forward direction and is not guaranteed symmetric, so it
 * has to be a search.
 *
 * <p>Each chunk runs one search filtered to the chunk's urns. Hits are attributed by reading their
 * own {@code siblings} aspect, which the search response does not carry, and each urn's total is
 * the number of hits attributed to it.
 */
@Slf4j
public final class SiblingsSearchBatchLoader {

  public static final String LOADER_NAME = "SiblingsSearch";

  private static final String SIBLINGS_FIELD = "siblings";

  // Keys per batched search. Bounded so one chunk's siblings comfortably fit the hit window.
  private static final int MAX_URNS_PER_AGG = 25;

  // Totals are counted from the returned hits, so the window has to hold every matched document.
  // Two terms, because either can dominate: chunk width sets a floor per urn, and a caller asking
  // for a large page needs at least that many hits per urn to fill it. MIN_WINDOW keeps narrow
  // chunks — a profile page resolves one urn — from truncating on a modest sibling count. `size`
  // is a cap rather than a fetch count, so a wider window costs nothing when few documents match.
  private static final int WINDOW_PER_URN = 8;
  private static final int WINDOW_HEADROOM_FACTOR = 3;
  private static final int MIN_WINDOW = 100;
  private static final int MAX_WINDOW = 500;

  private SiblingsSearchBatchLoader() {}

  /**
   * One {@code siblingsSearch} call. The field takes arguments, so the key is not a bare urn —
   * everything besides {@code urn} is the query shape, which keys in a batch must agree on. Holds
   * Pegasus {@link Filter}/{@link SearchFlags}; the GraphQL input classes compare by identity.
   */
  public static final class Key {
    @Getter private final String urn;
    @Getter private final List<String> entityNames;
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
          // Throw rather than return empty, which would read as "no siblings".
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

    final OperationContext opContext =
        queryContext
            .getOperationContext()
            .withSearchFlags(
                flags -> {
                  // Copy: the group's flags are part of the key, so mutating them breaks its
                  // hash. `flags` is already a copy. Default facets stay suppressed — a chunk's
                  // aggregations describe every urn in it, so they are never returned; callers
                  // that select facets take the unbatched path instead.
                  final SearchFlags base =
                      group.searchFlags != null ? copyFlags(group.searchFlags) : flags;
                  return base.setIncludeDefaultFacets(false);
                });

    ScrollResult searchResult =
        runChunkSearch(
            opContext, resolved, group, urns, windowFor(urns.size(), group.count), entityClient);

    // Every matched document is returned unless the window cut the page short, so a full page means
    // the attributed hits are the complete sibling set and their counts are exact.
    long matched = numEntities(searchResult);
    boolean windowTruncated = searchResult.getEntities().size() < matched;

    // A short page undercounts some key and the response cannot say which. The response does say
    // how many documents matched, so when they all fit the ceiling one resized retry answers the
    // whole chunk — cheaper than the per-key fallback, which costs a query per key.
    if (windowTruncated && matched <= MAX_WINDOW) {
      searchResult = runChunkSearch(opContext, resolved, group, urns, (int) matched, entityClient);
      // Recompute rather than assume: concurrent indexing can grow the match set between queries.
      matched = numEntities(searchResult);
      windowTruncated = searchResult.getEntities().size() < matched;
    }

    if (windowTruncated) {
      log.warn(
          "siblings window held {} of {} matching documents for {} urns; falling back to per-key"
              + " queries. Raising MAX_WINDOW would avoid this.",
          searchResult.getEntities().size(),
          matched,
          urns.size());
      final Map<Key, ScrollResults> perKey = new HashMap<>(chunk.size());
      for (Key key : chunk) {
        perKey.put(key, loadSingle(key, group, resolved, queryContext, entityClient));
      }
      return perKey;
    }

    final Map<String, List<SearchEntity>> hitsByUrn =
        attributeHits(searchResult, urns, opContext, entityClient);

    final Map<Key, ScrollResults> results = new HashMap<>(chunk.size());
    for (Key key : chunk) {
      final List<SearchEntity> attributed =
          hitsByUrn.getOrDefault(key.urn, Collections.emptyList());
      final List<SearchEntity> hits = page(attributed, group.count);
      results.put(
          key, toScrollResults(queryContext, hits, attributed.size(), hits.size() == group.count));
    }
    return results;
  }

  /**
   * The requested page of a urn's hits. A negative count reaches the unbatched path as an unbounded
   * page size, so it has to mean the same here; {@link java.util.stream.Stream#limit} rejects it.
   */
  private static List<SearchEntity> page(final List<SearchEntity> attributed, final int count) {
    return count < 0 ? attributed : attributed.stream().limit(count).collect(Collectors.toList());
  }

  /**
   * The window must hold every document the chunk matches, so it takes the larger of the two things
   * that drive that: how many urns share the search, and how large a page each one asked for.
   */
  private static int windowFor(final int urnCount, final int count) {
    // long: a large caller-supplied `count` would overflow the product.
    final long perUrn = Math.max(WINDOW_PER_URN, (long) count * WINDOW_HEADROOM_FACTOR);
    return (int) Math.min(MAX_WINDOW, Math.max(MIN_WINDOW, urnCount * perUrn));
  }

  private static long numEntities(final ScrollResult result) {
    return result.hasNumEntities() ? result.getNumEntities() : 0L;
  }

  /**
   * Same search API as the unbatched resolver, so first-page ranking matches by construction rather
   * than by measurement. keepAlive is null: this never continues a scroll, and a non-null value
   * would open a point-in-time snapshot per chunk (ESSearchDAO#usePIT).
   */
  private static ScrollResult runChunkSearch(
      final OperationContext opContext,
      final ResolvedQuery resolved,
      final GroupKey group,
      final List<String> urns,
      final int window,
      final EntityClient entityClient)
      throws Exception {
    return entityClient.scrollAcrossEntities(
        opContext,
        resolved.entityNames,
        group.query,
        combineWithSiblingsFilter(resolved.baseFilter, urns),
        null,
        null,
        Collections.emptyList(),
        window,
        Collections.emptyList());
  }

  private static ScrollResults loadSingle(
      final Key key,
      final GroupKey group,
      final ResolvedQuery resolved,
      final QueryContext queryContext,
      final EntityClient entityClient)
      throws Exception {

    // No facets requested, so don't aggregate the default set.
    final OperationContext opContext =
        queryContext
            .getOperationContext()
            .withSearchFlags(
                flags ->
                    (group.searchFlags != null ? copyFlags(group.searchFlags) : flags)
                        .setIncludeDefaultFacets(false));

    final ScrollResult result =
        entityClient.scrollAcrossEntities(
            opContext,
            resolved.entityNames,
            group.query,
            combineWithSiblingsFilter(resolved.baseFilter, List.of(key.urn)),
            null,
            null,
            Collections.emptyList(),
            group.count,
            Collections.emptyList());

    final ScrollResults single =
        toScrollResults(
            queryContext,
            result.getEntities(),
            result.hasNumEntities() ? result.getNumEntities() : 0,
            false);
    // This came from a real single-key scroll, so use its own cursor verbatim.
    single.setNextScrollId(result.getScrollId());
    return single;
  }

  /** Maps each hit to the chunk urns it is a sibling of, read from the hit's own aspect. */
  private static Map<String, List<SearchEntity>> attributeHits(
      @Nullable final ScrollResult searchResult,
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

    // batchGetV2 takes one entity type; siblings are datasets today, but group anyway.
    final Map<String, Set<Urn>> byEntityType = new LinkedHashMap<>();
    for (Urn urn : hitByUrn.keySet()) {
      byEntityType.computeIfAbsent(urn.getEntityType(), t -> new LinkedHashSet<>()).add(urn);
    }

    final Map<Urn, EntityResponse> responses = new HashMap<>();
    for (Map.Entry<String, Set<Urn>> entry : byEntityType.entrySet()) {
      // Only the siblings edge is read, so skip the key aspect.
      responses.putAll(
          entityClient.batchGetV2(
              opContext, entry.getKey(), entry.getValue(), Set.of(SIBLINGS_ASPECT_NAME), false));
    }

    // Iterate hits, not responses: batchGetV2 returns an unordered map, and callers read
    // searchResults[0]. Walking responses would pick an arbitrary sibling per request.
    for (Map.Entry<Urn, SearchEntity> hitEntry : hitByUrn.entrySet()) {
      final EntityResponse response = responses.get(hitEntry.getKey());
      if (response == null) {
        continue;
      }
      for (String siblingUrn : readSiblingUrns(response)) {
        if (chunkUrnSet.contains(siblingUrn)) {
          hitsByUrn.computeIfAbsent(siblingUrn, u -> new ArrayList<>()).add(hitEntry.getValue());
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
      final QueryContext queryContext,
      final List<SearchEntity> hits,
      final long total,
      final boolean pageIsFull) {
    final ScrollResults results = new ScrollResults();
    // Each hit carries its own cursor (SearchRequestHandler stamps extraFields.scrollId from that
    // hit's sort values). Sort is [_score, urn] and filter context does not score, so the value is
    // the same one the unbatched query would have produced for this row. Only emit it on a full
    // page, matching SearchRequestHandler's "there may be more" rule.
    // `count: 0` asks for a total and no hits, which counts as a full page but has no last hit.
    results.setNextScrollId(pageIsFull && !hits.isEmpty() ? cursorOf(hits) : null);
    // Entities returned, not the requested page size — matches UrnScrollResultsMapper.
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
    return toScrollResults(null, Collections.emptyList(), 0, false);
  }

  /**
   * The cursor the last hit carries, or null when it has none. Callers guard against an empty page.
   * A null here means the page cannot advertise where to resume even though more may exist, so say
   * so rather than letting the page look like the end of the results.
   */
  @Nullable
  private static String cursorOf(final List<SearchEntity> hits) {
    final SearchEntity last = hits.get(hits.size() - 1);
    final StringMap extra = last.getExtraFields();
    final String cursor = extra == null ? null : extra.get("scrollId");
    if (cursor == null) {
      log.warn(
          "Hit {} carries no scrollId, so this page cannot return a cursor and will read as the"
              + " last page.",
          last.getEntity());
    }
    return cursor;
  }

  private static SearchFlags copyFlags(final SearchFlags flags) {
    try {
      return flags.copy();
    } catch (CloneNotSupportedException e) {
      // Unreachable: DataTemplate is Cloneable.
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
