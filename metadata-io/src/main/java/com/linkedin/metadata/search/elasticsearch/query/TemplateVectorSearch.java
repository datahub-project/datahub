package com.linkedin.metadata.search.elasticsearch.query;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.util.exception.ESQueryException;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.VectorStoreConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.VectorRetrievalExplanation;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.query.NativeVectorStore.Neighbor;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCapabilities;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateWalk;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.VectorStore;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

/**
 * Runs a {@code combine: vector} rung. It embeds the query for the rung's embedding profile and
 * retrieves one window of nearest neighbors from the store serving that profile. The Search V3
 * index retrieves inside the filter the keyword leg applies: the caller's filter, the default
 * search filters, and the searched entity types. An external store's hits are hydrated on that
 * index inside the same filter, so a deleted, hidden, or filtered-out entity never surfaces. Hits
 * below the floors are dropped and the window is not refilled. The page, the total, and the facets
 * cover the hits that survive. View authorization masks the page after the top-k, as on the keyword
 * leg.
 *
 * <p>With {@code querySource: anchorEntity} (similarity) the query is an entity URN. That entity's
 * stored chunk vectors are the query vectors, so nothing is embedded, and a neighbor scores its
 * best chunk-to-chunk cosine. The anchor's entity type selects the embedding profile.
 */
@Slf4j
final class TemplateVectorSearch {
  // Failed calls in a row that pause an external store, and for how long, as for hybrid read
  private static final int FAILURES_BEFORE_PAUSE = 3;
  private static final long PAUSE_NANOS = TimeUnit.SECONDS.toNanos(30);

  // How many of one anchor's kNN requests run at once: a quarter of the coordinator's anchor pool,
  // so one anchor with many chunks never holds half of it and other searches keep running
  private static final int ANCHOR_KNN_CONCURRENCY = 4;

  // Best score first, then URN, so equal scores keep one order whatever order the store used
  private static final Comparator<KnnSearchResponse.Hit> BEST_FIRST =
      Comparator.comparingDouble(KnnSearchResponse.Hit::score)
          .reversed()
          .thenComparing(hit -> (String) hit.source().get("urn"));

  private final ESSearchDAO dao;
  private final NativeVectorStore nativeStore;
  private final Map<String, Breaker> breakers = new ConcurrentHashMap<>();

  TemplateVectorSearch(@Nonnull ESSearchDAO dao) {
    this.dao = dao;
    this.nativeStore = new NativeVectorStore(dao);
  }

  /**
   * One walk attempt for a vector rung. No eligible entity type is an empty page. A failed
   * embedding, store, hydration, or facet request follows the rung's {@code onFailure}: {@code
   * next} returns the failure to the ladder, and anything else fails the search. A store with no
   * adapter, or a caller filter on a store without preFilter, fails closed. Similarity checks its
   * anchor first, so an anchor it cannot use fails the search even when no requested type is
   * eligible.
   */
  @Nonnull
  SearchTemplateWalk.AttemptResult<SearchResult> attempt(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    final SearchTemplateGates gates = SearchTemplateRuntimes.coordinator(opContext).gates();
    final ResolvedSearchTemplate resolved =
        opContext.getSearchContext().getResolvedSearchTemplate();
    final String engine = resolved == null ? null : resolved.getSearchSystem();
    final Map<String, VectorStoreConfiguration> stores = stores();
    final String system =
        SearchTemplateVectorRetrieval.system(gates, template, entityNames, engine, stores);
    final int pageSize = ConfigUtils.applyLimit(dao.searchServiceConfig, size);
    // One deadline covers the embedding, or the anchor check and read, and the kNN calls on the
    // DataHub index. Bind starts it for a selected similarity rung, which checks the anchor before
    // the result cache. An external store's call has its own deadline and breaker.
    final long deadlineNanos =
        resolved != null && resolved.getVectorDeadlineNanos() != 0
            ? resolved.getVectorDeadlineNanos()
            : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(dao.hybridTimeoutMillis);
    final List<String> joined = new ArrayList<>();
    final List<String> excluded = new ArrayList<>();
    gates
        .entityGates(template, entityNames, system)
        .forEach(
            (entityType, reason) -> {
              if (reason == null) {
                joined.add(entityType);
              } else {
                excluded.add(entityType + ":" + reason);
              }
            });
    final boolean similarity =
        SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(
            template.getRetrieval().getQuerySource());
    final Urn anchor = similarity ? anchor(opContext, template, input) : null;
    final String profile;
    final List<float[]> anchorVectors;
    if (anchor != null) {
      // Similarity embeds nothing, so it needs semantic read and no embedding provider
      if (!SemanticEntitySearchService.shouldReadSemanticV3(
          dao.searchConfiguration.getEntityIndex())) {
        SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.UNAVAILABLE);
        throw new SearchTemplateFailClosedException(
            "Search template "
                + template.getUrn()
                + " finds similar entities, which needs Search V3 semantic read");
      }
      // The anchor's entity type must pass the entity gates, as a candidate type does. The rung's
      // scope assigns candidate types only, so it does not apply to the anchor.
      final Set<String> anchorProfiles = gates.profileIds(template, anchor.getEntityType(), system);
      if (anchorProfiles.size() != 1) {
        throw SearchTemplateVectorRetrieval.noAnchor(
            opContext, template, anchor + " has no embedding profile to search with");
      }
      profile = anchorProfiles.iterator().next();
      anchorVectors = anchorVectors(opContext, template, anchor, profile, deadlineNanos);
      // Only requested types indexed under the anchor's profile are candidates
      joined.removeIf(
          entityType -> {
            if (anchorProfiles.equals(
                gates.joinedProfileIds(template, List.of(entityType), system))) {
              return false;
            }
            excluded.add(entityType + ":" + SearchTemplateVectorRetrieval.NOT_ANCHOR_PROFILE);
            return true;
          });
    } else {
      profile = null;
      anchorVectors = null;
    }
    final VectorRetrievalExplanation explanation =
        new VectorRetrievalExplanation().setExcludedEntityTypes(new StringArray(excluded));
    final Map<String, String> indices =
        SemanticEntitySearchService.v3SemanticIndices(
            opContext, joined, dao.searchConfiguration.getEntityIndex().getSemanticSearch());
    if (indices.isEmpty()) {
      return page(
          opContext,
          explanation.setOutcome(SearchTemplateVectorRetrieval.NO_ELIGIBLE_ENTITIES),
          List.of(),
          new AggregationMetadataArray(),
          from,
          pageSize);
    }
    final HybridQueryEmbeddingService embedding = dao.vectorQueryEmbedding;
    if (anchor == null && embedding == null) {
      // The template asks for something this process cannot run, so it fails closed
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.UNAVAILABLE);
      throw new SearchTemplateFailClosedException(
          "Search template "
              + template.getUrn()
              + " retrieves by vector, which needs Search V3 semantic read and an embedding"
              + " provider");
    }
    // A profile this deployment cannot embed is not a failed call, so onFailure does not apply.
    // Similarity embeds nothing: it queries with the anchor's stored vectors.
    final Set<String> profiles = gates.joinedProfileIds(template, joined, system);
    if (anchor == null && !profiles.equals(Set.of(embedding.getModelEmbeddingKey()))) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.UNAVAILABLE);
      throw new SearchTemplateFailClosedException(
          "Search template "
              + template.getUrn()
              + " selects embedding profiles "
              + profiles
              + ", and this deployment embeds queries for "
              + embedding.getModelEmbeddingKey());
    }
    final Store store = store(system, stores);
    if (store == null) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.UNAVAILABLE);
      throw new SearchTemplateFailClosedException(
          "Search template "
              + template.getUrn()
              + " is served by vector store "
              + system
              + ", which has no adapter in this process");
    }
    // Trimming a global top-k to the caller's filter would return the wrong neighbors
    if (!store.capabilities().contains(VectorStore.PRE_FILTER)
        && hasCallerFilter(opContext, postFilters)) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.NO_PRE_FILTER);
      throw new SearchTemplateFailClosedException(
          "Search template "
              + template.getUrn()
              + " has a caller filter, and vector store "
              + system
              + " cannot apply it inside its search");
    }
    try {
      return retrieve(
          opContext,
          template,
          gates,
          embedding,
          store,
          anchor == null ? embedding.getModelEmbeddingKey() : profile,
          anchor,
          anchorVectors,
          new ArrayList<>(indices.keySet()),
          indices,
          input,
          postFilters,
          from,
          pageSize,
          facets,
          explanation,
          deadlineNanos);
    } catch (SearchTemplateFailClosedException e) {
      throw e;
    } catch (RuntimeException | IOException e) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.FAILED);
      final RuntimeException failure =
          e instanceof RuntimeException runtime
              ? runtime
              : new ESQueryException("Vector retrieval failed", e);
      // Similarity has no other rung to serve its neighbors, so it always fails
      if (anchor == null
          && SearchTemplateCapabilities.ON_FAILURE_NEXT.equals(
              template.getRetrieval().getOnFailure())) {
        // The next rung's page must not be cached, or the next search skips this rung
        if (resolved != null) {
          resolved.setSkipResultCache(true);
        }
        return SearchTemplateWalk.AttemptResult.failed(failure);
      }
      throw new SearchTemplateFailClosedException(
          "Search template " + template.getUrn() + " failed to retrieve by vector", failure);
    }
  }

  @Nonnull
  private SearchTemplateWalk.AttemptResult<SearchResult> retrieve(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateGates gates,
      @Nullable HybridQueryEmbeddingService embedding,
      @Nonnull Store store,
      @Nonnull String profileId,
      @Nullable Urn anchor,
      @Nullable List<float[]> anchorVectors,
      @Nonnull List<String> searched,
      @Nonnull Map<String, String> indices,
      @Nonnull String input,
      @Nullable Filter postFilters,
      int from,
      int pageSize,
      @Nonnull List<String> facets,
      @Nonnull VectorRetrievalExplanation explanation,
      long deadlineNanos)
      throws IOException {
    final Integer cap = template.getRetrieval().getCandidateCap();
    final int window = SearchTemplateVectorRetrieval.window(cap, from, pageSize);
    final int k =
        store.external() ? Math.max(1, Math.min(window, store.config().getMaxK())) : window;
    final Filter filter =
        dao.transformFilter(
            opContext, searched, postFilters, opContext.getSearchContext().getIndexConvention());
    final List<EntitySpec> entitySpecs =
        searched.stream().map(name -> opContext.getEntityRegistry().getEntitySpec(name)).toList();
    final SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            entitySpecs,
            dao.searchConfiguration,
            dao.customSearchConfiguration,
            dao.queryFilterRewriteChain,
            dao.searchServiceConfig);
    final SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    final boolean preFilter = store.capabilities().contains(VectorStore.PRE_FILTER);
    final List<Neighbor> hits;
    final int candidates;
    if (anchor != null) {
      // The keyword leg's filter, inside the kNN request, so neighbors are retrieved inside it
      final BoolQueryBuilder filterQuery = handler.getFilterQuery(opContext, filter);
      if (template.getRetrieval().getAnchor() != null
          && Boolean.TRUE.equals(template.getRetrieval().getAnchor().isExcludeSelf())) {
        filterQuery.mustNot(QueryBuilders.termQuery("urn", anchor.toString()));
      }
      final KnnSearchRequest.Builder request =
          KnnSearchRequest.builder()
              .indexName(String.join(",", indices.values()))
              .vectorField(V3HybridKnnRequestBuilder.vectorField(profileId))
              .k(k)
              .fieldsToFetch(
                  new ArrayList<>(
                      SearchDocFieldFetchConfig.resolve(
                          SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH, flags)))
              .filter(ESUtils.filterMap(filterQuery));
      hits = new ArrayList<>();
      for (KnnSearchResponse.Hit hit :
          neighbors(opContext, request, anchorVectors, k, deadlineNanos)) {
        final Urn urn = urn(hit);
        if (urn != null) {
          hits.add(
              new Neighbor(
                  urn,
                  SearchTemplateVectorRetrieval.cosine(hit.score()),
                  hit.score(),
                  hit.source()));
        }
      }
      candidates = hits.size();
    } else {
      final float[] vector = embedding.embed(input, deadlineNanos).vector();
      final long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        throw new UncheckedTimeoutException(
            "The vector retrieval deadline passed before the kNN call");
      }
      // The caller's filter only goes to a store that applies it inside its search
      final VectorStore.Request request =
          new VectorStore.Request(profileId, vector, k, searched, preFilter ? filter : null);
      if (store.external()) {
        final List<VectorStore.Hit> returned =
            call(store, explanation, timeout -> store.store().search(opContext, request, timeout));
        candidates = returned.size();
        hits = collapse(returned);
      } else {
        hits = nativeStore.knn(opContext, request, Duration.ofNanos(remainingNanos));
        candidates = hits.size();
      }
    }
    // A floor the template sets is bound only to a cosine profile. One inherited from configuration
    // is skipped on another metric, where a raw cosine means nothing.
    final Float templateFloor = template.getRetrieval().getMinSimilarity();
    final Float inheritedFloor = templateFloor == null ? gates.minSimilarity(template) : null;
    final EmbeddingProfile profile = gates.profiles().profile(profileId);
    final boolean cosine = profile != null && profile.cosine();
    final Float minSimilarity =
        templateFloor != null ? templateFloor : cosine ? inheritedFloor : null;
    if (inheritedFloor != null && !cosine) {
      explanation.setMinSimilaritySkipped(SearchTemplateVectorRetrieval.PROFILE_NOT_COSINE);
    }
    final Float minScore = flags.hasMinScore() ? flags.getMinScore() : null;
    final List<Neighbor> kept = new ArrayList<>();
    int belowMinSimilarity = 0;
    int belowMinScore = 0;
    for (Neighbor hit : hits) {
      // Compared as floats, so a hit at the floor is kept
      final boolean underSimilarity =
          minSimilarity != null && (float) hit.similarity() < minSimilarity;
      final boolean underScore = minScore != null && (float) hit.score() < minScore;
      belowMinSimilarity += underSimilarity ? 1 : 0;
      belowMinScore += underScore ? 1 : 0;
      if (!underSimilarity && !underScore) {
        kept.add(hit);
      }
    }
    final List<SearchEntity> survivors;
    final AggregationMetadataArray aggregations;
    if (store.external()) {
      // An external hit the caller cannot view never reaches a facet, the total, or the page
      final List<Neighbor> viewable =
          kept.stream().filter(hit -> !unauthorized(opContext, hit.urn())).toList();
      final Hydration hydration =
          hydrate(opContext, handler, searched, filter, viewable, facets, flags);
      survivors = hydration.survivors();
      aggregations = hydration.aggregations();
      explanation.setRemovedByHydration(kept.size() - survivors.size());
    } else {
      survivors = new ArrayList<>();
      kept.forEach(hit -> survivors.add(entity(opContext, hit, hit.source())));
      aggregations = facets(opContext, handler, entitySpecs, searched, filter, survivors, facets);
    }
    if (cap != null && cap > k) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.CAP_CLAMPED);
      explanation.setClampedCandidateCap(cap);
    }
    if (belowMinSimilarity + belowMinScore > 0) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.FLOOR_DROPPED);
    }
    if (minScore != null) {
      explanation.setDeprecations(
          new StringArray(SearchTemplateVectorRetrieval.MIN_SCORE_DEPRECATION));
    }
    explanation
        .setOutcome(
            survivors.isEmpty()
                ? SearchTemplateVectorRetrieval.EMPTY
                : from >= survivors.size()
                    ? SearchTemplateVectorRetrieval.PAST_WINDOW
                    : SearchTemplateVectorRetrieval.SERVED)
        .setProfile(profileId)
        .setStore(store.name(), SetMode.IGNORE_NULL)
        .setEntityTypes(new StringArray(searched))
        .setK(k)
        .setCandidates(candidates)
        .setMinSimilarity(minSimilarity, SetMode.IGNORE_NULL)
        .setBelowMinSimilarity(belowMinSimilarity)
        .setBelowMinScore(belowMinScore)
        .setFilterInVectorSearch(preFilter);
    return page(opContext, explanation, survivors, aggregations, from, pageSize);
  }

  /**
   * The anchor entity's query vectors for similarity, from the store serving the profile. A store
   * without findById cannot serve similarity, so the search fails closed.
   */
  @Nonnull
  List<float[]> anchorVectors(
      @Nonnull OperationContext opContext,
      @Nullable String system,
      @Nonnull String profile,
      @Nonnull Urn anchor)
      throws IOException {
    final Store store = store(system, stores());
    if (store == null || !store.capabilities().contains(VectorStore.FIND_BY_ID)) {
      throw new SearchTemplateFailClosedException(
          "Vector store "
              + system
              + " cannot find an entity by URN, so it cannot serve similarity");
    }
    // Only an external store finds by id, so the call runs under its deadline and breaker
    return call(
        store, null, timeout -> store.store().findById(opContext, profile, anchor, timeout));
  }

  /**
   * Hydrates an external store's hits on the DataHub index: one request inside the keyword leg's
   * filter, restricted to their URNs, that also counts the facets. A hit the index does not return,
   * because the entity is deleted, hidden, or outside the filter, is dropped. The store's order is
   * kept.
   */
  @Nonnull
  private Hydration hydrate(
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequestHandler handler,
      @Nonnull List<String> searched,
      @Nullable Filter filter,
      @Nonnull List<Neighbor> kept,
      @Nonnull List<String> facets,
      @Nonnull SearchFlags flags)
      throws IOException {
    if (kept.isEmpty()) {
      return new Hydration(List.of(), new AggregationMetadataArray());
    }
    final SearchRequest request =
        handler.getUrnFacetRequest(
            opContext, filter, kept.stream().map(hit -> hit.urn().toString()).toList(), facets);
    request
        .source()
        .size(kept.size())
        .fetchSource(
            SearchDocFieldFetchConfig.resolve(
                    SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH, flags)
                .toArray(String[]::new),
            null);
    request.indices(dao.entityIndexNames(opContext, searched));
    final SearchResponse response =
        dao.searchClient(opContext, request).search(opContext, request, RequestOptions.DEFAULT);
    if (response.getFailedShards() > 0 || response.isTimedOut()) {
      throw new IOException("The hydration request reported a timed-out or failed shard");
    }
    final Map<String, Map<String, Object>> sources = new HashMap<>();
    for (SearchHit hit : response.getHits().getHits()) {
      final Map<String, Object> source = hit.getSourceAsMap();
      if (source != null && source.get("urn") instanceof String urn) {
        sources.put(urn, source);
      }
    }
    final List<SearchEntity> survivors = new ArrayList<>();
    for (Neighbor hit : kept) {
      final Map<String, Object> source = sources.get(hit.urn().toString());
      if (source != null) {
        survivors.add(entity(opContext, hit, source));
      }
    }
    if (survivors.isEmpty() || !Boolean.FALSE.equals(flags.isSkipAggregates())) {
      return new Hydration(survivors, new AggregationMetadataArray());
    }
    return new Hydration(
        survivors,
        dao.transformIndexIntoEntityName(
                opContext,
                opContext.getSearchContext().getIndexConvention(),
                handler.extractResult(opContext, response, filter, 0, 0))
            .getMetadata()
            .getAggregations());
  }

  /**
   * The anchor's neighbors: one kNN request per anchor chunk, each for the window of {@code k}. A
   * hit keeps its best score, so a neighbor scores its best chunk-to-chunk cosine, and the window
   * is the k best of them. The requests run on the coordinator's anchor pool, {@link
   * #ANCHOR_KNN_CONCURRENCY} at a time, under one deadline; a full pool or the deadline fails the
   * search. Each request carries the time left as its engine and client timeout, so a request the
   * search stops waiting for frees its worker by about the deadline. Each chunk's hits are merged
   * in chunk order and the merged hits are cut back to the k best, so at most k and one chunk's
   * hits are kept. Cutting keeps the true k best in any chunk order: a neighbor cut while k others
   * beat it can come back only with a better score from a later chunk, and scores never drop.
   */
  @Nonnull
  private static List<KnnSearchResponse.Hit> neighbors(
      @Nonnull OperationContext opContext,
      @Nonnull KnnSearchRequest.Builder request,
      @Nonnull List<float[]> vectors,
      int k,
      long deadlineNanos)
      throws IOException {
    final SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    final Map<String, KnnSearchResponse.Hit> best = new HashMap<>();
    final Deque<Future<List<KnnSearchResponse.Hit>>> running = new ArrayDeque<>();
    int started = 0;
    try {
      for (int merged = 0; merged < vectors.size(); merged++) {
        while (started < vectors.size() && running.size() < ANCHOR_KNN_CONCURRENCY) {
          // Built here: the builder is shared and the calls run on other threads
          final KnnSearchRequest chunk = knnRequest(request, vectors.get(started++), deadlineNanos);
          running.add(coordinator.startAnchorCall(() -> knn(opContext, chunk)));
        }
        keepBest(best, coordinator.awaitAnchorCall(running.remove(), deadlineNanos), k);
      }
    } finally {
      running.forEach(call -> call.cancel(true));
    }
    return best.values().stream().sorted(BEST_FIRST).toList();
  }

  /** Merges one chunk's hits, each neighbor at its best score, and keeps the k best. */
  private static void keepBest(
      @Nonnull Map<String, KnnSearchResponse.Hit> best,
      @Nonnull List<KnnSearchResponse.Hit> hits,
      int k) {
    for (KnnSearchResponse.Hit hit : hits) {
      if (hit.source().get("urn") instanceof String urn) {
        best.merge(urn, hit, (kept, other) -> other.score() > kept.score() ? other : kept);
      }
    }
    if (best.size() > k) {
      final List<KnnSearchResponse.Hit> kept =
          best.values().stream().sorted(BEST_FIRST).limit(k).toList();
      best.clear();
      kept.forEach(hit -> best.put((String) hit.source().get("urn"), hit));
    }
  }

  /** One kNN request with this query vector, which ends by the deadline. */
  @Nonnull
  private static KnnSearchRequest knnRequest(
      @Nonnull KnnSearchRequest.Builder request, @Nonnull float[] vector, long deadlineNanos) {
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new UncheckedTimeoutException(
          "The vector retrieval deadline passed before the kNN call");
    }
    return request.queryVector(vector).timeout(Duration.ofNanos(remainingNanos)).build();
  }

  /** The hits of one kNN request, in the store's order. */
  @Nonnull
  private static List<KnnSearchResponse.Hit> knn(
      @Nonnull OperationContext opContext, @Nonnull KnnSearchRequest request) throws IOException {
    final KnnSearchResponse response =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3)
            .searchKnn(opContext, request);
    if (response.partial()) {
      throw new IOException("The kNN response reported a timed-out or failed shard");
    }
    return response.hits();
  }

  @Nullable
  private static Urn urn(@Nonnull KnnSearchResponse.Hit hit) {
    if (!(hit.source().get("urn") instanceof String value) || value.isEmpty()) {
      return null;
    }
    try {
      return Urn.createFromString(value);
    } catch (URISyntaxException e) {
      log.warn("Invalid URN in a kNN hit: {}", value);
      return null;
    }
  }

  /** The anchor URN, the whole trimmed query. A query that is not a URN fails the search. */
  @Nonnull
  private static Urn anchor(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull String input) {
    try {
      return Urn.createFromString(input.trim());
    } catch (URISyntaxException e) {
      throw SearchTemplateVectorRetrieval.noAnchor(
          opContext, template, "the query is not an entity URN");
    }
  }

  /**
   * The anchor's stored chunk vectors for the profile, read only when the viewer may use the
   * anchor. An anchor the viewer may not see, one the search filters leave out, and one with no
   * stored vectors fail the search the same way, as {@code noAnchor}, so the failure does not
   * reveal that the entity exists. Bind already refused such an anchor for the selected template. A
   * failed check or read fails the search: no other rung can serve the anchor's neighbors.
   */
  @Nonnull
  private static List<float[]> anchorVectors(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull Urn anchor,
      @Nonnull String profileId,
      long deadlineNanos) {
    final ResolvedSearchTemplate resolved =
        opContext.getSearchContext().getResolvedSearchTemplate();
    final List<float[]> vectors;
    try {
      final SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
      // Bind's answer for this anchor stands, either way: the cache key was stamped with it.
      // The rung checks only an anchor bind did not, or could not, check.
      final boolean usable =
          resolved != null && anchor.toString().equals(resolved.getCheckedAnchor())
              ? resolved.isCheckedAnchorUsable()
              : coordinator.anchorUsable(opContext, anchor, deadlineNanos);
      vectors =
          usable
              ? storedVectors(coordinator.anchorAspect(opContext, anchor, deadlineNanos), profileId)
              : List.of();
    } catch (RuntimeException | IOException e) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.FAILED);
      throw new SearchTemplateFailClosedException(
          "Search template " + template.getUrn() + " failed to read the anchor " + anchor, e);
    }
    if (vectors.isEmpty()) {
      // No keyword "like this" fallback: an anchor without vectors fails the search
      throw SearchTemplateVectorRetrieval.noAnchor(
          opContext, template, anchor + " has no stored vectors for " + profileId);
    }
    if (vectors.size() > SearchTemplateVectorRetrieval.MAX_ANCHOR_CHUNKS) {
      throw SearchTemplateVectorRetrieval.noAnchor(
          opContext,
          template,
          anchor
              + " has more than "
              + SearchTemplateVectorRetrieval.MAX_ANCHOR_CHUNKS
              + " stored chunks for "
              + profileId);
    }
    return vectors;
  }

  /** The anchor's chunk vectors for the profile, from its {@code semanticContent}. */
  @Nonnull
  private static List<float[]> storedVectors(@Nullable Aspect aspect, @Nonnull String profileId) {
    final EmbeddingModelData embeddings =
        aspect == null ? null : new SemanticContent(aspect.data()).getEmbeddings().get(profileId);
    if (embeddings == null) {
      return List.of();
    }
    final List<float[]> vectors = new ArrayList<>();
    for (EmbeddingChunk chunk : embeddings.getChunks()) {
      final float[] vector = new float[chunk.getVector().size()];
      for (int i = 0; i < vector.length; i++) {
        vector[i] = chunk.getVector().get(i);
      }
      if (vector.length > 0) {
        vectors.add(vector);
      }
    }
    return vectors;
  }

  /**
   * The facets of the surviving hits, from a request with no hits restricted to their URNs. Empty
   * when nothing survived or the caller skips aggregations.
   */
  @Nonnull
  private AggregationMetadataArray facets(
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequestHandler handler,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull List<String> searched,
      @Nullable Filter filter,
      @Nonnull List<SearchEntity> survivors,
      @Nonnull List<String> facets)
      throws IOException {
    if (survivors.isEmpty()
        || !Boolean.FALSE.equals(
            opContext.getSearchContext().getSearchFlags().isSkipAggregates())) {
      return new AggregationMetadataArray();
    }
    final SearchRequest request =
        handler.getUrnFacetRequest(
            opContext,
            filter,
            survivors.stream().map(survivor -> survivor.getEntity().toString()).toList(),
            facets);
    request.indices(dao.entityIndexNames(opContext, searched));
    final SearchResponse response =
        dao.searchClient(opContext, request).search(opContext, request, RequestOptions.DEFAULT);
    if (response.getFailedShards() > 0 || response.isTimedOut()) {
      throw new IOException("The facet request reported a timed-out or failed shard");
    }
    return dao.transformIndexIntoEntityName(
            opContext,
            opContext.getSearchContext().getIndexConvention(),
            handler.extractResult(opContext, response, filter, 0, 0))
        .getMetadata()
        .getAggregations();
  }

  @Nonnull
  private static SearchTemplateWalk.AttemptResult<SearchResult> page(
      @Nonnull OperationContext opContext,
      @Nonnull VectorRetrievalExplanation explanation,
      @Nonnull List<SearchEntity> survivors,
      @Nonnull AggregationMetadataArray aggregations,
      int from,
      int pageSize) {
    SearchTemplateMetrics.vectorRetrieval(opContext, explanation.getOutcome());
    final List<SearchEntity> page =
        from >= survivors.size()
            ? List.of()
            : new ArrayList<>(
                survivors.subList(from, (int) Math.min((long) from + pageSize, survivors.size())));
    ESAccessControlUtil.restrictSearchResult(opContext, page);
    final SearchResultMetadata metadata = new SearchResultMetadata().setAggregations(aggregations);
    if (Boolean.TRUE.equals(opContext.getSearchContext().getSearchFlags().isIncludeExplain())) {
      metadata.setVectorRetrieval(explanation);
    }
    final SearchResult result =
        new SearchResult()
            .setEntities(new SearchEntityArray(page))
            .setNumEntities(survivors.size())
            .setFrom(from)
            .setPageSize(pageSize)
            .setMetadata(metadata);
    // A vector rung orders by vector score, never by a template sort
    return SearchTemplateWalk.AttemptResult.ok(
        result, survivors.size(), page, List.of(), false, null, null, false);
  }

  /**
   * An external store's hits with one row per entity, at its best cosine, best first. A store that
   * searches chunks can return an entity once per chunk; the DataHub index returns it once.
   */
  @Nonnull
  private static List<Neighbor> collapse(@Nonnull List<VectorStore.Hit> hits) {
    final Map<Urn, VectorStore.Hit> best = new LinkedHashMap<>();
    for (VectorStore.Hit hit : hits) {
      best.merge(
          hit.urn(), hit, (kept, next) -> next.similarity() > kept.similarity() ? next : kept);
    }
    return best.values().stream()
        .sorted(Comparator.comparingDouble(VectorStore.Hit::similarity).reversed())
        .map(Neighbor::external)
        .toList();
  }

  @Nonnull
  private static SearchEntity entity(
      @Nonnull OperationContext opContext,
      @Nonnull Neighbor hit,
      @Nonnull Map<String, Object> source) {
    return new SearchEntity()
        .setEntity(hit.urn())
        .setScore(hit.score())
        .setFeatures(SearchResultUtils.buildBaseFeatures(hit.score(), source))
        .setExtraFields(SearchResultUtils.toExtraFields(opContext.getObjectMapper(), source));
  }

  /**
   * One call to an external store, on the hybrid read executor, waited on for the store's deadline.
   * A failed or timed-out call counts toward the store's breaker. A store whose calls fail {@link
   * #FAILURES_BEFORE_PAUSE} times in a row is paused, and its calls fail without reaching it.
   */
  private <T> T call(
      @Nonnull Store store,
      @Nullable VectorRetrievalExplanation explanation,
      @Nonnull StoreCall<T> call)
      throws IOException {
    final Breaker breaker = breakers.computeIfAbsent(store.name(), name -> new Breaker());
    if (breaker.paused()) {
      throw new IOException("Vector store " + store.name() + " is paused after repeated failures");
    }
    final Duration timeout = Duration.ofMillis(store.config().getTimeoutMillis());
    final long startNanos = System.nanoTime();
    // A rejected call never reached the store, so it does not count toward the pause
    final Future<T> future =
        dao.hybridExecutor.submit(Context.current().wrap(() -> call.run(timeout)));
    try {
      final T result = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (explanation != null) {
        explanation.setAdapterLatencyMillis(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
      }
      breaker.succeeded();
      return result;
    } catch (TimeoutException e) {
      // The interrupt ends a call blocked in an interruptible wait, and a queued call does not
      // start
      future.cancel(true);
      breaker.failed();
      throw new UncheckedTimeoutException(
          "Vector store " + store.name() + " did not answer within " + timeout.toMillis() + " ms");
    } catch (ExecutionException e) {
      breaker.failed();
      if (e.getCause() instanceof IOException failure) {
        throw failure;
      }
      if (e.getCause() instanceof RuntimeException failure) {
        throw failure;
      }
      throw new IOException("Vector store " + store.name() + " failed", e.getCause());
    } catch (InterruptedException e) {
      // The search itself was interrupted, which says nothing about the store
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for vector store " + store.name(), e);
    }
  }

  /**
   * The store serving {@code system}: an external store named in {@code semanticSearch.stores},
   * with its adapter in this process, else the DataHub index. Null for an external store with no
   * adapter. {@code stores} never holds an engine name, so the engine always means the DataHub
   * index.
   */
  @Nullable
  private Store store(
      @Nullable String system, @Nonnull Map<String, VectorStoreConfiguration> stores) {
    final boolean external = system != null && stores.containsKey(system);
    if (!external) {
      return new Store(system, nativeStore, NativeVectorStore.CAPABILITIES, null);
    }
    final VectorStoreConfiguration config = stores.get(system);
    final VectorStore adapter = dao.vectorStores.get(system);
    return adapter == null
        ? null
        : new Store(
            system,
            adapter,
            config.getCapabilities() == null ? Set.of() : config.getCapabilities(),
            config);
  }

  @Nonnull
  private Map<String, VectorStoreConfiguration> stores() {
    return SearchTemplateVectorRetrieval.stores(
        dao.searchConfiguration.getEntityIndex().getSemanticSearch());
  }

  /**
   * True when the request carries a filter of the caller's own. The system filters come from the
   * search flags, so they never count. A GraphQL resolver that adds the published-document default
   * records whether the caller's filter had a criterion before it did. Without that record, as on
   * Rest.li and OpenAPI, every filter criterion is the caller's.
   */
  private static boolean hasCallerFilter(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    final Boolean callerFiltered = opContext.getSearchContext().getCallerFiltered();
    return callerFiltered != null ? callerFiltered : FilterUtils.hasCriterion(filter);
  }

  /**
   * The keyword leg's view-authorization test for one hit: view authorization is on, the caller is
   * not the system, and the caller cannot view this view-restricted entity. False with
   * includeRestricted, where the page masks such a hit as the keyword leg does.
   */
  private static boolean unauthorized(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    final ViewAuthorizationConfiguration config =
        opContext.getOperationContextConfig().getViewAuthorizationConfiguration();
    return config.isEnabled()
        && !opContext.isSystemAuth()
        && !opContext.getSearchContext().isRestrictedSearch()
        && AuthUtil.isViewRestrictedEntityType(config, urn.getEntityType())
        && !EntityAuthorizationUtils.canViewEntity(opContext, urn);
  }

  /** A store serving a rung. {@code config} is null on the DataHub index. */
  private record Store(
      @Nullable String name,
      @Nonnull VectorStore store,
      @Nonnull Set<String> capabilities,
      @Nullable VectorStoreConfiguration config) {
    boolean external() {
      return config != null;
    }
  }

  private record Hydration(
      @Nonnull List<SearchEntity> survivors, @Nonnull AggregationMetadataArray aggregations) {}

  @FunctionalInterface
  private interface StoreCall<T> {
    T run(@Nonnull Duration timeout) throws IOException;
  }

  /** Pauses an external store after failed calls in a row. */
  private static final class Breaker {
    private final AtomicInteger failures = new AtomicInteger();
    private volatile long pausedUntilNanos = System.nanoTime();

    boolean paused() {
      return System.nanoTime() - pausedUntilNanos < 0;
    }

    void succeeded() {
      failures.set(0);
    }

    void failed() {
      if (failures.incrementAndGet() >= FAILURES_BEFORE_PAUSE) {
        failures.set(0);
        pausedUntilNanos = System.nanoTime() + PAUSE_NANOS;
      }
    }
  }
}
