package com.linkedin.metadata.search.elasticsearch.query;

import com.datahub.util.exception.ESQueryException;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.SearchComponent;
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
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCapabilities;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateWalk;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;

/**
 * Runs a {@code combine: vector} rung on the Search V3 index. It embeds the query for the rung's
 * embedding profile and retrieves one window of nearest neighbors inside the filter the keyword leg
 * applies: the caller's filter, the default search filters, and the searched entity types. Hits
 * below the floors are dropped and the window is not refilled. The page, the total, and the facets
 * cover the hits that survive. View authorization masks the page after the top-k, as on the keyword
 * leg.
 */
@Slf4j
final class TemplateVectorSearch {
  private final ESSearchDAO dao;

  TemplateVectorSearch(@Nonnull ESSearchDAO dao) {
    this.dao = dao;
  }

  /**
   * One walk attempt for a vector rung. No eligible entity type is an empty page. A failed
   * embedding, kNN, or facet request follows the rung's {@code onFailure}: {@code next} returns the
   * failure to the ladder, and anything else fails the search.
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
    final String system = resolved == null ? null : resolved.getSearchSystem();
    final int pageSize = ConfigUtils.applyLimit(dao.searchServiceConfig, size);
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
    if (embedding == null) {
      // The template asks for something this process cannot run, so it fails closed
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.UNAVAILABLE);
      throw new SearchTemplateException(
          "Search template "
              + template.getUrn()
              + " retrieves by vector, which needs Search V3 semantic read and an embedding"
              + " provider");
    }
    try {
      final Set<String> profiles = gates.joinedProfileIds(template, joined, system);
      if (!profiles.equals(Set.of(embedding.getModelEmbeddingKey()))) {
        throw new SearchTemplateException(
            "Search template "
                + template.getUrn()
                + " selects embedding profiles "
                + profiles
                + ", and this deployment embeds queries for "
                + embedding.getModelEmbeddingKey());
      }
      return retrieve(
          opContext,
          template,
          gates,
          embedding,
          new ArrayList<>(indices.keySet()),
          indices,
          input,
          postFilters,
          from,
          pageSize,
          facets,
          explanation);
    } catch (RuntimeException | IOException e) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.FAILED);
      final RuntimeException failure =
          e instanceof RuntimeException runtime
              ? runtime
              : new ESQueryException("Vector retrieval failed", e);
      if (SearchTemplateCapabilities.ON_FAILURE_NEXT.equals(
          template.getRetrieval().getOnFailure())) {
        return SearchTemplateWalk.AttemptResult.failed(failure);
      }
      throw failure;
    }
  }

  @Nonnull
  private SearchTemplateWalk.AttemptResult<SearchResult> retrieve(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateGates gates,
      @Nonnull HybridQueryEmbeddingService embedding,
      @Nonnull List<String> searched,
      @Nonnull Map<String, String> indices,
      @Nonnull String input,
      @Nullable Filter postFilters,
      int from,
      int pageSize,
      @Nonnull List<String> facets,
      @Nonnull VectorRetrievalExplanation explanation)
      throws IOException {
    final Integer cap = template.getRetrieval().getCandidateCap();
    final int k = SearchTemplateVectorRetrieval.window(cap, from, pageSize);
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
    // One deadline covers the embedding and the kNN call. There is no circuit breaker yet: the
    // corpus index is the only store, and a breaker per store can come with external stores.
    final long deadlineNanos =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(dao.hybridTimeoutMillis);
    final float[] vector = embedding.embed(input, deadlineNanos).vector();
    final long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new UncheckedTimeoutException(
          "The vector retrieval deadline passed before the kNN call");
    }
    // The keyword leg's filter, inside the kNN request, so neighbors are retrieved inside it
    final KnnSearchResponse response =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3)
            .searchKnn(
                opContext,
                KnnSearchRequest.builder()
                    .indexName(String.join(",", indices.values()))
                    .vectorField(
                        V3HybridKnnRequestBuilder.vectorField(embedding.getModelEmbeddingKey()))
                    .queryVector(vector)
                    .k(k)
                    .timeout(Duration.ofNanos(remainingNanos))
                    .fieldsToFetch(
                        new ArrayList<>(
                            SearchDocFieldFetchConfig.resolve(
                                SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH,
                                flags)))
                    .filter(ESUtils.filterMap(handler.getFilterQuery(opContext, filter)))
                    .build());
    if (response.partial()) {
      throw new IOException("The kNN response reported a timed-out or failed shard");
    }
    final Float minSimilarity = gates.minSimilarity(template);
    final Float minScore = flags.hasMinScore() ? flags.getMinScore() : null;
    final List<SearchEntity> survivors = new ArrayList<>();
    int belowMinSimilarity = 0;
    int belowMinScore = 0;
    for (KnnSearchResponse.Hit hit : response.hits()) {
      final Urn urn = urn(hit);
      if (urn == null) {
        continue;
      }
      // Compared as floats, so a hit at the floor is kept
      final boolean underSimilarity =
          minSimilarity != null
              && (float) SearchTemplateVectorRetrieval.cosine(hit.score()) < minSimilarity;
      final boolean underScore = minScore != null && hit.score() < minScore;
      belowMinSimilarity += underSimilarity ? 1 : 0;
      belowMinScore += underScore ? 1 : 0;
      if (!underSimilarity && !underScore) {
        survivors.add(
            new SearchEntity()
                .setEntity(urn)
                .setScore(hit.score())
                .setFeatures(SearchResultUtils.buildBaseFeatures(hit.score(), hit.source()))
                .setExtraFields(
                    SearchResultUtils.toExtraFields(opContext.getObjectMapper(), hit.source())));
      }
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
        .setProfile(embedding.getModelEmbeddingKey())
        .setEntityTypes(new StringArray(searched))
        .setK(k)
        .setCandidates(response.hits().size())
        .setMinSimilarity(minSimilarity, SetMode.IGNORE_NULL)
        .setBelowMinSimilarity(belowMinSimilarity)
        .setBelowMinScore(belowMinScore)
        .setFilterInVectorSearch(true);
    return page(
        opContext,
        explanation,
        survivors,
        facets(opContext, handler, entitySpecs, searched, filter, survivors, facets),
        from,
        pageSize);
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
}
