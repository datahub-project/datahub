package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatArray;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.VectorRetrievalExplanation;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCapabilities;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDryRender;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.LifecycleStageTypeService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateLadderStopArray;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalAnchor;
import com.linkedin.search.template.SearchTemplateScope;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A {@code combine: vector} rung served through {@link ESSearchDAO#search}. */
public class ESSearchDAOVectorRetrievalTest {
  private static final String VECTOR_URN = "urn:li:searchTemplate:vector";
  private static final String KEYWORD_URN = "urn:li:searchTemplate:keyword";
  private static final String GROUP_URN = "urn:li:searchTemplate:group";
  private static final String SEMANTIC_URN = "urn:li:searchTemplate:semantic";
  private static final String SIMILARITY_URN = "urn:li:searchTemplate:similarity";
  private static final String ANCHOR = "urn:li:document:anchor";
  private static final String HIDDEN_STAGE = "urn:li:lifecycleStageType:draft";
  private static final float[] FIRST_CHUNK = {1f, 0f};
  private static final float[] SECOND_CHUNK = {0f, 1f};
  private static final String MODEL_KEY = "text_embedding_3_small";
  private static final List<String> DOCUMENT = List.of("document");

  private SearchClientShim<?> client;
  private EmbeddingProvider provider;
  private MetricUtils metrics;
  private SearchTemplateFixtures.Harness harness;
  private SemanticSearchConfiguration semanticSearch;
  private ElasticSearchConfiguration elastic;

  @BeforeMethod
  public void setUp() throws IOException {
    client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    // The facet request finds no aggregation; a keyword rung gets these rows
    SearchResponse keyword = keywordResponse(3);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keyword);
    provider = mock(EmbeddingProvider.class);
    when(provider.embed(anyString(), any(), any(), any(Duration.class)))
        .thenReturn(new float[] {0.1f, 0.2f});
    metrics = mock(MetricUtils.class);
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    semanticSearch.setModels(Map.of(MODEL_KEY, new ModelEmbeddingConfig()));
    elastic = harness.elastic();
    elastic.getEntityIndex().setSemanticSearch(semanticSearch);
  }

  @Test
  public void testCandidateCapIsClampedToTheStoreMaximum() throws Exception {
    store(vector(r -> r.setCandidateCap(5_000)));
    knnReturns(hits(0.9, 0.8));

    SearchResult result = search(DOCUMENT, null, 0, 10);

    assertEquals(knnRequest().k(), SearchTemplateVectorRetrieval.MAX_K);
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getK().intValue(), SearchTemplateVectorRetrieval.MAX_K);
    assertEquals(explanation.getClampedCandidateCap().intValue(), 5_000);
    assertVectorOutcome(SearchTemplateVectorRetrieval.CAP_CLAMPED);

    // A cap under the maximum is the window, whatever the page
    clearInvocations(client);
    store(vector(r -> r.setCandidateCap(50)));
    SearchResult capped = search(DOCUMENT, null, 20, 10);
    assertEquals(knnRequest().k(), 50);
    assertFalse(capped.getMetadata().getVectorRetrieval().hasClampedCandidateCap());
  }

  @Test
  public void testWithoutACapTheWindowIsTheSemanticSearchPageWindow() throws Exception {
    store(vector(r -> {}));
    knnReturns(hits(0.9));

    search(DOCUMENT, null, 10, 10);

    // max(size, min(500, ceil((from + size) * 1.2)))
    assertEquals(knnRequest().k(), 24);
    // Clamped to the store's maximum, as a cap is
    assertEquals(
        SearchTemplateVectorRetrieval.window(null, 0, 2_000), SearchTemplateVectorRetrieval.MAX_K);
  }

  @Test
  public void testAHitAtMinScoreIsKept() throws Exception {
    store(vector(r -> {}));
    knnReturns(hits(0.85, 0.8));

    SearchResult result =
        search(context(DOCUMENT, flags -> flags.setMinScore(0.85f)), null, 0, 10, DOCUMENT);

    assertEquals(urns(result), List.of("urn:li:document:d0"));
  }

  @Test
  public void testAnInheritedFloorIsSkippedOnAProfileThatIsNotCosine() throws Exception {
    semanticSearch.setMinSimilarity(0.9f);
    store(vector(r -> {}));
    knnReturns(hits(0.99, 0.6));
    EmbeddingProfile l2 =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "l2",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(l2), semanticSearch, false));

    SearchResult result = search(context(DOCUMENT, f -> {}, coordinator), null, 0, 10, DOCUMENT);

    assertEquals(result.getNumEntities().intValue(), 2);
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertFalse(explanation.hasMinSimilarity());
    assertEquals(
        explanation.getMinSimilaritySkipped(), SearchTemplateVectorRetrieval.PROFILE_NOT_COSINE);
  }

  @Test
  public void testAProfileThisDeploymentCannotEmbedFailsClosed() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    SearchTemplateProperties next = vector(r -> r.setOnFailure("next"));
    next.setLadder(nextOnMiss());
    store(next);
    EmbeddingProfile other =
        new EmbeddingProfile(
            "other_profile",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "cosine",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateCoordinator coordinator =
        coordinator(
            SearchTemplateGates.of(
                new EmbeddingProfileRegistry(List.of(other), Map.of("document", other.id())),
                semanticSearch,
                false));

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> search(context(DOCUMENT, f -> {}, coordinator), null, 0, 10, DOCUMENT));
    verify(client, never()).searchKnn(any(), any());
    assertNoFallback();
  }

  @Test
  public void testAParallelVectorMemberThatFailsClosedFailsTheSearch() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    store(vector(r -> {}));
    SearchTemplateProperties group = keyword();
    group.setLadder(
        new SearchTemplateLadder()
            .setParallel(new UrnArray(UrnUtils.getUrn(VECTOR_URN)))
            .setJoin("any")
            .setNext(UrnUtils.getUrn(KEYWORD_URN)));
    harness.store(GROUP_URN, group);
    when(client.searchKnn(any(), any())).thenThrow(new IOException("kNN unavailable"));

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> search(pinned(context(DOCUMENT, f -> {}), GROUP_URN), null, 0, 10, DOCUMENT));
    assertNoFallback();
  }

  @Test
  public void testTheSearchCacheServesTheSameVectorPageAsTheDao() throws Exception {
    // The store answers with the k nearest of 300 neighbors, so a window's size shows in its total
    List<KnnSearchResponse.Hit> neighbors =
        hits(IntStream.range(0, 300).mapToDouble(i -> 0.99 - i * 0.001).toArray());
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation ->
                new KnnSearchResponse(
                    neighbors.subList(
                        0,
                        Math.min(
                            neighbors.size(),
                            ((KnnSearchRequest) invocation.getArgument(1)).k()))));
    ESSearchDAO dao = dao(null, embedding());
    CachingEntitySearchService cached = cached(dao);

    for (int[] page : new int[][] {{0, 10}, {95, 10}, {60, 10}}) {
      store(page[0] == 60 ? vector(r -> r.setCandidateCap(50)) : vector(r -> {}));
      SearchResult direct =
          search(dao, context(DOCUMENT, f -> {}), null, page[0], page[1], DOCUMENT);
      SearchResult throughCache =
          cached.search(
              context(DOCUMENT, f -> {}),
              DOCUMENT,
              "quarterly revenue",
              null,
              null,
              page[0],
              page[1],
              List.of());
      assertEquals(urns(throughCache), urns(direct), page[0] + "");
      assertEquals(throughCache.getNumEntities(), direct.getNumEntities(), page[0] + "");
      assertEquals(throughCache.getMetadata(), direct.getMetadata(), page[0] + "");
    }
    assertEquals(
        search(dao, context(DOCUMENT, f -> {}), null, 60, 10, DOCUMENT)
            .getMetadata()
            .getVectorRetrieval()
            .getOutcome(),
        SearchTemplateVectorRetrieval.PAST_WINDOW);
  }

  @Test
  public void testAPageServedAfterAVectorFailureIsNotCached() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    SearchTemplateProperties next = vector(r -> r.setOnFailure("next"));
    next.setLadder(nextOnMiss());
    store(next);
    when(client.searchKnn(any(), any())).thenThrow(new IOException("kNN unavailable"));
    ESSearchDAO dao = dao(null, embedding());
    CachingEntitySearchService cached = cached(dao);

    for (int attempt = 0; attempt < 2; attempt++) {
      assertEquals(
          cached
              .search(
                  context(DOCUMENT, f -> {}),
                  DOCUMENT,
                  "quarterly revenue",
                  null,
                  null,
                  0,
                  10,
                  List.of())
              .getEntities()
              .size(),
          3);
    }
    // The second search tried the vector rung again
    verify(client, times(2)).searchKnn(any(), any());
  }

  @Test
  public void testPageTotalAndFacetsCountTheSurvivors() throws Exception {
    store(vector(r -> r.setMinSimilarity(0.5f)));
    // Cosines 0.98, 0.9, 0.8, 0.0, and 0.6: the fourth is below the floor
    knnReturns(hits(0.99, 0.95, 0.9, 0.5, 0.8));

    SearchResult result = search(DOCUMENT, null, 0, 2);

    assertEquals(urns(result), List.of("urn:li:document:d0", "urn:li:document:d1"));
    assertEquals(result.getNumEntities().intValue(), 4);
    // The facets come from one request with no hits over the surviving URNs only
    SearchSourceBuilder facets = searchRequest().source();
    assertEquals(facets.size(), 0);
    assertEquals(
        urnTerms(facets.query()),
        Set.of(
            "urn:li:document:d0",
            "urn:li:document:d1",
            "urn:li:document:d2",
            "urn:li:document:d4"));
    assertFalse(facets.aggregations().getAggregatorFactories().isEmpty());

    // A page past the survivors is empty and keeps their total
    clearInvocations(client);
    SearchResult past = search(DOCUMENT, null, 10, 10);
    assertTrue(past.getEntities().isEmpty());
    assertEquals(past.getNumEntities().intValue(), 4);
    assertEquals(
        past.getMetadata().getVectorRetrieval().getOutcome(),
        SearchTemplateVectorRetrieval.PAST_WINDOW);
    assertNoFallback();
  }

  @Test
  public void testMinSimilarityDropsHitsWithoutRefillingTheWindow() throws Exception {
    store(vector(r -> r.setCandidateCap(4).setMinSimilarity(0.8f)));
    // Engine scores are (1 + cosine) / 2: cosines 0.98, 0.9, 0.8 (at the floor), and 0.7
    knnReturns(hits(0.99, 0.95, 0.9, 0.85));

    SearchResult result = search(DOCUMENT, null, 0, 10);

    assertEquals(
        urns(result), List.of("urn:li:document:d0", "urn:li:document:d1", "urn:li:document:d2"));
    assertEquals(result.getNumEntities().intValue(), 3);
    // One kNN request of the capped window; the dropped hit is not replaced
    verify(client, times(1)).searchKnn(any(), any());
    assertEquals(knnRequest().k(), 4);
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getCandidates().intValue(), 4);
    assertEquals(explanation.getBelowMinSimilarity().intValue(), 1);
    assertEquals(explanation.getMinSimilarity(), 0.8f);
    assertVectorOutcome(SearchTemplateVectorRetrieval.FLOOR_DROPPED);
    assertNoFallback();
  }

  @Test
  public void testTheDeploymentFloorAppliesUnlessTheTemplateSetsOne() throws Exception {
    semanticSearch.setMinSimilarity(0.9f);
    store(vector(r -> {}));
    knnReturns(hits(0.99, 0.95, 0.9));

    assertEquals(search(DOCUMENT, null, 0, 10).getNumEntities().intValue(), 2);

    // The template's floor replaces the deployment's
    store(vector(r -> r.setMinSimilarity(0.5f)));
    assertEquals(search(DOCUMENT, null, 0, 10).getNumEntities().intValue(), 3);
  }

  @Test
  public void testMinScoreStillDropsHitsBelowTheEngineScore() throws Exception {
    store(vector(r -> {}));
    knnReturns(hits(0.99, 0.95, 0.9));

    SearchResult result =
        search(context(DOCUMENT, flags -> flags.setMinScore(0.93f)), null, 0, 10, DOCUMENT);

    assertEquals(urns(result), List.of("urn:li:document:d0", "urn:li:document:d1"));
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getBelowMinScore().intValue(), 1);
    assertEquals(
        explanation.getDeprecations(),
        List.of(SearchTemplateVectorRetrieval.MIN_SCORE_DEPRECATION));
  }

  @Test
  public void testTheCallerFilterChangesWhichNeighborsAreRetrieved() throws Exception {
    store(vector(r -> {}));
    // The store finds neighbors inside the filter it is sent: a filtered request gets other hits
    when(client.searchKnn(any(), argThat(request -> !mentions(request, "urn:li:domain:sales"))))
        .thenReturn(new KnnSearchResponse(hits(0.99, 0.95)));
    when(client.searchKnn(any(), argThat(request -> mentions(request, "urn:li:domain:sales"))))
        .thenReturn(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit(
                        "sales", 0.7, Map.of("urn", "urn:li:document:sales")))));

    SearchResult unfiltered = search(DOCUMENT, null, 0, 10);
    SearchResult filtered = search(DOCUMENT, filter("domains", "urn:li:domain:sales"), 0, 10);

    assertEquals(urns(unfiltered), List.of("urn:li:document:d0", "urn:li:document:d1"));
    assertEquals(urns(filtered), List.of("urn:li:document:sales"));
    assertTrue(filtered.getMetadata().getVectorRetrieval().isFilterInVectorSearch());
  }

  @Test
  public void testTheSameFilterFlagsAndViewerMakeADocumentEligibleOnBothLegs() throws Exception {
    store(vector(r -> {}));
    harness.store(KEYWORD_URN, keyword());
    knnReturns(hits(0.9));
    // A removed filter bypasses the soft-delete clause on both legs
    for (Filter filter :
        java.util.Arrays.asList(
            null, filter("domains", "urn:li:domain:sales"), filter("removed", "true"))) {
      for (boolean includeSoftDeleted : List.of(false, true)) {
        Consumer<com.linkedin.metadata.query.SearchFlags> flags =
            f -> f.setIncludeSoftDeleted(includeSoftDeleted);
        clearInvocations(client);
        search(context(DOCUMENT, flags), filter, 0, 10, DOCUMENT);
        Map<String, Object> vectorLeg = knnRequest().filter().orElseThrow();

        clearInvocations(client);
        search(pinned(context(DOCUMENT, flags), KEYWORD_URN), filter, 0, 10, DOCUMENT);
        QueryBuilder keywordFilter =
            ((BoolQueryBuilder) searchRequest().source().query()).filter().get(0);

        assertEquals(
            vectorLeg, ESUtils.filterMap(keywordFilter), filter + " " + includeSoftDeleted);
      }
    }
  }

  @Test
  public void testAnEmptyWindowIsServedAndIsNotAFallback() throws Exception {
    store(vector(r -> {}));
    knnReturns(List.of());

    SearchResult result = search(DOCUMENT, null, 0, 10);

    assertTrue(result.getEntities().isEmpty());
    assertEquals(result.getNumEntities().intValue(), 0);
    assertVectorOutcome(SearchTemplateVectorRetrieval.EMPTY);
    assertNoFallback();
    // Nothing survived, so there is no facet request
    verify(client, never()).search(any(OperationContext.class), any(), any());
  }

  @Test
  public void testNoEligibleEntityReturnsEmptyWithoutEmbeddingOrKnn() throws Exception {
    store(vector(r -> {}));

    SearchResult result = search(List.of("dataset"), null, 0, 10);

    assertTrue(result.getEntities().isEmpty());
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getOutcome(), SearchTemplateVectorRetrieval.NO_ELIGIBLE_ENTITIES);
    assertEquals(
        explanation.getExcludedEntityTypes(),
        List.of("dataset:" + SearchTemplateGates.ENTITY_NOT_ENABLED));
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
    verify(client, never()).searchKnn(any(), any());
    assertNoFallback();
  }

  @Test
  public void testAMixedRequestServesOnlyTheTypesThatPassTheGates() throws Exception {
    store(vector(r -> {}));
    knnReturns(hits(0.9, 0.8));

    SearchResult result = search(List.of("dataset", "document"), null, 0, 10);

    // Dataset fails the gates: the kNN request does not read it, and no keyword result stands in
    assertEquals(urns(result), List.of("urn:li:document:d0", "urn:li:document:d1"));
    assertEquals(result.getNumEntities().intValue(), 2);
    assertFalse(knnRequest().indexName().contains("dataset"));
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getEntityTypes(), DOCUMENT);
    assertEquals(
        explanation.getExcludedEntityTypes(),
        List.of("dataset:" + SearchTemplateGates.ENTITY_NOT_ENABLED));

    // A type outside the rung's scope is not assigned to it, so it is not reported as dropped
    SearchTemplateProperties scoped = vector(r -> {});
    scoped.setScope(new SearchTemplateScope().setEntityTypes(new StringArray("document")));
    store(scoped);
    VectorRetrievalExplanation partitioned =
        search(List.of("dataset", "document"), null, 0, 10).getMetadata().getVectorRetrieval();
    assertEquals(partitioned.getEntityTypes(), DOCUMENT);
    assertTrue(partitioned.getExcludedEntityTypes().isEmpty());
  }

  @Test
  public void testAFailureFollowsNextOnlyWhenTheRungSaysSo() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    when(client.searchKnn(any(), any())).thenThrow(new IOException("kNN unavailable"));

    // onFailure next: the ladder serves the keyword rung and counts a real fallback
    SearchTemplateProperties next = vector(r -> r.setOnFailure("next"));
    next.setLadder(nextOnMiss());
    store(next);
    SearchResult served = search(DOCUMENT, null, 0, 10);
    assertEquals(served.getEntities().size(), 3);
    verify(metrics).incrementMicrometer(SearchTemplateMetrics.FALLBACK, 1, "kind", "ladder");
    assertVectorOutcome(SearchTemplateVectorRetrieval.FAILED);

    // Unset onFailure is fail: the search fails, even with a next to follow
    clearInvocations(metrics);
    SearchTemplateProperties fail = vector(r -> {});
    fail.setLadder(nextOnMiss());
    store(fail);
    assertThrows(RuntimeException.class, () -> search(DOCUMENT, null, 0, 10));
    assertNoFallback();
  }

  @Test
  public void testAProcessThatCannotEmbedFailsClosed() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    SearchTemplateProperties next = vector(r -> r.setOnFailure("next"));
    next.setLadder(nextOnMiss());
    store(next);

    assertThrows(
        SearchTemplateException.class,
        () -> search(dao(null, null), context(DOCUMENT, f -> {}), null, 0, 10, DOCUMENT));
    assertVectorOutcome(SearchTemplateVectorRetrieval.UNAVAILABLE);
    assertNoFallback();
  }

  @Test
  public void testHybridReadNeitherReranksNorReslicesAVectorPage() throws Exception {
    store(vector(r -> {}));
    knnReturns(hits(0.99, 0.95, 0.9));
    HybridSearchResultReranker reranker = mock(HybridSearchResultReranker.class);
    when(reranker.vectorEntityNames(any(OperationContext.class), any()))
        .thenReturn(Set.of("document"));

    SearchResult result =
        search(dao(reranker, embedding()), context(DOCUMENT, f -> {}), null, 1, 1, DOCUMENT);

    // The window follows the requested page, not the keyword rerank window
    assertEquals(knnRequest().k(), 3);
    assertEquals(urns(result), List.of("urn:li:document:d1"));
    verify(reranker, never()).rerank(any(), any(), any(), any(), any(), anyLong(), any());
  }

  @Test
  public void testMinSimilarityOnAnL2ProfileFailsAtBindBeforeAnyCall() throws Exception {
    store(vector(r -> r.setMinSimilarity(0.5f)));
    EmbeddingProfile l2 =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "l2",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    // This build's own gates, not a test capability set
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(l2), semanticSearch, false));
    OperationContext searching = pinned(context(DOCUMENT, f -> {}, coordinator), VECTOR_URN);

    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(searching, "revenue", false, DOCUMENT));
    SearchTemplateDryRender preview =
        coordinator.preview(
            searching, "revenue", DOCUMENT, false, SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertTrue(
        preview.capabilities().stream()
            .anyMatch(
                decision ->
                    "sections".equals(decision.name())
                        && !decision.accepted()
                        && SearchTemplateGates.MIN_SIMILARITY.equals(decision.detail())));
    assertThrows(
        SearchTemplateException.class,
        () -> search(dao(null, embedding()), searching, null, 0, 10, DOCUMENT));
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
    verify(client, never()).searchKnn(any(), any());
  }

  @Test
  public void testALegacyKnnBesideAVectorRungFailsAtBindBeforeAnyCall() throws Exception {
    store(vector(r -> {}));
    // A keyword root whose field-only knn reranks the first cache batch, and a vector next
    SearchTemplateProperties root = keyword();
    root.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    root.setLadder(onMissNext(VECTOR_URN));
    harness.store(KEYWORD_URN, root);
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    OperationContext searching = pinned(context(DOCUMENT, f -> {}, coordinator), KEYWORD_URN);

    SearchTemplateDryRender preview =
        coordinator.preview(
            searching, "revenue", DOCUMENT, false, SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertTrue(
        preview.capabilities().stream()
            .anyMatch(
                decision ->
                    "sections".equals(decision.name())
                        && !decision.accepted()
                        && SearchTemplateCapabilities.VECTOR_WITH_LEGACY_KNN.equals(
                            decision.detail())));
    assertThrows(
        SearchTemplateException.class,
        () ->
            cached(dao(null, embedding()))
                .search(searching, DOCUMENT, "revenue", null, null, 0, 10, List.of()));
    verify(client, never()).search(any(OperationContext.class), any(), any());
    verify(client, never()).searchKnn(any(), any());
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
  }

  @Test
  public void testMembersARequestAssignsNoTypeNeitherConflictNorRunTheCacheOncePerPage()
      throws Exception {
    // A Document vector member and a Chart legacy knn member, beside a keyword member
    SearchTemplateProperties vectorMember = vector(r -> {});
    vectorMember.setScope(new SearchTemplateScope().setEntityTypes(new StringArray("document")));
    store(vectorMember);
    String legacyUrn = "urn:li:searchTemplate:legacy";
    SearchTemplateProperties legacy = keyword();
    legacy.setApiVersion(2);
    legacy.setScope(new SearchTemplateScope().setEntityTypes(new StringArray("chart")));
    legacy.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store(legacyUrn, legacy);
    harness.store(KEYWORD_URN, keyword());
    SearchTemplateProperties group = keyword();
    group.setLadder(
        new SearchTemplateLadder()
            .setParallel(
                new UrnArray(
                    UrnUtils.getUrn(VECTOR_URN),
                    UrnUtils.getUrn(legacyUrn),
                    UrnUtils.getUrn(KEYWORD_URN)))
            .setJoin("allEmpty"));
    harness.store(GROUP_URN, group);
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    List<String> datasets = List.of("dataset");
    OperationContext searching = pinned(context(datasets, f -> {}, coordinator), GROUP_URN);

    // A Dataset request runs neither member, so the dry render, bind, and the cache all skip them
    assertTrue(
        coordinator
            .preview(
                searching,
                "revenue",
                datasets,
                false,
                SearchClientShim.SearchEngineType.OPENSEARCH_3)
            .capabilities()
            .stream()
            .anyMatch(decision -> "sections".equals(decision.name()) && decision.accepted()));
    assertFalse(
        coordinator
            .bind(searching, "revenue", false, datasets)
            .getSearchContext()
            .getResolvedSearchTemplate()
            .isVectorReachable());
    // A request that runs both members still fails closed
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(searching, "revenue", false, List.of("chart", "document")));
  }

  @Test
  public void testFilterAggregationAndCountFailAScopeThatLeavesOutARequestedType()
      throws Exception {
    // They render the bound template once for the caller's types and do not walk a ladder
    SearchTemplateProperties documents = keyword();
    documents.setApiVersion(2);
    documents.setScope(new SearchTemplateScope().setEntityTypes(new StringArray("document")));
    harness.store(KEYWORD_URN, documents);
    ESSearchDAO dao = dao(null, embedding());
    OperationContext searching =
        pinned(
            context(
                DOCUMENT,
                f -> {},
                coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false))),
            KEYWORD_URN);

    assertThrows(
        SearchTemplateException.class, () -> dao.filter(searching, "dataset", null, null, 0, 10));
    assertThrows(
        SearchTemplateException.class,
        () ->
            dao.buildAggregateByValue(
                searching, List.of("dataset", "document"), "platform", null, 10));
    assertThrows(
        SearchTemplateException.class, () -> dao.buildDocCountRequest(searching, "dataset", null));
    assertThrows(
        SearchTemplateException.class,
        () -> dao.buildFilterRequest(searching, "dataset", null, null, 0, 10));
    verify(client, never()).search(any(OperationContext.class), any(), any());
    // A scope that covers the request renders as before
    assertEquals(dao.filter(searching, "document", null, null, 0, 10).getEntities().size(), 3);
    assertNotNull(dao.buildAggregateByValue(searching, DOCUMENT, "platform", null, 10));
    assertNotNull(dao.buildDocCountRequest(searching, "document", null));
  }

  @Test
  public void testAKeywordRerankRootWithAVectorNextStillReranksThroughTheCache() throws Exception {
    store(vector(r -> {}));
    SearchTemplateProperties root = keyword();
    root.setApiVersion(2);
    root.setRetrieval(new SearchTemplateRetrieval().setCombine("keywordRerank"));
    root.setKnn("{ \"others\": \"keepKeywordPlaces\" }");
    root.setLadder(onMissNext(VECTOR_URN));
    harness.store(KEYWORD_URN, root);
    HybridSearchResultReranker reranker = mock(HybridSearchResultReranker.class);
    // Reverses the window it is given, so reranked rows are told from keyword ones
    when(reranker.rerank(
            any(OperationContext.class), any(), any(), anyList(), any(), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              List<SearchEntity> rows = new ArrayList<>(invocation.getArgument(3));
              Collections.reverse(rows);
              return Optional.of(rows);
            });
    when(reranker.vectorEntityNames(any(OperationContext.class), any()))
        .thenReturn(Set.of("document"));
    when(reranker.providerSucceededSince(anyLong())).thenReturn(true);
    when(reranker.modelEmbeddingKey()).thenReturn(MODEL_KEY);
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, true));

    SearchResult page =
        cached(dao(reranker, embedding()))
            .search(
                pinned(context(DOCUMENT, f -> {}, coordinator), KEYWORD_URN),
                DOCUMENT,
                "quarterly revenue",
                null,
                null,
                0,
                10,
                List.of());

    // The root's three rows stop the ladder, and the search reranks them before the cache
    assertEquals(
        urns(page), List.of("urn:li:document:k2", "urn:li:document:k1", "urn:li:document:k0"));
    verify(reranker, times(1))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong(), any());
    verify(client, never()).searchKnn(any(), any());
  }

  @Test
  public void testALegacyKnnWithoutAVectorRungKeepsTheBatchedRerank() throws Exception {
    SearchTemplateProperties root = keyword();
    root.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    harness.store(KEYWORD_URN, root);
    SearchResponse batch = keywordResponse(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(batch);
    SemanticEntitySearch semantic = mock(SemanticEntitySearch.class);
    when(semantic.search(any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()).setNumEntities(0));
    CachingEntitySearchService cache = cached(dao(null, embedding()));
    cache.setSemanticEntitySearch(semantic);

    SearchResult page =
        cache.search(
            pinned(context(DOCUMENT, f -> {}), KEYWORD_URN),
            DOCUMENT,
            "quarterly revenue",
            null,
            null,
            10,
            10,
            List.of());

    // A page past the first rows is still served from the first batch, whose 100 document rows
    // the knn reranks
    assertEquals(
        urns(page), IntStream.range(10, 20).mapToObj(i -> "urn:li:document:k" + i).toList());
    verify(semantic, times(1)).search(any(), eq(DOCUMENT), any(), any(), any(), eq(0), eq(100));
  }

  @Test
  public void testDocumentExplainFailsClosedOnAVectorRung() throws Exception {
    store(vector(r -> {}));
    // A keyword root that finds nothing hands the search to the vector rung
    SearchTemplateProperties root = keyword();
    root.setLadder(onMissNext(VECTOR_URN));
    harness.store(KEYWORD_URN, root);
    // A parallel group with the vector member, whose next is a plain keyword rung
    String plain = "urn:li:searchTemplate:plain";
    harness.store(plain, keyword());
    SearchTemplateProperties group = keyword();
    group.setLadder(
        new SearchTemplateLadder()
            .setParallel(new UrnArray(UrnUtils.getUrn(VECTOR_URN)))
            .setJoin("any")
            .setNext(UrnUtils.getUrn(plain)));
    harness.store(GROUP_URN, group);
    SearchResponse empty = keywordResponse(0);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(empty);

    for (String first : List.of(VECTOR_URN, KEYWORD_URN, GROUP_URN)) {
      OperationContext explaining = pinned(context(DOCUMENT, f -> {}), first);
      assertThrows(
          SearchTemplateException.class,
          () ->
              dao(null, embedding())
                  .explain(
                      explaining,
                      "quarterly revenue",
                      "urn:li:document:d0",
                      "document",
                      null,
                      null,
                      null,
                      null,
                      10,
                      List.of()));
    }
    verify(client, never()).explain(any(), any(), any());
    verify(client, never()).searchKnn(any(), any());
  }

  @Test
  public void testACountOnlyRequestCountsTheWidestWindow() throws Exception {
    List<KnnSearchResponse.Hit> neighbors =
        hits(IntStream.range(0, 600).mapToDouble(i -> 0.99 - i * 0.0005).toArray());
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation ->
                new KnnSearchResponse(
                    neighbors.subList(
                        0,
                        Math.min(
                            neighbors.size(),
                            ((KnnSearchRequest) invocation.getArgument(1)).k()))));
    ESSearchDAO dao = dao(null, embedding());

    for (Integer cap : java.util.Arrays.asList(null, 50)) {
      store(vector(r -> r.setCandidateCap(cap, SetMode.IGNORE_NULL)));
      // A fresh cache: the test store keeps one modification time, so a cached page would carry
      // over
      CachingEntitySearchService cached = cached(dao);
      int widest = cap == null ? 500 : 50;
      SearchResult direct = search(dao, context(DOCUMENT, f -> {}), null, 0, 0, DOCUMENT);
      SearchResult throughCache =
          cached.search(
              context(DOCUMENT, f -> {}),
              DOCUMENT,
              "quarterly revenue",
              null,
              null,
              0,
              0,
              List.of());
      assertEquals(direct.getNumEntities().intValue(), widest, String.valueOf(cap));
      assertEquals(throughCache.getNumEntities().intValue(), widest, String.valueOf(cap));
    }
  }

  @Test
  public void testAFailClosedMemberCancelsItsParallelSiblings() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    store(vector(r -> {}));
    SearchTemplateProperties group = keyword();
    group.setLadder(
        new SearchTemplateLadder()
            .setParallel(new UrnArray(UrnUtils.getUrn(VECTOR_URN), UrnUtils.getUrn(KEYWORD_URN)))
            .setJoin("any"));
    harness.store(GROUP_URN, group);
    // The keyword sibling is running when the vector member fails, and runs until interrupted
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              started.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
              }
              return keywordResponse(3);
            });
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation -> {
              started.await(5, TimeUnit.SECONDS);
              throw new IOException("kNN unavailable");
            });
    ExecutorService ladder = Executors.newFixedThreadPool(2);
    try {
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(
              elastic,
              null,
              harness.loader,
              ladder,
              SearchTemplateComponentRegistry.empty(),
              SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
      assertThrows(
          SearchTemplateFailClosedException.class,
          () ->
              search(
                  pinned(context(DOCUMENT, f -> {}, coordinator), GROUP_URN),
                  null,
                  0,
                  10,
                  DOCUMENT));
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      ladder.shutdownNow();
    }
  }

  @Test
  public void testAPageAcrossTheBatchSeamRunsOnceThroughTheCache() throws Exception {
    harness.store(KEYWORD_URN, keyword());
    SearchTemplateProperties next = vector(r -> r.setOnFailure("next"));
    next.setLadder(nextOnMiss());
    store(next);
    // The first kNN call answers; a second one would fail over to the keyword rung
    List<KnnSearchResponse.Hit> neighbors =
        hits(IntStream.range(0, 300).mapToDouble(i -> 0.99 - i * 0.001).toArray());
    AtomicInteger calls = new AtomicInteger();
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation -> {
              if (calls.incrementAndGet() > 1) {
                throw new IOException("kNN unavailable");
              }
              return new KnnSearchResponse(
                  neighbors.subList(0, ((KnnSearchRequest) invocation.getArgument(1)).k()));
            });

    SearchResult page =
        cached(dao(null, embedding()))
            .search(
                context(DOCUMENT, f -> {}),
                DOCUMENT,
                "quarterly revenue",
                null,
                null,
                95,
                10,
                List.of());

    // One execution of the vector rung serves the whole page: no keyword rows are mixed in
    assertEquals(calls.get(), 1);
    assertEquals(
        urns(page), IntStream.range(95, 105).mapToObj(i -> "urn:li:document:d" + i).toList());
  }

  @Test
  public void testACountOnlyPageSizeSelectRoutesTheSameThroughTheCache() throws Exception {
    String paged = "urn:li:searchTemplate:paged";
    store(vector(r -> {}));
    // A keyword root that runs only for a page with rows; a count goes to the vector rung
    SearchTemplateProperties root = keyword();
    root.setLadder(
        new SearchTemplateLadder()
            .setSelect(new StringArray(List.of("pageSize")))
            .setNext(UrnUtils.getUrn(VECTOR_URN)));
    harness.store(paged, root);
    List<KnnSearchResponse.Hit> neighbors =
        hits(IntStream.range(0, 600).mapToDouble(i -> 0.99 - i * 0.0005).toArray());
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation ->
                new KnnSearchResponse(
                    neighbors.subList(0, ((KnnSearchRequest) invocation.getArgument(1)).k())));
    ESSearchDAO dao = dao(null, embedding());

    SearchResult direct =
        search(dao, pinned(context(DOCUMENT, f -> {}), paged), null, 0, 0, DOCUMENT);
    SearchResult throughCache =
        cached(dao)
            .search(
                pinned(context(DOCUMENT, f -> {}), paged),
                DOCUMENT,
                "quarterly revenue",
                null,
                null,
                0,
                0,
                List.of());

    assertEquals(direct.getNumEntities().intValue(), 500);
    assertEquals(throughCache.getNumEntities(), direct.getNumEntities());
  }

  @Test
  public void testSimilarityScoresEachNeighborByItsBestChunk() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK, SECOND_CHUNK);
    // Each anchor chunk finds its own neighbors; d1 is nearer the second chunk
    when(client.searchKnn(any(), argThat(request -> queries(request, FIRST_CHUNK))))
        .thenReturn(new KnnSearchResponse(hits(0.95, 0.92)));
    when(client.searchKnn(any(), argThat(request -> queries(request, SECOND_CHUNK))))
        .thenReturn(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit("d1", 0.97, Map.of("urn", "urn:li:document:d1")),
                    new KnnSearchResponse.Hit("d2", 0.91, Map.of("urn", "urn:li:document:d2")))));

    SearchResult result = similar(context(DOCUMENT, f -> {}), DOCUMENT);

    // Every neighbor scores its best chunk; the anchor is not embedded and is not its own neighbor
    assertEquals(
        urns(result), List.of("urn:li:document:d1", "urn:li:document:d0", "urn:li:document:d2"));
    assertEquals(result.getEntities().get(0).getScore(), 0.97);
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
    // Each chunk asks for one more than the window of 500, for the anchor's own place
    ArgumentCaptor<KnnSearchRequest> requests = ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(client, times(2)).searchKnn(any(), requests.capture());
    for (KnnSearchRequest request : requests.getAllValues()) {
      assertEquals(request.k(), 501);
    }
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getOutcome(), SearchTemplateVectorRetrieval.SERVED);
    assertEquals(explanation.getProfile(), MODEL_KEY);
    assertEquals(explanation.getCandidates().intValue(), 3);
    // Bind checks the anchor once, with the budget as its search timeout, and the rung reuses it
    verify(client, times(1))
        .search(
            any(OperationContext.class),
            argThat(
                request ->
                    request != null
                        && request.source().toString().contains(ANCHOR)
                        && request.source().timeout() != null),
            any());
  }

  @Test
  public void testSimilarityLeavesOutTypesItCannotSearchAndStillServes() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));

    SearchResult result = similar(context(DOCUMENT, f -> {}), List.of("document", "dataset"));

    assertEquals(urns(result), List.of("urn:li:document:d0"));
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getEntityTypes(), DOCUMENT);
    assertEquals(
        explanation.getExcludedEntityTypes(),
        List.of("dataset:" + SearchTemplateGates.ENTITY_NOT_ENABLED));

    // A requested type under another profile than the anchor's is left out too
    semanticSearch.setEnabledEntities(Set.of("document", "dataset"));
    EmbeddingProfile datasets =
        new EmbeddingProfile(
            "dataset_profile",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "cosine",
            Set.of("dataset"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateCoordinator twoProfiles =
        coordinator(
            new SearchTemplateGates(
                new EmbeddingProfileRegistry(
                    List.of(cosine(), datasets),
                    Map.of("document", MODEL_KEY, "dataset", datasets.id())),
                semanticSearch,
                false,
                SearchTemplateGates.ADVERTISED,
                Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document", "dataset"))));
    String datasetAnchor = "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)";
    OperationContext searching = context(DOCUMENT, f -> {}, twoProfiles);
    clearInvocations(client, metrics);

    // With no requested type left, an anchor without vectors still fails rather than empties
    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            dao(null, embedding())
                .search(searching, DOCUMENT, datasetAnchor, null, null, 0, 10, List.of()));
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);

    anchorHas(datasetAnchor, datasets.id(), 1L, FIRST_CHUNK);
    SearchResult other =
        dao(null, embedding())
            .search(searching, DOCUMENT, datasetAnchor, null, null, 0, 10, List.of());

    assertTrue(other.getEntities().isEmpty());
    VectorRetrievalExplanation leftOut = other.getMetadata().getVectorRetrieval();
    assertEquals(leftOut.getOutcome(), SearchTemplateVectorRetrieval.NO_ELIGIBLE_ENTITIES);
    assertEquals(
        leftOut.getExcludedEntityTypes(),
        List.of("document:" + SearchTemplateVectorRetrieval.NOT_ANCHOR_PROFILE));
    verify(client, never()).searchKnn(any(), any());
  }

  @Test
  public void testAnAnchorOutsideTheRungsScopeStillSelectsItsProfile() throws Exception {
    // The scope assigns the requested types the rung searches. The anchor is the query source, so
    // its type selects the profile even when the scope leaves that type out.
    SearchTemplateProperties datasetsOnly = similarity(r -> {});
    datasetsOnly.setScope(new SearchTemplateScope().setEntityTypes(new StringArray("dataset")));
    store(datasetsOnly);
    semanticSearch.setEnabledEntities(Set.of("document", "dataset"));
    EmbeddingProfile shared =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            "text-embedding-3-small",
            "cosine",
            Set.of("document", "dataset"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateCoordinator bothTypes =
        coordinator(
            new SearchTemplateGates(
                new EmbeddingProfileRegistry(
                    List.of(shared), Map.of("document", MODEL_KEY, "dataset", MODEL_KEY)),
                semanticSearch,
                false,
                SearchTemplateGates.ADVERTISED,
                Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document", "dataset"))));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));

    SearchResult result =
        similar(context(DOCUMENT, f -> {}, bothTypes), List.of("document", "dataset"));

    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getOutcome(), SearchTemplateVectorRetrieval.SERVED);
    assertEquals(explanation.getProfile(), MODEL_KEY);
    // The requested document type is still outside the scope: not searched, not reported dropped
    assertEquals(explanation.getEntityTypes(), List.of("dataset"));
    assertTrue(explanation.getExcludedEntityTypes().isEmpty());
    String index = knnRequest().indexName();
    assertTrue(index.contains("dataset") && !index.contains("document"), index);
    // Bind resolved the anchor's profile and checked the anchor once, and the rung reused it
    verify(client, times(1))
        .search(
            any(OperationContext.class),
            argThat(
                request ->
                    request != null
                        && request.source().toString().contains(ANCHOR)
                        && request.source().timeout() != null),
            any());
  }

  @Test
  public void testSimilarityDropsNeighborsBelowItsFloorWithoutRefilling() throws Exception {
    // The template's floor replaces the deployment's
    semanticSearch.setMinSimilarity(0.5f);
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    // Cosines 0.9, 0.8 (at the floor), and 0.7
    knnReturns(hits(0.95, 0.9, 0.85));

    SearchResult result = similar(context(DOCUMENT, f -> {}), DOCUMENT);

    assertEquals(urns(result), List.of("urn:li:document:d0", "urn:li:document:d1"));
    assertEquals(result.getNumEntities().intValue(), 2);
    // The window of 500 and one more for the anchor's place; the floor does not refill it
    assertEquals(knnRequest().k(), 501);
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getMinSimilarity(), 0.8f);
    assertEquals(explanation.getBelowMinSimilarity().intValue(), 1);
    assertVectorOutcome(SearchTemplateVectorRetrieval.FLOOR_DROPPED);
  }

  @Test
  public void testAnAnchorWithoutVectorsFailsWithoutAKeywordFallback() throws Exception {
    store(similarity(r -> {}));

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> similar(context(DOCUMENT, f -> {}), DOCUMENT));

    verify(client, never()).searchKnn(any(), any());
    // The anchor check fetches no rows; no search that does ran
    verify(client, never())
        .search(
            any(OperationContext.class),
            argThat(request -> request != null && request.source().size() != 0),
            any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
    assertNoFallback();
  }

  @Test
  public void testAnAnchorTheViewerCannotSeeFailsAtBindWithoutBeingRead() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    OperationContext searching = context(DOCUMENT, f -> {});
    SearchTemplateCoordinator coordinator =
        (SearchTemplateCoordinator) searching.getSearchContext().getSearchTemplateRuntime();
    clearInvocations(harness.retriever);

    try (MockedStatic<ESAccessControlUtil> access =
        mockStatic(ESAccessControlUtil.class, Mockito.CALLS_REAL_METHODS)) {
      access
          .when(() -> ESAccessControlUtil.restrictUrn(any(), eq(UrnUtils.getUrn(ANCHOR))))
          .thenReturn(true);

      assertThrows(
          SearchTemplateFailClosedException.class,
          () -> coordinator.bind(searching, ANCHOR, false, DOCUMENT));
    }

    // Nothing about it is read, and its neighbors would reveal what it is about
    verify(harness.retriever, never())
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(ANCHOR)),
            eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
    verify(client, never()).search(any(OperationContext.class), any(), any());
    verify(client, never()).searchKnn(any(), any());
    // An anchor the viewer may not use is a vector retrieval outcome, not a template failure
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
    verify(metrics, never())
        .incrementMicrometer(
            eq(SearchTemplateMetrics.RENDER_FAILURE), anyDouble(), any(String[].class));
  }

  @Test
  public void testTheCacheKeyHasTheAnchorsVectorStampAndScrollFails() throws Exception {
    store(similarity(r -> {}));
    OperationContext searching = context(DOCUMENT, f -> {});
    SearchTemplateCoordinator coordinator =
        (SearchTemplateCoordinator) searching.getSearchContext().getSearchTemplateRuntime();
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    String before =
        coordinator
            .bind(searching, ANCHOR, false, DOCUMENT)
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent();

    // Embedding the anchor again changes the key, so its old neighbors are not served
    anchorHas(ANCHOR, MODEL_KEY, 2L, FIRST_CHUNK);
    String after =
        coordinator
            .bind(searching, ANCHOR, false, DOCUMENT)
            .getSearchContext()
            .getResolvedSearchTemplate()
            .cacheKeyComponent();

    assertTrue(before.contains(ANCHOR), before);
    assertFalse(before.equals(after), after);
    assertThrows(
        SearchTemplateException.class, () -> coordinator.bind(searching, ANCHOR, true, DOCUMENT));
  }

  /**
   * The key of a similarity rung carries the profile's metric next to the anchor stamp, so the same
   * profile id on another metric does not serve the earlier page.
   */
  @Test
  public void testTheSimilarityCacheKeyMovesWithTheProfileMetric() throws Exception {
    store(similarity(SearchTemplateRetrieval::removeMinSimilarity));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    EmbeddingProfile l2 =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "l2",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    Set<String> keys = new HashSet<>();
    for (EmbeddingProfile profile : List.of(cosine(), l2)) {
      SearchTemplateCoordinator coordinator =
          coordinator(SearchTemplateGates.of(registry(profile), semanticSearch, false));
      keys.add(
          coordinator
              .bind(context(DOCUMENT, f -> {}, coordinator), ANCHOR, false, DOCUMENT)
              .getSearchContext()
              .getResolvedSearchTemplate()
              .cacheKeyComponent());
    }

    assertEquals(keys.size(), 2, keys.toString());
    assertTrue(keys.stream().allMatch(key -> key.contains(ANCHOR)), keys.toString());
    assertTrue(keys.stream().anyMatch(key -> key.contains(MODEL_KEY + "/l2")), keys.toString());
  }

  /**
   * Bind refuses the anchor before the result cache. A viewer who may not see it is refused even
   * under the cache key of a viewer who can, and nothing about it is read for them. The page the
   * other viewer cached is still served to that viewer.
   */
  @Test
  public void testAViewerWhoCannotSeeTheAnchorIsRefusedBeforeTheCache() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    CachingEntitySearchService cached = cached(dao(null, embedding()));
    OperationContext searching = context(DOCUMENT, f -> {});
    assertEquals(
        urns(cached.search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of())),
        List.of("urn:li:document:d0"));
    clearInvocations(client, harness.retriever, metrics);

    try (MockedStatic<ESAccessControlUtil> access =
        mockStatic(ESAccessControlUtil.class, Mockito.CALLS_REAL_METHODS)) {
      access
          .when(() -> ESAccessControlUtil.restrictUrn(any(), eq(UrnUtils.getUrn(ANCHOR))))
          .thenReturn(true);

      assertThrows(
          SearchTemplateFailClosedException.class,
          () -> cached.search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of()));
    }

    verify(harness.retriever, never())
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(ANCHOR)),
            eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);

    clearInvocations(client);
    assertEquals(
        urns(cached.search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of())),
        List.of("urn:li:document:d0"));
    verify(client, never()).searchKnn(any(), any());
  }

  @Test
  public void testAnAnchorSearchWouldLeaveOutFailsAtBindUnlessTheRequestOptsIn() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    LifecycleStageTypeService stages = mock(LifecycleStageTypeService.class);
    when(stages.getHiddenStageUrns(anySet())).thenReturn(Set.of(HIDDEN_STAGE));
    LifecycleStageTypeService previous = ESUtils.getLifecycleStageTypeService();
    ESUtils.setLifecycleStageTypeService(stages);
    try {
      Map<String, Consumer<com.linkedin.metadata.query.SearchFlags>> optIns =
          Map.of(
              ESUtils.REMOVED,
              f -> f.setIncludeSoftDeleted(true),
              HIDDEN_STAGE,
              f -> f.setIncludeHiddenLifecycleStages(true));
      for (Map.Entry<String, Consumer<com.linkedin.metadata.query.SearchFlags>> optIn :
          optIns.entrySet()) {
        // The store holds the anchor soft-deleted, or in a hidden stage, so a request that leaves
        // those out does not find it
        doAnswer(
                invocation ->
                    keywordResponse(
                        invocation
                                .<SearchRequest>getArgument(1)
                                .source()
                                .toString()
                                .contains(optIn.getKey())
                            ? 0
                            : 1))
            .when(client)
            .search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
        OperationContext searching = context(DOCUMENT, f -> {});
        SearchTemplateCoordinator coordinator =
            (SearchTemplateCoordinator) searching.getSearchContext().getSearchTemplateRuntime();
        clearInvocations(client, harness.retriever, metrics);

        // Bind refuses it before reading it
        assertThrows(
            SearchTemplateFailClosedException.class,
            () -> coordinator.bind(searching, ANCHOR, false, DOCUMENT));
        verify(harness.retriever, never())
            .getLatestAspectObject(
                any(),
                eq(UrnUtils.getUrn(ANCHOR)),
                eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
        assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);

        // The flag that shows such an entity in search lets it anchor a search too
        assertEquals(
            urns(similar(context(DOCUMENT, optIn.getValue()), DOCUMENT)),
            List.of("urn:li:document:d0"),
            optIn.getKey());
      }
    } finally {
      ESUtils.setLifecycleStageTypeService(previous);
    }
  }

  /**
   * A similarity rung only a ladder reaches does not fail at bind, since the search may stop before
   * it. Its anchor is still not read for a viewer who may not see it, and the rung fails if it
   * runs.
   */
  @Test
  public void testASimilarityRungOnlyALadderReachesFailsOnlyIfItRuns() throws Exception {
    harness.store(SIMILARITY_URN, similarity(r -> {}));
    SearchTemplateProperties first = keyword();
    first.setLadder(onMissNext(SIMILARITY_URN));
    store(first);
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    OperationContext searching = context(DOCUMENT, f -> {});
    clearInvocations(harness.retriever);

    try (MockedStatic<ESAccessControlUtil> access =
        mockStatic(ESAccessControlUtil.class, Mockito.CALLS_REAL_METHODS)) {
      access
          .when(() -> ESAccessControlUtil.restrictUrn(any(), eq(UrnUtils.getUrn(ANCHOR))))
          .thenReturn(true);

      // The keyword rung serves
      assertEquals(similar(searching, DOCUMENT).getEntities().size(), 3);

      // The keyword rung misses, so the similarity rung runs and fails
      SearchResponse noRows = keywordResponse(0);
      when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
          .thenReturn(noRows);
      assertThrows(SearchTemplateFailClosedException.class, () -> similar(searching, DOCUMENT));
    }

    verify(harness.retriever, never())
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(ANCHOR)),
            eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
  }

  /**
   * A failed anchor check for a similarity rung only a ladder reaches does not fail the search at
   * bind, so the keyword rung serves. That page is not cached, since its key does not say what the
   * anchor was.
   */
  @Test
  public void testALadderOnlyAnchorCheckThatFailsLetsTheKeywordRungServe() throws Exception {
    harness.store(SIMILARITY_URN, similarity(r -> {}));
    SearchTemplateProperties first = keyword();
    first.setLadder(onMissNext(SIMILARITY_URN));
    store(first);
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    SearchResponse rows = keywordResponse(3);
    doAnswer(
            invocation -> {
              if (isAnchorProbe(invocation.getArgument(1))) {
                throw new IOException("anchor lookup unavailable");
              }
              return rows;
            })
        .when(client)
        .search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
    CachingEntitySearchService cached = cached(dao(null, embedding()));
    OperationContext searching = context(DOCUMENT, f -> {});

    for (int i = 0; i < 2; i++) {
      assertEquals(
          cached
              .search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of())
              .getEntities()
              .size(),
          3);
    }

    // Each search ran the keyword rung, so neither page came from the cache
    verify(client, times(2))
        .search(
            any(OperationContext.class),
            argThat(request -> request != null && !isAnchorProbe(request)),
            any());
  }

  /**
   * Bind's answer for a ladder-only anchor stands at the rung. An anchor the search filters left
   * out at bind stays out when the rung runs, even if the store would now keep it, since the cache
   * key was stamped unusable.
   */
  @Test
  public void testALadderOnlyAnchorUnusableAtBindStaysUnusableAtTheRung() throws Exception {
    harness.store(SIMILARITY_URN, similarity(r -> {}));
    SearchTemplateProperties first = keyword();
    first.setLadder(onMissNext(SIMILARITY_URN));
    store(first);
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    // The keyword rung misses. The anchor is left out at bind and kept from then on.
    AtomicInteger probes = new AtomicInteger();
    SearchResponse none = keywordResponse(0);
    SearchResponse kept = keywordResponse(1);
    doAnswer(
            invocation ->
                isAnchorProbe(invocation.getArgument(1)) && probes.getAndIncrement() > 0
                    ? kept
                    : none)
        .when(client)
        .search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> similar(context(DOCUMENT, f -> {}), DOCUMENT));

    assertEquals(probes.get(), 1);
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
  }

  /** A stalled anchor lookup at bind fails the search within the vector retrieval budget. */
  @Test
  public void testAStalledAnchorLookupFailsWithinTheBudget() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    coordinator.setVectorTimeoutMillis(100);
    CountDownLatch release = new CountDownLatch(1);
    SearchResponse kept = keywordResponse(1);
    doAnswer(
            invocation -> {
              release.await(10, TimeUnit.SECONDS);
              return kept;
            })
        .when(client)
        .search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT));
    OperationContext searching = context(DOCUMENT, f -> {}, coordinator);

    long start = System.nanoTime();
    try {
      assertThrows(SearchTemplateFailClosedException.class, () -> similar(searching, DOCUMENT));
    } finally {
      release.countDown();
    }

    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.FAILED);
  }

  @Test
  public void testAStalledRungReadOfTheAnchorFailsWithinTheBudget() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    Aspect stored =
        harness.retriever.getLatestAspectObject(
            null,
            UrnUtils.getUrn(ANCHOR),
            SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME);
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    coordinator.setVectorTimeoutMillis(1_000);
    // Bind reads the anchor for the cache key; the rung's read for its vectors stalls
    AtomicInteger reads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              if (reads.incrementAndGet() > 1) {
                release.await(10, TimeUnit.SECONDS);
              }
              return stored;
            })
        .when(harness.retriever)
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(ANCHOR)),
            eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
    OperationContext searching = context(DOCUMENT, f -> {}, coordinator);

    long start = System.nanoTime();
    try {
      assertThrows(SearchTemplateFailClosedException.class, () -> similar(searching, DOCUMENT));
    } finally {
      release.countDown();
    }

    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
    assertEquals(reads.get(), 2);
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.FAILED);
  }

  @Test
  public void testAFullAnchorPoolFailsTheSearchWithoutCheckingOnTheCaller() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    ThreadPoolExecutor pool = SearchTemplateCoordinator.newAnchorExecutor();
    CountDownLatch release = new CountDownLatch(1);
    int capacity = pool.getMaximumPoolSize() + pool.getQueue().remainingCapacity();
    for (int i = 0; i < capacity; i++) {
      pool.execute(
          () -> {
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }
    coordinator.setAnchorExecutor(pool);
    OperationContext searching = context(DOCUMENT, f -> {}, coordinator);

    try {
      assertThrows(SearchTemplateFailClosedException.class, () -> similar(searching, DOCUMENT));
    } finally {
      release.countDown();
      pool.shutdownNow();
    }

    // The full pool refused the check, so it never ran on the request thread
    verify(client, never())
        .search(any(OperationContext.class), argThat(request -> isAnchorProbe(request)), any());
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.FAILED);
  }

  /**
   * One search keeps four of its anchor's kNN requests on the pool at a time, a quarter of it.
   * Three long anchors searched together hold twelve of the sixteen workers, nothing waits in the
   * queue, and all three serve once the engine answers, as does a later search.
   */
  @Test
  public void testOverlappingLongAnchorsHoldFourWorkersEachAndAllServe() throws Exception {
    store(similarity(r -> {}));
    float[][] pipelineMost = new float[SearchTemplateVectorRetrieval.MAX_ANCHOR_CHUNKS][];
    Arrays.fill(pipelineMost, FIRST_CHUNK);
    anchorHas(ANCHOR, MODEL_KEY, 1L, pipelineMost);
    AtomicInteger started = new AtomicInteger();
    CountDownLatch twelveStarted = new CountDownLatch(12);
    CountDownLatch thirteenthStarted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(client.searchKnn(any(), any()))
        .thenAnswer(
            invocation -> {
              if (started.incrementAndGet() == 13) {
                thirteenthStarted.countDown();
              }
              twelveStarted.countDown();
              release.await(10, TimeUnit.SECONDS);
              return new KnnSearchResponse(hits(0.95));
            });
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    coordinator.setVectorTimeoutMillis(10_000);
    ThreadPoolExecutor pool = SearchTemplateCoordinator.newAnchorExecutor();
    coordinator.setAnchorExecutor(pool);
    OperationContext searching = context(DOCUMENT, f -> {}, coordinator);
    ExecutorService callers = Executors.newFixedThreadPool(3);
    try {
      List<Future<SearchResult>> searches = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        searches.add(callers.submit(() -> similar(searching, DOCUMENT)));
      }
      assertTrue(twelveStarted.await(10, TimeUnit.SECONDS));
      // No search starts a fifth request while its first four run, and the third search queues
      // nothing, so it is neither refused nor waiting
      assertFalse(thirteenthStarted.await(200, TimeUnit.MILLISECONDS));
      assertTrue(pool.getQueue().isEmpty());

      release.countDown();
      for (Future<SearchResult> search : searches) {
        assertEquals(urns(search.get(10, TimeUnit.SECONDS)).size(), 1);
      }
      assertEquals(urns(similar(searching, DOCUMENT)).size(), 1);
    } finally {
      release.countDown();
      callers.shutdownNow();
      pool.shutdownNow();
    }
    verify(client, times(4 * SearchTemplateVectorRetrieval.MAX_ANCHOR_CHUNKS))
        .searchKnn(any(), any());
  }

  @Test
  public void testAnAnchorWithThePipelinesMostChunksServesAndOneOverTheCapFails() throws Exception {
    store(similarity(r -> {}));
    knnReturns(hits(0.95));
    // The default embedding pipeline stores up to 100 chunks for one document
    float[][] pipelineMost = new float[100][];
    Arrays.fill(pipelineMost, FIRST_CHUNK);
    anchorHas(ANCHOR, MODEL_KEY, 1L, pipelineMost);

    assertEquals(urns(similar(context(DOCUMENT, f -> {}), DOCUMENT)).size(), 1);
    verify(client, times(100)).searchKnn(any(), any());

    float[][] overCap = new float[SearchTemplateVectorRetrieval.MAX_ANCHOR_CHUNKS + 1][];
    Arrays.fill(overCap, FIRST_CHUNK);
    anchorHas(ANCHOR, MODEL_KEY, 2L, overCap);
    clearInvocations(client);

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> similar(context(DOCUMENT, f -> {}), DOCUMENT));

    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
  }

  @Test
  public void testExcludeSelfDropsTheAnchorTheStoreFindsWithoutShrinkingTheWindow()
      throws Exception {
    store(similarity(r -> r.setCandidateCap(2)));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    // The anchor is its own nearest neighbor, and nothing in the request filters it out
    chunkFinds(FIRST_CHUNK, neighbor("anchor", 1.0), neighbor("d1", 0.97), neighbor("d2", 0.95));

    SearchResult result = similar(context(DOCUMENT, f -> {}), DOCUMENT);

    // The chunk asks for one more than the window, so the anchor's place goes to d2
    assertEquals(urns(result), List.of("urn:li:document:d1", "urn:li:document:d2"));
    KnnSearchRequest request = knnRequest();
    assertEquals(request.k(), 3);
    assertFalse(request.filter().orElseThrow().toString().contains(ANCHOR));

    // Without excludeSelf the anchor is a neighbor like any other
    store(
        similarity(
            r ->
                r.setCandidateCap(2)
                    .setAnchor(
                        new SearchTemplateRetrievalAnchor()
                            .setUrnParameter("raw")
                            .setExcludeSelf(false))));
    assertEquals(
        urns(similar(context(DOCUMENT, f -> {}), DOCUMENT)), List.of(ANCHOR, "urn:li:document:d1"));
  }

  @Test
  public void testSimilarityKeepsTheTrueTopKWhateverTheChunkOrder() throws Exception {
    store(similarity(r -> r.setCandidateCap(2)));
    float[] thirdChunk = {1f, 1f};
    // d2 and d3 are the two best; d1 leads after the first chunk, and d2's best comes last
    chunkFinds(FIRST_CHUNK, neighbor("d1", 0.93), neighbor("d2", 0.92));
    chunkFinds(SECOND_CHUNK, neighbor("d3", 0.97), neighbor("d1", 0.925));
    chunkFinds(thirdChunk, neighbor("d2", 0.99), neighbor("d4", 0.95));
    List<float[][]> orders =
        List.of(
            new float[][] {FIRST_CHUNK, SECOND_CHUNK, thirdChunk},
            new float[][] {FIRST_CHUNK, thirdChunk, SECOND_CHUNK},
            new float[][] {SECOND_CHUNK, FIRST_CHUNK, thirdChunk},
            new float[][] {SECOND_CHUNK, thirdChunk, FIRST_CHUNK},
            new float[][] {thirdChunk, FIRST_CHUNK, SECOND_CHUNK},
            new float[][] {thirdChunk, SECOND_CHUNK, FIRST_CHUNK});

    for (int i = 0; i < orders.size(); i++) {
      anchorHas(ANCHOR, MODEL_KEY, i, orders.get(i));
      SearchResult result = similar(context(DOCUMENT, f -> {}), DOCUMENT);

      assertEquals(urns(result), List.of("urn:li:document:d2", "urn:li:document:d3"), "order " + i);
      assertEquals(result.getEntities().get(0).getScore(), 0.99);
      assertEquals(result.getEntities().get(1).getScore(), 0.97);
    }
  }

  @Test
  public void testAStalledChunkCallFailsWithinTheBudget() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK, SECOND_CHUNK);
    chunkFinds(FIRST_CHUNK, neighbor("d1", 0.95));
    CountDownLatch reached = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(client.searchKnn(any(), argThat(request -> queries(request, SECOND_CHUNK))))
        .thenAnswer(
            invocation -> {
              reached.countDown();
              release.await(10, TimeUnit.SECONDS);
              return new KnnSearchResponse(List.of(neighbor("d2", 0.95)));
            });
    SearchTemplateCoordinator coordinator =
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false));
    coordinator.setVectorTimeoutMillis(1_000);
    OperationContext searching = context(DOCUMENT, f -> {}, coordinator);

    long start = System.nanoTime();
    try {
      assertThrows(SearchTemplateFailClosedException.class, () -> similar(searching, DOCUMENT));
    } finally {
      release.countDown();
    }

    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
    assertEquals(reached.getCount(), 0L);
    assertVectorOutcome(SearchTemplateVectorRetrieval.FAILED);
  }

  @Test
  public void testSimilarityNeedsSemanticReadButNoEmbeddingProvider() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK);
    knnReturns(hits(0.95));
    OperationContext searching = context(DOCUMENT, f -> {});

    // The stored vectors are the query, so a process that cannot embed still finds neighbors
    SearchResult result =
        dao(null, null).search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of());
    assertEquals(urns(result), List.of("urn:li:document:d0"));
    assertTrue(queries(knnRequest(), FIRST_CHUNK));

    ESSearchDAO withoutSemanticRead =
        new ESSearchDAO(
            false,
            harness.elastic(),
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            null,
            null);
    clearInvocations(client);
    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            withoutSemanticRead.search(searching, DOCUMENT, ANCHOR, null, null, 0, 10, List.of()));
    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.UNAVAILABLE);
  }

  @Test
  public void testNeighborsWithEqualScoresAreOrderedByUrn() throws Exception {
    store(similarity(r -> {}));
    anchorHas(ANCHOR, MODEL_KEY, 1L, FIRST_CHUNK, SECOND_CHUNK);
    // d2 and d1 tie inside one response, and d0 ties with both from the other chunk
    when(client.searchKnn(any(), argThat(request -> queries(request, FIRST_CHUNK))))
        .thenReturn(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit("d2", 0.95, Map.of("urn", "urn:li:document:d2")),
                    new KnnSearchResponse.Hit("d1", 0.95, Map.of("urn", "urn:li:document:d1")))));
    when(client.searchKnn(any(), argThat(request -> queries(request, SECOND_CHUNK))))
        .thenReturn(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit("d0", 0.95, Map.of("urn", "urn:li:document:d0")))));

    assertEquals(
        urns(similar(context(DOCUMENT, f -> {}), DOCUMENT)),
        List.of("urn:li:document:d0", "urn:li:document:d1", "urn:li:document:d2"));

    // An anchor with one chunk orders its ties the same way
    anchorHas(ANCHOR, MODEL_KEY, 2L, FIRST_CHUNK);
    assertEquals(
        urns(similar(context(DOCUMENT, f -> {}), DOCUMENT)),
        List.of("urn:li:document:d1", "urn:li:document:d2"));
  }

  @Test
  public void testAUrnOfAnEntityTypeWithoutAProfileFailsAsNoAnchor() throws Exception {
    store(similarity(r -> {}));
    OperationContext searching = context(DOCUMENT, f -> {});

    // The route sends any entity URN here. A type with no embedding profile, registered or not,
    // cannot anchor a search.
    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            dao(null, embedding())
                .search(searching, DOCUMENT, "urn:li:notAnEntity:x", null, null, 0, 10, List.of()));

    verify(client, never()).searchKnn(any(), any());
    assertVectorOutcome(SearchTemplateVectorRetrieval.NO_ANCHOR);
  }

  /**
   * The build before this one runs vector retrieval but not similarity. The shipped semantic
   * template has no route to similarity yet, so that build still embeds a URN query on it.
   */
  @Test
  public void testAnEarlierBuildEmbedsAUrnOnTheShippedSemantic() throws Exception {
    SearchTemplateFixtures.readShipped().forEach(harness::store);
    knnReturns(hits(0.95));
    SearchTemplateCoordinator earlier =
        coordinator(
            new SearchTemplateGates(
                registry(cosine()),
                semanticSearch,
                false,
                Set.of(SearchTemplateGates.DOCUMENT_RERANK, SearchTemplateGates.VECTOR_RETRIEVAL),
                SearchTemplateGates.ADVERTISED_ENTITY_TYPES));
    OperationContext searching = pinned(context(DOCUMENT, f -> {}, earlier), SEMANTIC_URN);

    SearchResult result = similar(searching, DOCUMENT);

    assertEquals(urns(result), List.of("urn:li:document:d0"));
    verify(provider).embed(eq(ANCHOR), any(), any(), any(Duration.class));
  }

  private SearchResult similar(OperationContext searching, List<String> entities) {
    return dao(null, embedding()).search(searching, entities, ANCHOR, null, null, 0, 10, List.of());
  }

  /** The anchor's semanticContent: one chunk per vector under the profile. */
  private void anchorHas(String anchor, String profile, long generatedAt, float[]... chunks) {
    EmbeddingChunkArray stored = new EmbeddingChunkArray();
    for (int i = 0; i < chunks.length; i++) {
      FloatArray vector = new FloatArray();
      for (float value : chunks[i]) {
        vector.add(value);
      }
      stored.add(new EmbeddingChunk().setPosition(i).setVector(vector));
    }
    SemanticContent content =
        new SemanticContent()
            .setEmbeddings(
                new EmbeddingModelDataMap(
                    Map.of(
                        profile,
                        new EmbeddingModelData()
                            .setModelVersion("text-embedding-3-small")
                            .setGeneratedAt(generatedAt)
                            .setTotalChunks(chunks.length)
                            .setChunks(stored))));
    doReturn(new Aspect(content.data()))
        .when(harness.retriever)
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(anchor)),
            eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME));
  }

  /** The size-0 request that checks the anchor against the default search filters. */
  private static boolean isAnchorProbe(SearchRequest request) {
    return request != null
        && request.source().size() == 0
        && request.source().toString().contains(ANCHOR);
  }

  private void chunkFinds(float[] chunk, KnnSearchResponse.Hit... found) throws IOException {
    when(client.searchKnn(any(), argThat(request -> queries(request, chunk))))
        .thenReturn(new KnnSearchResponse(List.of(found)));
  }

  private static KnnSearchResponse.Hit neighbor(String id, double score) {
    return new KnnSearchResponse.Hit(id, score, Map.of("urn", "urn:li:document:" + id));
  }

  private static boolean queries(KnnSearchRequest request, float[] vector) {
    return request != null && Arrays.equals(request.queryVector(), vector);
  }

  /** The shipped similarity shape: the anchor's neighbors, 500 at most, above a cosine of 0.8. */
  private static SearchTemplateProperties similarity(Consumer<SearchTemplateRetrieval> setup) {
    return vector(
        r -> {
          r.setQuerySource("anchorEntity")
              .setCandidateCap(500)
              .setMinSimilarity(0.8f)
              .setAnchor(
                  new SearchTemplateRetrievalAnchor().setUrnParameter("raw").setExcludeSelf(true));
          setup.accept(r);
        });
  }

  private CachingEntitySearchService cached(ESSearchDAO dao) {
    EntitySearchService entitySearch = mock(EntitySearchService.class);
    when(entitySearch.getSearchServiceConfig()).thenReturn(TEST_SEARCH_SERVICE_CONFIG);
    when(entitySearch.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenAnswer(
            invocation ->
                dao.search(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4),
                    invocation.getArgument(5),
                    invocation.getArgument(6),
                    invocation.getArgument(7)));
    // One cache across a test's searches, so a page cached for one search must not serve another
    return new CachingEntitySearchService(new ConcurrentMapCacheManager(), entitySearch, 100, true);
  }

  private void store(SearchTemplateProperties template) {
    harness.store(VECTOR_URN, template);
  }

  private void knnReturns(List<KnnSearchResponse.Hit> hits) throws IOException {
    when(client.searchKnn(any(), any())).thenReturn(new KnnSearchResponse(hits));
  }

  private SearchResult search(List<String> entities, Filter filter, int from, int size)
      throws Exception {
    return search(context(entities, f -> {}), filter, from, size, entities);
  }

  private SearchResult search(
      OperationContext searching, Filter filter, int from, int size, List<String> entities)
      throws Exception {
    return search(dao(null, embedding()), searching, filter, from, size, entities);
  }

  private static SearchResult search(
      ESSearchDAO dao,
      OperationContext searching,
      Filter filter,
      int from,
      int size,
      List<String> entities) {
    return dao.search(
        searching, entities, "quarterly revenue", filter, null, from, size, List.of());
  }

  /** A fulltext search on OpenSearch 3 with explain on and the vector rung pinned. */
  private OperationContext context(
      List<String> entities, Consumer<com.linkedin.metadata.query.SearchFlags> flags)
      throws Exception {
    return context(
        entities,
        flags,
        coordinator(SearchTemplateGates.of(registry(cosine()), semanticSearch, false)));
  }

  private OperationContext context(
      List<String> entities,
      Consumer<com.linkedin.metadata.query.SearchFlags> flags,
      SearchTemplateCoordinator coordinator)
      throws Exception {
    OperationContext base = harness.opContext;
    OperationContext measured =
        base.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(metrics).build())
            .build(base.getSessionActorContext(), false);
    return pinned(
        TestOperationContexts.withFixedSearchClient(measured, client)
            .withSearchTemplateRuntime(coordinator)
            .withSearchFlags(
                f -> {
                  f.setFulltext(true).setIncludeExplain(true);
                  flags.accept(f);
                  return f;
                }),
        VECTOR_URN);
  }

  private static OperationContext pinned(OperationContext searching, String urn) {
    return searching.withSearchFlags(f -> f.setSearchTemplate(UrnUtils.getUrn(urn)));
  }

  private SearchTemplateCoordinator coordinator(SearchTemplateGates gates) {
    return new SearchTemplateCoordinator(
        elastic,
        null,
        harness.loader,
        ForkJoinPool.commonPool(),
        SearchTemplateComponentRegistry.empty(),
        gates);
  }

  private ESSearchDAO dao(
      HybridSearchResultReranker reranker, HybridQueryEmbeddingService embedding) {
    return new ESSearchDAO(
        false,
        elastic,
        null,
        QueryFilterRewriteChain.EMPTY,
        false,
        TEST_SEARCH_SERVICE_CONFIG,
        new Sha256UrnEntityDocumentIdHasher(),
        reranker,
        embedding);
  }

  private HybridQueryEmbeddingService embedding() {
    return new HybridQueryEmbeddingService(provider, "text-embedding-3-small", MODEL_KEY, 0);
  }

  private static EmbeddingProfile cosine() {
    return new EmbeddingProfile(
        MODEL_KEY,
        EmbeddingProfile.TEXT_CHUNKS,
        "text-embedding-3-small",
        "cosine",
        Set.of("document"),
        Set.of(EmbeddingProfileRegistry.OPENSEARCH));
  }

  private static EmbeddingProfileRegistry registry(EmbeddingProfile document) {
    return new EmbeddingProfileRegistry(List.of(document), Map.of("document", MODEL_KEY));
  }

  private static SearchTemplateProperties vector(Consumer<SearchTemplateRetrieval> setup) {
    SearchTemplateProperties template = keyword();
    template.setName("Vector");
    template.setApiVersion(2);
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval().setCombine("vector");
    setup.accept(retrieval);
    template.setRetrieval(retrieval);
    return template;
  }

  private static SearchTemplateProperties keyword() {
    SearchTemplateProperties template = new SearchTemplateProperties();
    template.setName("Keyword");
    template.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    template.setEditable(true);
    template.setQuery("{ \"match_all\": {} }");
    return template;
  }

  private static SearchTemplateLadder onMissNext(String urn) {
    return new SearchTemplateLadder()
        .setStop(new SearchTemplateLadderStopArray(new SearchTemplateLadderStop().setMinHits(1)))
        .setNext(UrnUtils.getUrn(urn));
  }

  private static SearchTemplateLadder nextOnMiss() {
    return onMissNext(KEYWORD_URN);
  }

  private static Filter filter(String field, String value) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            CriterionUtils.buildCriterion(field, Condition.EQUAL, value)))));
  }

  /** Hits d0, d1, ... with these engine scores. */
  private static List<KnnSearchResponse.Hit> hits(double... scores) {
    return IntStream.range(0, scores.length)
        .mapToObj(
            i ->
                new KnnSearchResponse.Hit(
                    "d" + i, scores[i], Map.of("urn", "urn:li:document:d" + i)))
        .collect(Collectors.toList());
  }

  private static SearchResponse keywordResponse(int count) {
    SearchHit[] hits =
        IntStream.range(0, count)
            .mapToObj(
                i -> {
                  SearchHit hit = new SearchHit(i, "k" + i, Map.of(), Map.of());
                  hit.sourceRef(new BytesArray("{\"urn\":\"urn:li:document:k" + i + "\"}"));
                  hit.score(count - i);
                  return hit;
                })
            .toArray(SearchHit[]::new);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(new SearchHits(hits, new TotalHits(count, TotalHits.Relation.EQUAL_TO), count));
    return response;
  }

  private KnnSearchRequest knnRequest() throws IOException {
    ArgumentCaptor<KnnSearchRequest> request = ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(client, times(1)).searchKnn(any(), request.capture());
    return request.getValue();
  }

  private SearchRequest searchRequest() throws IOException {
    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, times(1))
        .search(any(OperationContext.class), request.capture(), eq(RequestOptions.DEFAULT));
    return request.getValue();
  }

  private static Set<String> urnTerms(QueryBuilder query) {
    return ((BoolQueryBuilder) query)
        .filter().stream()
            .filter(TermsQueryBuilder.class::isInstance)
            .map(TermsQueryBuilder.class::cast)
            .filter(terms -> "urn".equals(terms.fieldName()))
            .flatMap(terms -> terms.values().stream())
            .map(String::valueOf)
            .collect(Collectors.toSet());
  }

  private static boolean mentions(KnnSearchRequest request, String value) {
    return request != null && request.filter().map(Object::toString).orElse("").contains(value);
  }

  private static List<String> urns(SearchResult result) {
    return result.getEntities().stream()
        .map(SearchEntity::getEntity)
        .map(Object::toString)
        .collect(Collectors.toList());
  }

  private void assertVectorOutcome(String outcome) {
    verify(metrics)
        .incrementMicrometer(SearchTemplateMetrics.VECTOR_RETRIEVAL, 1, "outcome", outcome);
  }

  private void assertNoFallback() {
    verify(metrics, never())
        .incrementMicrometer(eq(SearchTemplateMetrics.FALLBACK), anyDouble(), any(String[].class));
  }
}
