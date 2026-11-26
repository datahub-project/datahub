package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.VectorStoreConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.VectorRetrievalExplanation;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.VectorStore;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalAnchor;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A {@code combine: vector} rung served by an external vector store, through a fake adapter. */
public class ESSearchDAOVectorStoreTest {
  private static final String VECTOR_URN = "urn:li:searchTemplate:vector";
  private static final String MODEL_KEY = "text_embedding_3_small";
  private static final String STORE = "fake";
  private static final List<String> DOCUMENT = List.of("document");
  private static final Urn D0 = UrnUtils.getUrn("urn:li:document:d0");
  private static final Urn D1 = UrnUtils.getUrn("urn:li:document:d1");
  private static final Urn D2 = UrnUtils.getUrn("urn:li:document:d2");
  private static final Urn MASKED = UrnUtils.getUrn("urn:li:restricted:masked");
  private static final Urn ANCHOR = UrnUtils.getUrn("urn:li:document:anchor");
  private static final float[] FIRST_CHUNK = {1f, 0f};
  private static final float[] SECOND_CHUNK = {0f, 1f};

  private SearchClientShim<?> client;
  private EmbeddingProvider provider;
  private MetricUtils metrics;
  private SearchTemplateFixtures.Harness harness;
  private ElasticSearchConfiguration elastic;
  private VectorStoreConfiguration storeConfig;
  private FakeVectorStore fake;
  private ESSearchDAO dao;
  // The systems the document profile lists
  private Set<String> profileSystems;
  // The time a similarity search has, from bind, for its anchor check, anchor read and searches
  private long vectorTimeoutMillis;

  @BeforeMethod
  public void setUp() throws IOException {
    client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    provider = mock(EmbeddingProvider.class);
    when(provider.embed(anyString(), any(), any(), any(Duration.class)))
        .thenReturn(new float[] {0.1f, 0.2f});
    metrics = mock(MetricUtils.class);
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    storeConfig = new VectorStoreConfiguration();
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    semanticSearch.setModels(Map.of(MODEL_KEY, new ModelEmbeddingConfig()));
    semanticSearch.setStores(Map.of(STORE, storeConfig));
    elastic = harness.elastic();
    elastic.getEntityIndex().setSemanticSearch(semanticSearch);
    harness.store(VECTOR_URN, vector(r -> {}));
    profileSystems = Set.of(STORE);
    vectorTimeoutMillis = 2_000;
    fake = new FakeVectorStore();
    fake.hits =
        List.of(
            new VectorStore.Hit(D0, 0.9),
            new VectorStore.Hit(D1, 0.8),
            new VectorStore.Hit(D2, 0.7));
    dao =
        new ESSearchDAO(
            false,
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            null,
            new HybridQueryEmbeddingService(provider, "text-embedding-3-small", MODEL_KEY, 0));
    dao.setVectorStores(Map.of(STORE, fake));
  }

  @Test
  public void testHydrationDropsHitsTheDataHubIndexDoesNotReturn() throws Exception {
    // d1 is deleted or hidden, so the DataHub index does not return it inside the system filters
    hydrationReturns(D0, D2);

    SearchResult result = search(null, f -> {});

    assertEquals(urns(result), List.of(D0, D2));
    assertEquals(result.getNumEntities().intValue(), 2);
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getStore(), STORE);
    assertEquals(explanation.getCandidates().intValue(), 3);
    assertEquals(explanation.getRemovedByHydration().intValue(), 1);
    assertTrue(explanation.hasAdapterLatencyMillis());
    assertFalse(explanation.isFilterInVectorSearch());
    // One request: a row per hit, by URN, inside the system filters, which drop soft-deleted
    // entities, and the facets of the hits that survive
    SearchSourceBuilder hydration = hydrationRequest().source();
    assertEquals(hydration.size(), 3);
    assertFalse(hydration.aggregations().getAggregatorFactories().isEmpty());
    String query = hydration.query().toString();
    for (Urn urn : List.of(D0, D1, D2)) {
      assertTrue(query.contains(urn.toString()), query);
    }
    assertTrue(query.contains("\"removed\""), query);
  }

  @Test
  public void testAnEntityTheStoreReturnsPerChunkIsOneHitAtItsBestCosine() throws Exception {
    hydrationReturns(D0, D1);
    fake.hits =
        List.of(
            new VectorStore.Hit(D0, 0.9),
            new VectorStore.Hit(D1, 0.8),
            new VectorStore.Hit(D0, 0.7));

    SearchResult result = search(null, f -> {});

    assertEquals(urns(result), List.of(D0, D1));
    assertEquals(result.getNumEntities().intValue(), 2);
    assertEquals(result.getMetadata().getVectorRetrieval().getCandidates().intValue(), 3);
    // One row per entity in the hydration request, so the facets count d0 once
    SearchSourceBuilder hydration = hydrationRequest().source();
    assertEquals(hydration.size(), 2);
    assertEquals(urnTerms(hydration), List.of(D0.toString(), D1.toString()));

    // The order follows each entity's best cosine, not where the store first returned it
    fake.hits =
        List.of(
            new VectorStore.Hit(D1, 0.8),
            new VectorStore.Hit(D0, 0.7),
            new VectorStore.Hit(D0, 0.95));
    assertEquals(urns(search(null, f -> {})), List.of(D0, D1));

    // The floor sees each entity's best cosine
    harness.store(VECTOR_URN, vector(r -> r.setMinSimilarity(0.85f)));
    fake.hits =
        List.of(
            new VectorStore.Hit(D0, 0.7),
            new VectorStore.Hit(D1, 0.8),
            new VectorStore.Hit(D0, 0.9));
    SearchResult floored = search(null, f -> {});
    assertEquals(urns(floored), List.of(D0));
    assertEquals(floored.getMetadata().getVectorRetrieval().getBelowMinSimilarity().intValue(), 1);
  }

  @Test
  public void testHydrationRemovesHitsTheViewerCannotSee() throws Exception {
    // The DataHub index holds all three, so only authorization can remove the denied one
    hydrationReturns(D0, D1, D2);

    try (MockedStatic<EntityAuthorizationUtils> auth = denying(D1)) {
      SearchResult result = search(viewer(), null, f -> {});

      assertEquals(urns(result), List.of(D0, D2));
      assertEquals(result.getNumEntities().intValue(), 2);
      assertEquals(result.getMetadata().getVectorRetrieval().getRemovedByHydration().intValue(), 1);
      // The hydration request, which also counts the facets, never sees the denied entity
      String query = hydrationRequest().source().query().toString();
      assertTrue(query.contains(D0.toString()), query);
      assertFalse(query.contains(D1.toString()), query);
    }
  }

  @Test
  public void testIncludeRestrictedMasksAHitTheViewerCannotSee() throws Exception {
    hydrationReturns(D0, D1, D2);

    try (MockedStatic<EntityAuthorizationUtils> auth = denying(D1)) {
      // As on the keyword leg, includeRestricted keeps the row and masks the entity
      SearchResult result = search(viewer(), null, f -> f.setIncludeRestricted(true));

      assertEquals(urns(result), List.of(D0, MASKED, D2));
      assertEquals(result.getNumEntities().intValue(), 3);
    }
  }

  @Test
  public void testAStoreWithoutPreFilterFailsClosedOnACallerFilterButNotOnSystemDefaults()
      throws Exception {
    hydrationReturns(D0, D1, D2);
    // onFailure next does not apply: the store cannot run this request at all
    harness.store(VECTOR_URN, vector(r -> r.setOnFailure("next")));
    Filter caller = filter("domains", "urn:li:domain:sales");

    assertThrows(SearchTemplateFailClosedException.class, () -> search(caller, f -> {}));
    assertTrue(fake.requests.isEmpty());
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
    verify(metrics)
        .incrementMicrometer(
            SearchTemplateMetrics.VECTOR_RETRIEVAL,
            1,
            "outcome",
            SearchTemplateVectorRetrieval.NO_PRE_FILTER);

    // No caller filter: hydration applies the system defaults; the store is sent no filter
    SearchResult unfiltered = search(null, f -> {});
    assertEquals(unfiltered.getNumEntities().intValue(), 3);
    assertNull(fake.requests.get(0).filter());

    // A Rest.li or OpenAPI caller cannot mark its filter, so a filter equal to the
    // published-document default is still the caller's
    Filter documentDefault = documentDefault();
    assertThrows(SearchTemplateFailClosedException.class, () -> search(documentDefault, f -> {}));
    // A GraphQL resolver records that the caller set no filter before it added that default
    search(harness.opContext.withCallerFiltered(false), documentDefault, f -> {});
    assertNull(fake.requests.get(1).filter());
    // ... or that the caller set one
    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            search(
                harness.opContext.withCallerFiltered(true),
                FilterUtils.combineFilters(caller, documentDefault),
                f -> {}));
    assertEquals(fake.requests.size(), 2);

    // A store with preFilter retrieves inside the caller's filter
    storeConfig.setCapabilities(Set.of(VectorStore.PRE_FILTER));
    SearchResult filtered = search(caller, f -> {});
    assertTrue(fake.requests.get(2).filter().toString().contains("urn:li:domain:sales"));
    assertTrue(filtered.getMetadata().getVectorRetrieval().isFilterInVectorSearch());
  }

  @Test
  public void testAStoreNamedLikeTheEngineDoesNotShadowTheDataHubIndex() throws Exception {
    hydrationReturns(D0);
    elastic
        .getEntityIndex()
        .getSemanticSearch()
        .setStores(
            Map.of(
                EmbeddingProfileRegistry.OPENSEARCH,
                new VectorStoreConfiguration(),
                STORE,
                storeConfig));
    profileSystems = Set.of(EmbeddingProfileRegistry.OPENSEARCH);
    when(client.searchKnn(any(), any()))
        .thenReturn(
            new KnnSearchResponse(
                List.of(new KnnSearchResponse.Hit("d0", 0.9, Map.of("urn", D0.toString())))));

    SearchResult result = search(null, f -> {});

    assertEquals(urns(result), List.of(D0));
    assertEquals(
        result.getMetadata().getVectorRetrieval().getStore(), EmbeddingProfileRegistry.OPENSEARCH);
    verify(client, times(1)).searchKnn(any(), any());
    assertTrue(fake.requests.isEmpty());
  }

  @Test
  public void testAStoreServesOnAnOpenSearchTheDataHubIndexCannotSearch() throws Exception {
    // OpenSearch 3.5 is the floor of the DataHub index's kNN filter; a store runs its own search
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(client.getEngineVersion()).thenReturn("2.19.3");
    hydrationReturns(D0, D1, D2);

    assertEquals(urns(search(null, f -> {})), List.of(D0, D1, D2));
    assertEquals(fake.requests.size(), 1);
  }

  @Test
  public void testOfTwoStoresServingTheProfileTheFirstByNameServes() throws Exception {
    hydrationReturns(D0);
    FakeVectorStore declaredFirst = new FakeVectorStore();
    Map<String, VectorStoreConfiguration> stores = new LinkedHashMap<>();
    stores.put("zeta", new VectorStoreConfiguration());
    stores.put("alpha", storeConfig);
    elastic.getEntityIndex().getSemanticSearch().setStores(stores);
    dao.setVectorStores(Map.of("zeta", declaredFirst, "alpha", fake));
    profileSystems = Set.of("zeta", "alpha");

    SearchResult result = search(null, f -> {});

    // The configuration declares zeta first, but alpha sorts first by name, so alpha serves
    assertEquals(result.getMetadata().getVectorRetrieval().getStore(), "alpha");
    assertEquals(fake.requests.size(), 1);
    assertTrue(declaredFirst.requests.isEmpty());
  }

  @Test
  public void testAStoreWithoutFindByIdCannotServeSimilarity() throws Exception {
    harness.store(VECTOR_URN, similarity());
    fake.anchor = List.of(FIRST_CHUNK);
    // Bind finds the anchor usable
    hydrationReturns(D0);

    assertThrows(SearchTemplateFailClosedException.class, this::similar);

    // The store is asked neither for the anchor nor for its neighbors
    assertEquals(fake.findByIdCalls.get(), 0);
    assertTrue(fake.requests.isEmpty());
    verify(metrics)
        .incrementMicrometer(
            SearchTemplateMetrics.VECTOR_RETRIEVAL,
            1,
            "outcome",
            SearchTemplateVectorRetrieval.UNAVAILABLE);
  }

  @Test
  public void testSimilarityOnAStoreFindsTheAnchorAndHydratesItsNeighbors() throws Exception {
    storeConfig.setCapabilities(Set.of(VectorStore.FIND_BY_ID));
    harness.store(VECTOR_URN, similarity());
    fake.anchor = List.of(FIRST_CHUNK, SECOND_CHUNK);
    // Each chunk finds the anchor first, and d0 once per chunk it matches
    fake.hits =
        List.of(
            new VectorStore.Hit(ANCHOR, 1.0),
            new VectorStore.Hit(D1, 0.8),
            new VectorStore.Hit(D0, 0.6),
            new VectorStore.Hit(D0, 0.9));
    hydrationReturns(D0, D1);

    SearchResult result = similar();

    // Each neighbor at its best cosine, and the anchor is not its own neighbor
    assertEquals(urns(result), List.of(D0, D1));
    assertEquals(result.getEntities().get(0).getScore(), 0.95, 1e-9);
    verify(provider, never()).embed(anyString(), any(), any(), any(Duration.class));
    // The store returns the anchor's vectors and is searched once per chunk, each search one
    // longer than the window of ten for the anchor's place
    assertEquals(fake.findByIdCalls.get(), 1);
    assertEquals(fake.requests.size(), 2);
    for (VectorStore.Request request : fake.requests) {
      assertEquals(request.k(), 11);
      assertEquals(request.collection(), MODEL_KEY);
    }
    VectorRetrievalExplanation explanation = result.getMetadata().getVectorRetrieval();
    assertEquals(explanation.getStore(), STORE);
    assertEquals(explanation.getCandidates().intValue(), 2);
    assertTrue(explanation.hasAdapterLatencyMillis());
    // Bind's anchor check, then one hydration request for the neighbors, without the anchor
    ArgumentCaptor<SearchRequest> requests = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, times(2))
        .search(any(OperationContext.class), requests.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(
        urnTerms(requests.getAllValues().get(1).source()), List.of(D0.toString(), D1.toString()));
  }

  /**
   * Bind checks the anchor on the store serving the rung, so a viewer who cannot use the anchor is
   * refused before the result cache, as on the DataHub index.
   */
  @Test
  public void testAViewerWhoCannotUseTheAnchorIsRefusedBeforeTheCache() throws Exception {
    storeConfig.setCapabilities(Set.of(VectorStore.FIND_BY_ID));
    harness.store(VECTOR_URN, similarity());
    fake.anchor = List.of(FIRST_CHUNK);
    hydrationReturns(D0);
    CachingEntitySearchService cached = cached();
    OperationContext searching = context(harness.opContext, f -> {});
    assertEquals(
        urns(
            cached.search(
                searching, DOCUMENT, ANCHOR.toString(), null, List.of(), 0, 10, List.of())),
        List.of(D0));

    try (MockedStatic<ESAccessControlUtil> access =
        mockStatic(ESAccessControlUtil.class, Mockito.CALLS_REAL_METHODS)) {
      access.when(() -> ESAccessControlUtil.restrictUrn(any(), eq(ANCHOR))).thenReturn(true);

      assertThrows(
          SearchTemplateFailClosedException.class,
          () ->
              cached.search(
                  searching, DOCUMENT, ANCHOR.toString(), null, List.of(), 0, 10, List.of()));
    }

    assertEquals(fake.findByIdCalls.get(), 1);
    assertEquals(fake.requests.size(), 1);
  }

  @Test
  public void testAStoreThatKeepsFailingIsPausedWithoutBeingCalled() throws Exception {
    fake.failure = new IOException("store unavailable");

    for (int i = 0; i < 4; i++) {
      assertThrows(SearchTemplateFailClosedException.class, () -> search(null, f -> {}));
    }

    // The fourth search fails at the breaker
    assertEquals(fake.requests.size(), 3);
  }

  @Test
  public void testABlockingStoreFailsTheRungAtItsDeadline() throws Exception {
    storeConfig.setTimeoutMillis(50);
    fake.block = true;

    long startNanos = System.nanoTime();
    assertThrows(SearchTemplateFailClosedException.class, () -> search(null, f -> {}));

    // The adapter blocks for 20 s; the rung fails at the deadline and cancels the call
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) < 10_000);
    assertTrue(fake.interrupted.await(10, TimeUnit.SECONDS));
    assertEquals(fake.timeouts.get(0), Duration.ofMillis(50));
    verify(client, never())
        .search(any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // A timeout counts toward the breaker: after three, the store is not called
    for (int i = 0; i < 3; i++) {
      assertThrows(SearchTemplateFailClosedException.class, () -> search(null, f -> {}));
    }
    assertEquals(fake.requests.size(), 3);
  }

  /**
   * A similarity search that runs out of its own time cuts the store's call short. That says
   * nothing about the store, so it does not count toward the breaker.
   */
  @Test
  public void testSimilarityCallsTheSearchDeadlineCutsShortDoNotPauseTheStore() throws Exception {
    storeConfig.setCapabilities(Set.of(VectorStore.FIND_BY_ID));
    storeConfig.setTimeoutMillis(60_000);
    vectorTimeoutMillis = 200;
    harness.store(VECTOR_URN, similarity());
    fake.anchor = List.of(FIRST_CHUNK);
    hydrationReturns(D0);
    fake.blockFindById = true;

    for (int i = 0; i < 3; i++) {
      assertThrows(SearchTemplateFailClosedException.class, this::similar);
    }

    // Each anchor read reached the store with the time the search had left, not the store's own
    assertEquals(fake.findByIdCalls.get(), 3);
    fake.findByIdTimeouts.forEach(
        timeout -> assertTrue(timeout.toMillis() <= 200, timeout.toString()));
    assertTrue(fake.requests.isEmpty());
    // Three in a row did not pause the store
    fake.blockFindById = false;
    vectorTimeoutMillis = 2_000;
    assertEquals(urns(similar()), List.of(D0));
    assertEquals(fake.findByIdCalls.get(), 4);
    assertEquals(fake.requests.size(), 1);
  }

  /** A store that cannot apply the caller's filter fails a similarity search before any call. */
  @Test
  public void testSimilarityWithACallerFilterOnAStoreWithoutPreFilterNeverCallsTheStore()
      throws Exception {
    storeConfig.setCapabilities(Set.of(VectorStore.FIND_BY_ID));
    harness.store(VECTOR_URN, similarity());
    fake.anchor = List.of(FIRST_CHUNK);
    hydrationReturns(D0);

    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            dao.search(
                context(harness.opContext, f -> {}),
                DOCUMENT,
                ANCHOR.toString(),
                filter("domains", "urn:li:domain:sales"),
                null,
                0,
                10,
                List.of()));

    assertEquals(fake.findByIdCalls.get(), 0);
    assertTrue(fake.requests.isEmpty());
    verify(metrics)
        .incrementMicrometer(
            SearchTemplateMetrics.VECTOR_RETRIEVAL,
            1,
            "outcome",
            SearchTemplateVectorRetrieval.NO_PRE_FILTER);
  }

  @Test
  public void testAStoreCannotRaiseAHitPastMinScore() throws Exception {
    hydrationReturns(D0, D1, D2);
    // The service scores a hit (1 + cosine) / 2 from the cosine the store returns
    fake.hits = List.of(new VectorStore.Hit(D0, 0.2), new VectorStore.Hit(D1, 0.9));

    SearchResult result = search(null, f -> f.setMinScore(0.9f));

    assertEquals(urns(result), List.of(D1));
    assertEquals(result.getEntities().get(0).getScore(), 0.95, 1e-9);
    assertEquals(result.getMetadata().getVectorRetrieval().getBelowMinScore().intValue(), 1);
  }

  @Test
  public void testAPageFromTheStoreIsCachedOnlyWithinItsTtl() throws Exception {
    hydrationReturns(D0, D1, D2);
    CachingEntitySearchService cached = cached();
    OperationContext searching = context(harness.opContext, f -> {});
    storeConfig.setResultTtlMillis(3_600_000);

    cached.search(searching, DOCUMENT, "quarterly revenue", null, List.of(), 0, 10, List.of());
    cached.search(searching, DOCUMENT, "quarterly revenue", null, List.of(), 0, 10, List.of());
    assertEquals(fake.requests.size(), 1);

    storeConfig.setResultTtlMillis(50);
    cached.search(searching, DOCUMENT, "quarterly revenue", null, List.of(), 0, 10, List.of());
    Thread.sleep(120);
    cached.search(searching, DOCUMENT, "quarterly revenue", null, List.of(), 0, 10, List.of());
    assertEquals(fake.requests.size(), 3);
  }

  /**
   * Records every call; returns the configured hits or anchor vectors, throws the configured
   * failure, or blocks until the call is cancelled. Calls run on the hybrid read executor, and a
   * similarity rung's on the anchor pool.
   */
  private static final class FakeVectorStore implements VectorStore {
    volatile List<Hit> hits = List.of();
    volatile List<float[]> anchor = List.of();
    volatile IOException failure;
    volatile boolean block;
    volatile boolean blockFindById;
    final CountDownLatch interrupted = new CountDownLatch(1);
    final List<Request> requests = new CopyOnWriteArrayList<>();
    final List<Duration> timeouts = new CopyOnWriteArrayList<>();
    final AtomicInteger findByIdCalls = new AtomicInteger();
    final List<Duration> findByIdTimeouts = new CopyOnWriteArrayList<>();

    @Override
    public List<Hit> search(OperationContext opContext, Request request, Duration timeout)
        throws IOException {
      requests.add(request);
      timeouts.add(timeout);
      if (failure != null) {
        throw failure;
      }
      if (block) {
        try {
          Thread.sleep(20_000);
        } catch (InterruptedException e) {
          interrupted.countDown();
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("cancelled");
        }
      }
      return hits;
    }

    @Override
    public List<float[]> findById(
        OperationContext opContext, String collection, Urn urn, Duration timeout)
        throws IOException {
      findByIdCalls.incrementAndGet();
      findByIdTimeouts.add(timeout);
      if (blockFindById) {
        try {
          Thread.sleep(20_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("cancelled");
        }
      }
      return anchor;
    }
  }

  private SearchResult search(
      Filter filter, Consumer<com.linkedin.metadata.query.SearchFlags> flags) throws Exception {
    return search(harness.opContext, filter, flags);
  }

  private SearchResult search(
      OperationContext base, Filter filter, Consumer<com.linkedin.metadata.query.SearchFlags> flags)
      throws Exception {
    return dao.search(
        context(base, flags), DOCUMENT, "quarterly revenue", filter, null, 0, 10, List.of());
  }

  /** The neighbors of {@link #ANCHOR}. */
  private SearchResult similar() throws Exception {
    return dao.search(
        context(harness.opContext, f -> {}),
        DOCUMENT,
        ANCHOR.toString(),
        null,
        null,
        0,
        10,
        List.of());
  }

  /** A fulltext search with explain on and the vector rung pinned. */
  private OperationContext context(
      OperationContext base, Consumer<com.linkedin.metadata.query.SearchFlags> flags)
      throws Exception {
    OperationContext measured =
        base.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(metrics).build())
            .build(base.getSessionActorContext(), false);
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty(),
            gates());
    coordinator.setVectorTimeoutMillis(vectorTimeoutMillis);
    return TestOperationContexts.withFixedSearchClient(measured, client)
        .withSearchTemplateRuntime(coordinator)
        .withSearchFlags(
            f -> {
              f.setFulltext(true).setIncludeExplain(true);
              flags.accept(f);
              return f.setSearchTemplate(UrnUtils.getUrn(VECTOR_URN));
            });
  }

  /** A caller who is not the system, with view authorization on for every entity type. */
  private OperationContext viewer() {
    RestrictedService restricted = mock(RestrictedService.class);
    when(restricted.encryptRestrictedUrn(any())).thenReturn(MASKED);
    OperationContext base = harness.opContext;
    return base.toBuilder()
        .operationContextConfig(
            base.getOperationContextConfig().toBuilder()
                .allowSystemAuthentication(false)
                .viewAuthorizationConfiguration(
                    ViewAuthorizationConfiguration.builder()
                        .enabled(true)
                        .effectiveUnrestrictedEntityTypes(Set.of())
                        .build())
                .build())
        .servicesRegistryContext(
            ServicesRegistryContext.builder().restrictedService(restricted).build())
        .build(base.getSessionActorContext(), false);
  }

  /** The viewer can see every entity but {@code denied}. Close it after the search. */
  private static MockedStatic<EntityAuthorizationUtils> denying(Urn denied) {
    MockedStatic<EntityAuthorizationUtils> auth = mockStatic(EntityAuthorizationUtils.class);
    auth.when(() -> EntityAuthorizationUtils.canViewEntity(any(OperationContext.class), any()))
        .thenAnswer(invocation -> !denied.equals(invocation.getArgument(1)));
    return auth;
  }

  /** The document profile, listed on {@link #profileSystems}. */
  private SearchTemplateGates gates() {
    EmbeddingProfile profile =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            "text-embedding-3-small",
            "cosine",
            Set.of("document"),
            profileSystems);
    return SearchTemplateGates.of(
        new EmbeddingProfileRegistry(List.of(profile), Map.of("document", MODEL_KEY)),
        elastic.getEntityIndex().getSemanticSearch(),
        false);
  }

  private CachingEntitySearchService cached() {
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
    return new CachingEntitySearchService(new ConcurrentMapCacheManager(), entitySearch, 100, true);
  }

  private static SearchTemplateProperties vector(Consumer<SearchTemplateRetrieval> setup) {
    SearchTemplateProperties template = new SearchTemplateProperties();
    template.setName("Vector");
    template.setApiVersion(2);
    template.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    template.setEditable(true);
    template.setQuery("{ \"match_all\": {} }");
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval().setCombine("vector");
    setup.accept(retrieval);
    template.setRetrieval(retrieval);
    return template;
  }

  /** The ten entities nearest the anchor the query names, without the anchor. */
  private static SearchTemplateProperties similarity() {
    return vector(
        r ->
            r.setQuerySource("anchorEntity")
                .setCandidateCap(10)
                .setAnchor(
                    new SearchTemplateRetrievalAnchor()
                        .setUrnParameter("raw")
                        .setExcludeSelf(true)));
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

  /** The DataHub index holds, inside the system filters, only these entities. */
  private void hydrationReturns(Urn... urns) throws IOException {
    SearchHit[] hits = new SearchHit[urns.length];
    for (int i = 0; i < urns.length; i++) {
      hits[i] = new SearchHit(i, "h" + i, Map.of(), Map.of());
      hits[i].sourceRef(new BytesArray("{\"urn\":\"" + urns[i] + "\"}"));
    }
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(hits, new TotalHits(urns.length, TotalHits.Relation.EQUAL_TO), 1f));
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(response);
  }

  private SearchRequest hydrationRequest() throws IOException {
    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, times(1))
        .search(any(OperationContext.class), request.capture(), eq(RequestOptions.DEFAULT));
    return request.getValue();
  }

  /** The published-document default the GraphQL resolvers add when the caller sets no filter. */
  private static Filter documentDefault() {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            equal("lifecycleStage", "urn:li:lifecycleStageType:PUBLISHED", false),
                            equal("showInGlobalContext", "false", true))),
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            new Criterion()
                                .setField("lifecycleStage")
                                .setCondition(Condition.IS_NULL),
                            equal("state", "UNPUBLISHED", true),
                            equal("showInGlobalContext", "false", true)))));
  }

  private static Criterion equal(String field, String value, boolean negated) {
    return new Criterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValues(new StringArray(value))
        .setNegated(negated);
  }

  /** The URNs the request's terms clause on {@code urn} names, in order. */
  private static List<String> urnTerms(SearchSourceBuilder source) {
    return ((BoolQueryBuilder) source.query())
        .filter().stream()
            .filter(TermsQueryBuilder.class::isInstance)
            .map(TermsQueryBuilder.class::cast)
            .filter(terms -> "urn".equals(terms.fieldName()))
            .flatMap(terms -> terms.values().stream())
            .map(String::valueOf)
            .toList();
  }

  private static List<Urn> urns(SearchResult result) {
    return result.getEntities().stream().map(SearchEntity::getEntity).toList();
  }
}
