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
import com.linkedin.metadata.search.utils.DocumentDefaultFilter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRetrieval;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.common.bytes.BytesArray;
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

  private SearchClientShim<?> client;
  private EmbeddingProvider provider;
  private MetricUtils metrics;
  private SearchTemplateFixtures.Harness harness;
  private ElasticSearchConfiguration elastic;
  private VectorStoreConfiguration storeConfig;
  private FakeVectorStore fake;
  private ESSearchDAO dao;
  // The system the document profile lists
  private String profileSystem;

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
    profileSystem = STORE;
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

    // The published-document default the GraphQL resolvers pass with no caller filter is a
    // system default too
    Filter documentDefault = DocumentDefaultFilter.build(true, false);
    search(documentDefault, f -> {});
    assertNull(fake.requests.get(1).filter());
    // A caller filter still counts when it is shaped like part of that default, alone or merged
    // with it, and so does the default merged with a caller facet
    Filter published =
        filter("lifecycleStage", DocumentDefaultFilter.PUBLISHED_LIFECYCLE_STAGE_URN);
    Filter noStage =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("lifecycleStage")
                                    .setCondition(Condition.IS_NULL)))));
    for (Filter callerShaped :
        List.of(
            published,
            noStage,
            FilterUtils.combineFilters(published, documentDefault),
            FilterUtils.combineFilters(caller, documentDefault))) {
      assertThrows(SearchTemplateFailClosedException.class, () -> search(callerShaped, f -> {}));
    }
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
    profileSystem = EmbeddingProfileRegistry.OPENSEARCH;
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
  public void testAStoreWithoutFindByIdCannotServeSimilarity() throws Exception {
    fake.anchor = new float[] {0.3f, 0.4f};
    TemplateVectorSearch vectorSearch = new TemplateVectorSearch(dao);
    OperationContext opContext = harness.opContext;

    assertThrows(
        SearchTemplateFailClosedException.class,
        () -> vectorSearch.anchorVectors(opContext, STORE, MODEL_KEY, D0));
    assertEquals(fake.findByIdCalls.get(), 0);
    // The DataHub index store does not find by id either
    assertThrows(
        SearchTemplateFailClosedException.class,
        () ->
            vectorSearch.anchorVectors(
                opContext, EmbeddingProfileRegistry.OPENSEARCH, MODEL_KEY, D0));

    // With findById the store returns the anchor's query vector for the service to send back
    storeConfig.setCapabilities(Set.of(VectorStore.FIND_BY_ID));
    List<float[]> vectors = vectorSearch.anchorVectors(opContext, STORE, MODEL_KEY, D0);
    assertEquals(vectors.size(), 1);
    assertEquals(vectors.get(0), fake.anchor);
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
   * Records every call; returns the configured hits or anchor vector, throws the configured
   * failure, or blocks until the call is cancelled. Calls run on the hybrid read executor.
   */
  private static final class FakeVectorStore implements VectorStore {
    volatile List<Hit> hits = List.of();
    volatile float[] anchor;
    volatile IOException failure;
    volatile boolean block;
    final CountDownLatch interrupted = new CountDownLatch(1);
    final List<Request> requests = new CopyOnWriteArrayList<>();
    final List<Duration> timeouts = new CopyOnWriteArrayList<>();
    final AtomicInteger findByIdCalls = new AtomicInteger();

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
        OperationContext opContext, String collection, Urn urn, Duration timeout) {
      findByIdCalls.incrementAndGet();
      return List.of(anchor);
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

  /** A fulltext search with explain on and the vector rung pinned. */
  private OperationContext context(
      OperationContext base, Consumer<com.linkedin.metadata.query.SearchFlags> flags)
      throws Exception {
    OperationContext measured =
        base.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(metrics).build())
            .build(base.getSessionActorContext(), false);
    return TestOperationContexts.withFixedSearchClient(measured, client)
        .withSearchTemplateRuntime(
            new SearchTemplateCoordinator(
                elastic,
                null,
                harness.loader,
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty(),
                gates()))
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

  /** The document profile, listed on {@link #profileSystem}. */
  private SearchTemplateGates gates() {
    EmbeddingProfile profile =
        new EmbeddingProfile(
            MODEL_KEY,
            EmbeddingProfile.TEXT_CHUNKS,
            "text-embedding-3-small",
            "cosine",
            Set.of("document"),
            Set.of(profileSystem));
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

  private static List<Urn> urns(SearchResult result) {
    return result.getEntities().stream().map(SearchEntity::getEntity).toList();
  }
}
