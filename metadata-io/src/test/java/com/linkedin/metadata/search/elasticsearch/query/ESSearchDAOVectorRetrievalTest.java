package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.VectorRetrievalExplanation;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDryRender;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.utils.ESUtils;
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
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A {@code combine: vector} rung served through {@link ESSearchDAO#search}. */
public class ESSearchDAOVectorRetrievalTest {
  private static final String VECTOR_URN = "urn:li:searchTemplate:vector";
  private static final String KEYWORD_URN = "urn:li:searchTemplate:keyword";
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

  private static SearchTemplateLadder nextOnMiss() {
    return new SearchTemplateLadder()
        .setStop(new SearchTemplateLadderStopArray(new SearchTemplateLadderStop().setMinHits(1)))
        .setNext(UrnUtils.getUrn(KEYWORD_URN));
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
