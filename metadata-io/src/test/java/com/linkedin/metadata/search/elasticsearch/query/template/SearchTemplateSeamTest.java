package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_SEARCH_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_STRUCTURED_URN;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SearchRanker;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * Locks the review seams: config defaults, surface names, context keys, and one fuzzy derivation.
 */
public class SearchTemplateSeamTest {

  @Test
  public void testSafetyDefaultsMatchShippedYaml() throws Exception {
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    assertFalse(templates.isEnabled());
    assertEquals(templates.getMaxLeafClauses(), 0);
    assertEquals(templates.getHybridKeywordScale(), 0d);
    assertEquals(templates.getMaxParentDepth(), 0);
    assertEquals(templates.getCacheMaximumSize(), 0);
    assertEquals(templates.getMaxRoutes(), 0);
    assertEquals(templates.getMaxRoutePatternLength(), 0);
    assertEquals(templates.getMaxRouteHops(), 0);
    String yaml =
        Files.readString(
            Path.of(
                "..",
                "metadata-service",
                "configuration",
                "src",
                "main",
                "resources",
                "application.yaml"));
    assertEquals(templates.getDocumentRerankTimeoutSeconds(), 2);
    assertTrue(yaml.contains("maxLeafClauses: ${SEARCH_V3_TEMPLATES_MAX_LEAF_CLAUSES:1024}"));
    assertTrue(
        yaml.contains(
            "documentRerankTimeoutSeconds: ${SEARCH_V3_TEMPLATES_DOCUMENT_RERANK_TIMEOUT_SECONDS:2}"));
    assertTrue(
        yaml.contains("hybridKeywordScale: ${SEARCH_V3_TEMPLATES_HYBRID_KEYWORD_SCALE:500}"));
    assertTrue(yaml.contains("maxParentDepth: ${SEARCH_V3_TEMPLATES_MAX_PARENT_DEPTH:5}"));
    assertTrue(yaml.contains("cacheMaximumSize: ${SEARCH_V3_TEMPLATES_CACHE_MAXIMUM_SIZE:500}"));
    assertTrue(yaml.contains("maxRoutes: ${SEARCH_V3_TEMPLATES_MAX_ROUTES:32}"));
    assertTrue(
        yaml.contains(
            "maxRoutePatternLength: ${SEARCH_V3_TEMPLATES_MAX_ROUTE_PATTERN_LENGTH:256}"));
    assertTrue(yaml.contains("maxRouteHops: ${SEARCH_V3_TEMPLATES_MAX_ROUTE_HOPS:5}"));
  }

  @Test
  public void testKnnCandidateDefaultSaturates() {
    SearchTemplateKnn knn =
        SearchTemplateJson.parseKnn("{\"field\":\"embedding\",\"k\":" + Integer.MAX_VALUE + "}");
    assertEquals(knn.k(), Integer.MAX_VALUE);
    assertEquals(knn.numCandidates(), Integer.MAX_VALUE);
  }

  @Test
  public void testEntitySurfacesShareOneNamingPattern() {
    assertEquals(SearchTemplateSurfaces.SEARCH_ENTITIES, "searchEntities");
    assertEquals(SearchTemplateSurfaces.SCROLL_ENTITIES, "scrollEntities");
    assertEquals(SearchTemplateSurfaces.AGGREGATE_ENTITIES, "aggregateEntities");
    assertNotEquals(SearchTemplateSurfaces.SEARCH_ENTITIES, SearchTemplateSurfaces.SCROLL_ENTITIES);
    assertTrue(SearchTemplateRollout.scrollSurface(SearchTemplateSurfaces.SCROLL_ENTITIES));
    assertFalse(SearchTemplateRollout.scrollSurface(SearchTemplateSurfaces.SEARCH_ENTITIES));
    assertFalse(SearchTemplateRollout.scrollSurface(null));
    assertFalse(SearchTemplateRollout.scrollSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES));
  }

  @Test
  public void testMissingSurfaceFollowsTheExecutionShape() {
    OperationContext plain = TestOperationContexts.systemContextNoSearchAuthorization();
    assertEquals(
        SearchTemplateSurfaces.withDefault(plain, SearchTemplateSurfaces.SEARCH_ENTITIES)
            .getSearchContext()
            .getSearchSurface(),
        SearchTemplateSurfaces.SEARCH_ENTITIES);
    assertEquals(
        SearchTemplateSurfaces.withDefault(plain, SearchTemplateSurfaces.SCROLL_ENTITIES)
            .getSearchContext()
            .getSearchSurface(),
        SearchTemplateSurfaces.SCROLL_ENTITIES);
    OperationContext aggregate = plain.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES);
    assertEquals(
        SearchTemplateSurfaces.withDefault(aggregate, SearchTemplateSurfaces.SEARCH_ENTITIES)
            .getSearchContext()
            .getSearchSurface(),
        SearchTemplateSurfaces.AGGREGATE_ENTITIES);
  }

  @Test
  public void testContextAndSubfieldConstants() {
    assertEquals(SearchTemplateContextVariables.QUERY, "query");
    assertTrue(
        SearchTemplateContextVariables.STABLE.contains(SearchTemplateContextVariables.QUERY));
    assertTrue(
        SearchTemplateContextVariables.PATHS.contains(
            SearchTemplateContextVariables.FLAGS_ALPHANUMERIC_SPLIT));
    assertEquals(StructuredPropertyReferences.KEYWORD, StructuredPropertySubfields.KEYWORD);
    assertEquals(
        StructuredPropertySubfields.suffix("keyword"), StructuredPropertySubfields.KEYWORD_SUFFIX);
    assertEquals(StructuredPropertySubfields.strip("name.keyword"), "name");
  }

  @Test
  public void testUntaggedPageSearchUsesTheLightLadder() {
    SearchTemplateFixtures.Harness harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installShippedTemplates();
    try {
      shippedSurfaceSeeds(harness);
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(harness.elastic(), null, harness.loader);
      ESSearchDAO dao = mock(ESSearchDAO.class);
      ElasticSearchService elastic = elasticSearch(harness, dao);
      OperationContext page = capturePageSearch(elastic, dao, harness.opContext);
      ResolvedSearchTemplate light = resolve(coordinator, page, false);
      assertEquals(light.getRouteChain().get(0), "urn:li:searchTemplate:searchLight");
      assertEquals(light.getUrn(), "urn:li:searchTemplate:light");

      CachingEntitySearchService cache = mock(CachingEntitySearchService.class);
      when(cache.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
          .thenReturn(emptyPage());
      SearchService searchService =
          new SearchService(
              mock(EntityDocCountCache.class),
              cache,
              mock(SearchRanker.class),
              TEST_SEARCH_SERVICE_CONFIG);
      searchService.searchAcrossEntities(
          harness.opContext, List.of("dataset"), "orders", null, List.of(), 0, 10);
      ArgumentCaptor<OperationContext> across = ArgumentCaptor.forClass(OperationContext.class);
      verify(cache).search(across.capture(), any(), any(), any(), any(), anyInt(), any(), any());
      ResolvedSearchTemplate acrossResolved = resolve(coordinator, across.getValue(), false);
      assertEquals(acrossResolved.getRouteChain(), light.getRouteChain());
      assertEquals(acrossResolved.getUrn(), light.getUrn());

      OperationContext structuredRequest =
          capturePageSearch(
              elastic, dao, harness.opContext.withSearchFlags(flags -> flags.setFulltext(false)));
      assertEquals(
          resolve(coordinator, structuredRequest, false).getUrn(), SEARCH_TEMPLATE_STRUCTURED_URN);

      OperationContext untagged =
          capturePageSearch(elastic, dao, harness.opContext.withSearchSurface(null));
      assertEquals(
          untagged.getSearchContext().getSearchSurface(), SearchTemplateSurfaces.SEARCH_ENTITIES);
      ResolvedSearchTemplate untaggedResolved = resolve(coordinator, untagged, false);
      assertEquals(untaggedResolved.getRouteChain().get(0), "urn:li:searchTemplate:searchLight");
      assertEquals(untaggedResolved.getUrn(), "urn:li:searchTemplate:light");

      OperationContext aggregate =
          capturePageSearch(
              elastic,
              dao,
              harness.opContext.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES));
      assertEquals(
          aggregate.getSearchContext().getSearchSurface(),
          SearchTemplateSurfaces.AGGREGATE_ENTITIES);
      ResolvedSearchTemplate aggregateResolved = resolve(coordinator, aggregate, false);
      assertEquals(aggregateResolved.getRouteChain().get(0), SEARCH_TEMPLATE_SEARCH_URN);
      assertEquals(aggregateResolved.getUrn(), SEARCH_TEMPLATE_FULLTEXT_URN);

      when(cache.scroll(any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(emptyScroll());
      searchService.scrollAcrossEntities(
          harness.opContext,
          List.of("dataset"),
          "orders",
          null,
          List.of(),
          null,
          null,
          10,
          List.of());
      ArgumentCaptor<OperationContext> scroll = ArgumentCaptor.forClass(OperationContext.class);
      verify(cache)
          .scroll(scroll.capture(), any(), any(), any(), any(), any(), any(), any(), any());
      ResolvedSearchTemplate scrolled = resolve(coordinator, scroll.getValue(), true);
      assertEquals(
          scroll.getValue().getSearchContext().getSearchSurface(),
          SearchTemplateSurfaces.SCROLL_ENTITIES);
      assertEquals(scrolled.getRouteChain().get(0), SEARCH_TEMPLATE_SEARCH_URN);
      assertEquals(scrolled.getUrn(), SEARCH_TEMPLATE_FULLTEXT_URN);
      assertFalse(scrolled.getRouteChain().contains("urn:li:searchTemplate:searchLight"));
      assertFalse(scrolled.getRouteChain().contains("urn:li:searchTemplate:light"));
    } finally {
      SearchTemplates.install(null);
    }
  }

  private static void shippedSurfaceSeeds(SearchTemplateFixtures.Harness harness) {
    harness
        .templates
        .getSurfaces()
        .put(
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            fulltextSeed("urn:li:searchTemplate:searchLight"));
    harness
        .templates
        .getSurfaces()
        .put(SearchTemplateSurfaces.SCROLL_ENTITIES, fulltextSeed(SEARCH_TEMPLATE_SEARCH_URN));
    harness
        .templates
        .getSurfaces()
        .put(SearchTemplateSurfaces.AGGREGATE_ENTITIES, fulltextSeed(SEARCH_TEMPLATE_SEARCH_URN));
  }

  private static Map<String, Map<String, Object>> fulltextSeed(String urn) {
    Map<String, Map<String, Object>> seed = new HashMap<>();
    seed.put("fulltext", new HashMap<>(Map.of("default", urn)));
    return seed;
  }

  private static ElasticSearchService elasticSearch(
      SearchTemplateFixtures.Harness harness, ESSearchDAO dao) {
    return new ElasticSearchService(
        mock(ESIndexBuilder.class),
        TEST_SEARCH_SERVICE_CONFIG,
        harness.elastic(),
        mock(MappingsBuilder.class),
        mock(SettingsBuilder.class),
        dao,
        mock(ESBrowseDAO.class),
        mock(ESWriteDAO.class));
  }

  private static OperationContext capturePageSearch(
      ElasticSearchService elastic, ESSearchDAO dao, OperationContext opContext) {
    reset(dao);
    when(dao.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(emptyPage());
    elastic.search(opContext, List.of("dataset"), "orders", null, List.of(), 0, 10);
    ArgumentCaptor<OperationContext> captured = ArgumentCaptor.forClass(OperationContext.class);
    verify(dao).search(captured.capture(), any(), any(), any(), any(), anyInt(), any(), any());
    return captured.getValue();
  }

  private static ResolvedSearchTemplate resolve(
      SearchTemplateCoordinator coordinator, OperationContext opContext, boolean scroll) {
    return coordinator
        .bind(opContext, "orders", scroll, List.of("dataset"))
        .getSearchContext()
        .getResolvedSearchTemplate();
  }

  private static SearchResult emptyPage() {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()))
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(0);
  }

  private static ScrollResult emptyScroll() {
    return new ScrollResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setPageSize(10)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  @Test
  public void testFuzzyDerivationUsesTheQueryBuilder() {
    assertEquals(
        SearchTemplateQueryShape.of("orders").fuzzyQuery,
        V3SearchQueryBuilder.makeFuzzyQuery("orders"));
    assertEquals(
        SearchTemplateQueryShape.of("qualified").fuzzyQuery,
        V3SearchQueryBuilder.makeFuzzyQuery("qualified"));
  }
}
