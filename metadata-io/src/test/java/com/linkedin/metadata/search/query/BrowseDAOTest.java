package com.linkedin.metadata.search.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRouteArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Import(SearchCommonTestConfiguration.class)
public class BrowseDAOTest extends AbstractTestNGSpringContextTests {
  private SearchClientShim<?> mockClient;
  private ESBrowseDAO browseDAO;
  private OperationContext opContext;

  @Autowired
  @Qualifier("defaultTestCustomSearchConfig")
  private CustomSearchConfiguration customSearchConfiguration;

  @BeforeMethod
  public void setup() throws RemoteInvocationException, URISyntaxException {
    mockClient = mock(SearchClientShim.class);
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("es_browse_dao_test"),
            SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

    opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoSearchAuthorization(
                SearchContext.EMPTY.toBuilder().indexConvention(indexConvention).build()),
            mockClient);
    browseDAO =
        new ESBrowseDAO(
            TEST_OS_SEARCH_CONFIG,
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  public static Urn makeUrn(Object id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBrowseV2WithLimitedResults() throws Exception {
    // Configure mock response for testing browseV2 method
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    // Configure aggregations for groups response
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);

    // Configure client to return our mock response
    when(mockClient.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse);

    // Configure search configuration with specific limits
    SearchServiceConfiguration testSearchServiceConfig =
        SearchServiceConfiguration.builder()
            .limit(
                new LimitConfig()
                    .setResults(
                        new ResultsLimitConfig().setMax(25).setApiDefault(25).setStrict(false)))
            .build();

    // Create a new browse DAO with our test configuration
    ESBrowseDAO testBrowseDAO =
        new ESBrowseDAO(
            TEST_OS_SEARCH_CONFIG,
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY,
            testSearchServiceConfig);

    // Call browseV2 with a count that exceeds the limit
    int requestedCount = 30;
    BrowseResultV2 result =
        testBrowseDAO.browseV2(
            opContext, "dataset", "/test/path", null, "test query", 0, requestedCount);

    // Verify the search request captured by the mock client
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient)
        .search(any(OperationContext.class), requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    // This method doesn't directly use the size parameter in the captured request,
    // but we can still verify the page size in the result
    assertEquals(result.getPageSize(), 25);
  }

  @Test
  public void testBrowseV2OnV3UsesV3FullTextQuery() throws Exception {
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);
    when(mockClient.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse);

    new ESBrowseDAO(
            v3Config(true),
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .browseV2(opContext, "dataset", "", null, "orders", 0, 10);

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient)
        .search(any(OperationContext.class), requestCaptor.capture(), eq(RequestOptions.DEFAULT));
    String query = requestCaptor.getValue().source().query().toString();
    // Browse depth reads the browse path token count as on V2; the input query is the V3
    // full-text query over the shared _search fields (deliberate V3 change)
    assertTrue(query.contains("\"browsePathV2.length\""), query);
    assertFalse(query.contains("_aspects."), query);
    assertTrue(query.contains("name.text"), query);
    assertFalse(query.contains("query_urn_component"), query);
  }

  @Test
  public void testBrowseV2RequestUsesSearchRouter() throws Exception {
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    Map<String, Object> search = new HashMap<>();
    search.put("default", "urn:li:searchTemplate:search");
    Map<String, Object> structured = new HashMap<>();
    structured.put("default", "urn:li:searchTemplate:structured");
    templates.setDefaults(Map.of("fulltext", search, "structured", structured));
    ElasticSearchConfiguration elastic =
        v3Config(true).toBuilder()
            .search(
                v3Config(true).getSearch().toBuilder()
                    .v3(new SearchV3Configuration().setTemplates(templates))
                    .build())
            .build();
    opContext =
        opContext.withSearchTemplateRuntime(
            new SearchTemplateCoordinator(
                elastic,
                null,
                new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH),
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty()));
    try {
      assertBrowseQuery(elastic, "", "*", "match_all");
      assertBrowseQuery(elastic, "", "orders", "simple_query_string");
      assertBrowseQuery(elastic, "␟prod", "orders", "simple_query_string");
    } finally {
      opContext = opContext.withSearchTemplateRuntime(null);
    }
  }

  @Test
  public void testBrowseRequestKeepsJavaGroupingWhenTemplateAddsAggregations() throws Exception {
    SearchTemplateProperties leaf = new SearchTemplateProperties();
    leaf.setName("Hostile browse");
    leaf.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    leaf.setQuery("{ \"term\": { \"templateMarker\": \"browse-lexical\" } }");
    leaf.setFunctionScore(
        "{ \"functions\": [], \"score_mode\": \"multiply\", \"boost_mode\": \"multiply\" }");
    leaf.setRescore(
        "{ \"window_size\": 10, \"query\": { \"rescore_query\": { \"match_all\": {} }, \"query_weight\": 1.0, \"rescore_query_weight\": 1.0 } }");
    leaf.setAggregations(
        "{ \"groups\": { \"terms\": { \"field\": \"stolen\", \"size\": 3 } }, \"platform\": { \"terms\": { \"field\": \"platform.keyword\", \"size\": 9 } } }");
    leaf.setRoutes(new SearchTemplateRouteArray());
    SearchTemplateProperties router = new SearchTemplateProperties();
    router.setName("Browse");
    router.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    router.setRoutes(new SearchTemplateRouteArray());
    SearchTemplateDefaultRoute route = new SearchTemplateDefaultRoute();
    route.setTemplate(UrnUtils.getUrn("urn:li:searchTemplate:browseHostile"));
    router.setDefaultRoute(route);
    Map<String, SearchTemplateProperties> catalog = new HashMap<>();
    catalog.put("urn:li:searchTemplate:browseRouter", router);
    catalog.put("urn:li:searchTemplate:browseHostile", leaf);

    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    Map<String, Object> browse = new HashMap<>();
    browse.put("default", "urn:li:searchTemplate:browseRouter");
    templates.setDefaults(Map.of("fulltext", browse));
    ElasticSearchConfiguration elastic =
        v3Config(true).toBuilder()
            .search(
                v3Config(true).getSearch().toBuilder()
                    .v3(new SearchV3Configuration().setTemplates(templates))
                    .build())
            .build();
    opContext =
        opContext.withSearchTemplateRuntime(
            new SearchTemplateCoordinator(
                elastic,
                null,
                new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH, catalog),
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty()));
    try {
      SearchRequest single = browseRequest(elastic, "dataset-path");
      SearchRequest across = browseAcrossRequest(elastic, "dataset-path");
      for (SearchRequest request : List.of(single, across)) {
        String query = request.source().query().toString();
        assertTrue(query.contains("browse-lexical"), query);
        assertTrue(query.contains("function_score"), query);
        assertTrue(query.contains("browsePathV2"), query);
        assertTrue(query.contains("removed"), query);
        assertNull(request.source().highlighter());
        assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
        assertNull(request.source().suggest());
        assertEquals(request.source().size(), 0);
        List<TermsAggregationBuilder> groups =
            request.source().aggregations().getAggregatorFactories().stream()
                .filter(aggregation -> "groups".equals(aggregation.getName()))
                .map(TermsAggregationBuilder.class::cast)
                .toList();
        assertEquals(groups.size(), 1);
        assertEquals(groups.get(0).field(), "browsePathV2");
        assertEquals(groups.get(0).size(), 2000);
        assertFalse(groups.get(0).field().equals("stolen"));
        assertTrue(
            request.source().aggregations().getAggregatorFactories().stream()
                .noneMatch(aggregation -> "platform".equals(aggregation.getName())));
      }
    } finally {
      opContext = opContext.withSearchTemplateRuntime(null);
    }
  }

  private SearchRequest browseRequest(ElasticSearchConfiguration elastic, String path)
      throws Exception {
    return captureBrowse(
        () ->
            new ESBrowseDAO(
                    elastic,
                    customSearchConfiguration,
                    QueryFilterRewriteChain.EMPTY,
                    TEST_SEARCH_SERVICE_CONFIG)
                .browseV2(
                    opContext
                        .withSearchSurface(null)
                        .withSearchFlags(flags -> flags.setGetSuggestions(true).setFulltext(true)),
                    "dataset",
                    path,
                    null,
                    "orders",
                    0,
                    10));
  }

  private SearchRequest browseAcrossRequest(ElasticSearchConfiguration elastic, String path)
      throws Exception {
    return captureBrowse(
        () ->
            new ESBrowseDAO(
                    elastic,
                    customSearchConfiguration,
                    QueryFilterRewriteChain.EMPTY,
                    TEST_SEARCH_SERVICE_CONFIG)
                .browseV2(
                    opContext
                        .withSearchSurface(null)
                        .withSearchFlags(flags -> flags.setGetSuggestions(true).setFulltext(true)),
                    List.of("dataset"),
                    path,
                    null,
                    "orders",
                    0,
                    10));
  }

  private SearchRequest captureBrowse(BrowseCall call) throws Exception {
    org.mockito.Mockito.reset(mockClient);
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);
    when(mockClient.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse);
    call.run();
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient)
        .search(any(OperationContext.class), requestCaptor.capture(), eq(RequestOptions.DEFAULT));
    return requestCaptor.getValue();
  }

  @FunctionalInterface
  private interface BrowseCall {
    void run() throws Exception;
  }

  private void assertBrowseQuery(
      ElasticSearchConfiguration elastic, String path, String input, String lexicalClause)
      throws Exception {
    org.mockito.Mockito.reset(mockClient);
    SearchResponse mockGroupsResponse = mock(SearchResponse.class);
    SearchHits mockGroupsHits = mock(SearchHits.class);
    when(mockGroupsResponse.getHits()).thenReturn(mockGroupsHits);
    when(mockGroupsHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    Aggregations mockAggs = mock(Aggregations.class);
    when(mockAggs.get("groups")).thenReturn(new ParsedStringTerms());
    when(mockGroupsResponse.getAggregations()).thenReturn(mockAggs);
    when(mockClient.search(
            any(OperationContext.class), any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockGroupsResponse);

    new ESBrowseDAO(
            elastic,
            customSearchConfiguration,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG)
        .browseV2(
            opContext
                .withSearchSurface(null)
                .withSearchFlags(flags -> flags.setGetSuggestions(true).setFulltext(true)),
            "dataset",
            path,
            null,
            input,
            0,
            10);

    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient)
        .search(any(OperationContext.class), requestCaptor.capture(), eq(RequestOptions.DEFAULT));
    SearchRequest request = requestCaptor.getValue();
    String query = request.source().query().toString();
    assertTrue(query.contains(lexicalClause), query);
    // An empty query or * is the complete URN list, which does not rank.
    if ("*".equals(input) || input.isEmpty()) {
      assertFalse(query.contains("function_score"), query);
    } else {
      assertTrue(query.contains("function_score"), query);
    }
    assertTrue(query.contains("\"browsePathV2.length\""), query);
    assertTrue(query.contains("removed"), query);
    if (path.isEmpty()) {
      assertFalse(query.contains("␟prod"), query);
    } else {
      assertTrue(query.contains(path), query);
    }
    assertEquals(request.source().size(), 0);
    assertNull(request.source().highlighter());
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
    assertNull(request.source().suggest());
    TermsAggregationBuilder groups =
        request.source().aggregations().getAggregatorFactories().stream()
            .filter(aggregation -> "groups".equals(aggregation.getName()))
            .map(TermsAggregationBuilder.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(groups.field(), "browsePathV2");
    assertEquals(groups.size(), 2000);
  }

  /** V2 and V3 both written; V3 keyword reads as given. */
  private static ElasticSearchConfiguration v3Config(boolean keywordReadEnabled) {
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(keywordReadEnabled)
                        .build())
                .build())
        .build();
  }
}
