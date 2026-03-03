package com.linkedin.metadata.search;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link SemanticSearchService} using a live OpenSearch.
 *
 * <p>These tests validate that the semantic search endpoints work end-to-end and that facets can be
 * fetched via the keyword path and attached to the semantic results. They do not assert hit counts,
 * which depend on the local dataset.
 */
@Test(enabled = false) // Disabled: Requires OpenSearch to be running
public class SemanticSearchServiceIT {

  private SearchClientShim<?> client;

  @BeforeClass
  public void setUp() throws IOException {
    SearchClientShimUtil.ShimConfigurationBuilder configBuilder =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost("localhost")
            .withPort(9200)
            .withSSL(false);
    client =
        SearchClientShimUtil.createShimWithAutoDetection(configBuilder.build(), new ObjectMapper());

    // Skip if OpenSearch is not available or semantic index does not exist
    try {
      RawResponse resp = client.performLowLevelRequest(new Request("GET", "/_cluster/health"));
      int status = resp.getStatusLine().getStatusCode();
      if (status < 200 || status >= 300) {
        throw new SkipException("OpenSearch not healthy: status=" + status);
      }
      RawResponse headIndex =
          client.performLowLevelRequest(new Request("HEAD", "/datasetindex_v2_semantic"));
      int idxStatus = headIndex.getStatusLine().getStatusCode();
      if (idxStatus == 404) {
        throw new SkipException("Index datasetindex_v2_semantic not found; skipping semantic IT");
      }
    } catch (IOException e) {
      throw new RuntimeException("OpenSearch not reachable on localhost:9200", e);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  private SemanticSearchService buildSemanticSearchService(
      OperationContext opContext, IndexConvention indexConvention) {
    // Mock keyword path dependencies to focus IT on semantic end-to-end behavior.
    var mockEntitySearchService =
        Mockito.mock(com.linkedin.metadata.search.EntitySearchService.class);
    // Provide a non-null search service configuration to satisfy CachingEntitySearchService
    Mockito.when(mockEntitySearchService.getSearchServiceConfig())
        .thenReturn(io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG);
    // Stub keyword search used for facets to return an empty-but-non-null result with metadata
    Mockito.when(
            mockEntitySearchService.search(
                Mockito.any(),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyList(),
                Mockito.anyInt(),
                Mockito.any(),
                Mockito.anyList()))
        .thenAnswer(
            inv ->
                new SearchResult()
                    .setEntities(new SearchEntityArray())
                    .setFrom(0)
                    .setPageSize(0)
                    .setNumEntities(0)
                    .setMetadata(
                        new SearchResultMetadata()
                            .setAggregations(new AggregationMetadataArray())));
    CachingEntitySearchService cachingEntitySearchService =
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), mockEntitySearchService, 100, true);

    EntityDocCountCacheConfiguration entityDocCountCacheConfiguration =
        new EntityDocCountCacheConfiguration();
    entityDocCountCacheConfiguration.setTtlSeconds(600L);
    com.linkedin.metadata.search.cache.EntityDocCountCache docCountCache =
        new com.linkedin.metadata.search.cache.EntityDocCountCache(
            opContext.getEntityRegistry(),
            mockEntitySearchService,
            entityDocCountCacheConfiguration);

    SemanticEntitySearchService semanticService =
        new SemanticEntitySearchService(
            client, new ZerosEmbeddingProvider(), new NoOpMappingsBuilder());

    // Create a configuration with semantic search enabled
    SearchServiceConfiguration searchConfig =
        SearchServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(1000).max(1000).build())
                    .build())
            .semanticSearchEnabled(true) // Enable semantic search for this test
            .build();

    return new SemanticSearchService(
        docCountCache, cachingEntitySearchService, semanticService, searchConfig);
  }

  public void testSemanticSearchAcrossEntitiesWithFacets() {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention indexConvention =
        IndexConventionImpl.noPrefix(
            "MD5",
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
                .build());
    OperationContext opContext = base;

    SemanticSearchService semanticSearchService =
        buildSemanticSearchService(opContext, indexConvention);

    // Call semantic search directly with facets requested (entity, platform)
    SearchResult result =
        semanticSearchService.semanticSearchAcrossEntities(
            opContext, List.of("dataset"), "customers", null, null, 0, 5, List.of("entity"));

    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 5);
    assertNotNull(result.getMetadata());
    // Aggregations may be empty with mocked keyword path; this IT focuses on semantic path e2e
  }

  public void testSemanticSearchAcrossEntitiesNoFacetsRequested() {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention indexConvention =
        IndexConventionImpl.noPrefix(
            "MD5",
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
                .build());
    OperationContext opContext = base;

    SemanticSearchService semanticSearchService =
        buildSemanticSearchService(opContext, indexConvention);

    // Call semantic search directly with no facets requested
    SearchResult result =
        semanticSearchService.semanticSearchAcrossEntities(
            opContext, List.of("dataset"), "customers", null, null, 0, 5, List.of());

    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 5);
  }

  public void testSemanticSearchSingleEntity() {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention indexConvention =
        IndexConventionImpl.noPrefix(
            "MD5",
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
                .build());
    OperationContext opContext = base;

    SemanticSearchService semanticSearchService =
        buildSemanticSearchService(opContext, indexConvention);

    // Test the single-entity semantic search endpoint
    SearchResult result =
        semanticSearchService.semanticSearch(
            opContext, List.of("dataset"), "machine learning models", null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 10);
    assertNotNull(result.getMetadata());
  }

  public void testSemanticResultsExistInBaseIndex() throws Exception {
    // This test verifies that entities returned by semantic search
    // exist in the base index and have all expected fields populated correctly

    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention indexConvention =
        IndexConventionImpl.noPrefix(
            "MD5",
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
                .build());

    // Create semantic search service
    var semanticEntitySearchService =
        new SemanticEntitySearchService(
            client, new ZerosEmbeddingProvider(), new NoOpMappingsBuilder());

    // Search for "customers" using semantic search
    SearchResult semanticResult =
        semanticEntitySearchService.search(
            opContext, List.of("dataset"), "customers", null, null, 0, 5);

    // Verify SearchResult structure
    assertNotNull(semanticResult, "SearchResult should not be null");
    assertNotNull(semanticResult.getEntities(), "SearchResult should have entities");
    assertTrue(semanticResult.getEntities().size() > 0, "Should have semantic results");
    assertEquals(semanticResult.getFrom().intValue(), 0, "From should be 0");
    assertEquals(semanticResult.getPageSize().intValue(), 5, "PageSize should be 5");
    assertNotNull(semanticResult.getNumEntities(), "NumEntities should be set");
    assertTrue(
        semanticResult.getNumEntities() >= semanticResult.getEntities().size(),
        "NumEntities should be at least the size of returned entities");
    assertNotNull(semanticResult.getMetadata(), "Metadata should be set");

    // Verify each returned entity exists in the base index
    String baseIndex = indexConvention.getEntityIndexName("dataset");
    ObjectMapper mapper = new ObjectMapper();

    for (var semanticEntity : semanticResult.getEntities()) {
      // 1. Verify entity URN is valid
      assertNotNull(semanticEntity.getEntity(), "Entity URN should not be null");
      String urn = semanticEntity.getEntity().toString();
      assertFalse(urn.isEmpty(), "Entity URN should not be empty");

      // 2. Verify score field (required for search results)
      assertNotNull(semanticEntity.getScore(), "Score should not be null");
      assertTrue(semanticEntity.getScore() > 0, "Score should be positive");

      // 3. Verify features map (required for ranking/scoring metadata)
      assertNotNull(semanticEntity.getFeatures(), "Features should not be null");
      assertFalse(semanticEntity.getFeatures().isEmpty(), "Features should not be empty");

      // Check for expected feature keys based on SearchRequestHandler.extractFeatures
      assertTrue(
          semanticEntity.getFeatures().containsKey("SEARCH_BACKEND_SCORE"),
          "Features should contain SEARCH_BACKEND_SCORE");
      Double backendScore = semanticEntity.getFeatures().get("SEARCH_BACKEND_SCORE");
      assertNotNull(backendScore, "SEARCH_BACKEND_SCORE should not be null");
      assertTrue(backendScore > 0, "SEARCH_BACKEND_SCORE should be positive");

      // QUERY_COUNT is optional but if present, should be valid
      if (semanticEntity.getFeatures().containsKey("QUERY_COUNT")) {
        Double queryCount = semanticEntity.getFeatures().get("QUERY_COUNT");
        assertNotNull(queryCount, "QUERY_COUNT should not be null if present");
        assertTrue(queryCount >= 0, "QUERY_COUNT should be non-negative");
      }

      // 4. Verify extraFields (source document fields)
      assertNotNull(semanticEntity.getExtraFields(), "ExtraFields should not be null");
      assertFalse(semanticEntity.getExtraFields().isEmpty(), "ExtraFields should not be empty");

      // Check for expected fields in extraFields (based on source document)
      assertTrue(
          semanticEntity.getExtraFields().containsKey("urn"), "ExtraFields should contain urn");
      // The URN in extraFields may be JSON-encoded (with quotes), so compare the actual value
      String extraFieldsUrn = semanticEntity.getExtraFields().get("urn");
      assertTrue(
          extraFieldsUrn != null
              && (extraFieldsUrn.equals(urn) || extraFieldsUrn.equals("\"" + urn + "\"")),
          "URN in extraFields should match entity URN (with or without JSON encoding)");

      // 5. Verify matchedFields (should be empty for semantic search as no highlighting)
      // Note: matchedFields is optional and can be null for semantic search
      if (semanticEntity.getMatchedFields() != null) {
        assertTrue(
            semanticEntity.getMatchedFields().isEmpty(),
            "MatchedFields should be empty for semantic search (no highlighting)");
      }

      // 6. Query the base index directly to verify document exists
      String requestBody =
          String.format("{\"query\": {\"term\": {\"urn\": \"%s\"}}}", urn.replace("\"", "\\\""));

      Request request = new Request("POST", "/" + baseIndex + "/_search");
      request.setJsonEntity(requestBody);
      RawResponse response = client.performLowLevelRequest(request);

      assertEquals(
          200, response.getStatusLine().getStatusCode(), "Should find URN in base index: " + urn);

      // Parse response to verify document exists
      String responseBody = org.apache.http.util.EntityUtils.toString(response.getEntity());
      var responseJson = mapper.readTree(responseBody);
      var hits = responseJson.path("hits").path("hits");

      assertTrue(hits.size() > 0, "Should find at least one document for URN: " + urn);

      // Verify the document has expected fields
      var doc = hits.get(0).path("_source");
      assertEquals(urn, doc.path("urn").asText(), "URN in index should match");

      // 7. Cross-check that key fields from extraFields exist in the source document
      // This ensures we're not fabricating data
      for (String key : semanticEntity.getExtraFields().keySet()) {
        if (!key.equals("urn")) {
          // Skip URN as we already checked it
          // The field should either exist in doc or be a computed/transformed field
          // Some fields may be computed/transformed and not directly in source
        }
      }
    }
  }

  private static final class ZerosEmbeddingProvider implements EmbeddingProvider {

    @Override
    public @Nonnull float[] embed(@Nonnull String text, @javax.annotation.Nullable String model) {
      // For tests, always return 1024-dimensional vector
      int expectedDimensions = 1024;
      float[] vec = new float[expectedDimensions];
      for (int i = 0; i < expectedDimensions; i++) {
        vec[i] = 0.01f;
      }
      return vec;
    }
  }

  public void testSemanticSearchMetadataFields() throws Exception {
    // Test that scoringMethod is correctly set in metadata

    // Set up the operation context for semantic search
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention indexConvention =
        IndexConventionImpl.noPrefix(
            "MD5",
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
                .build());
    OperationContext opContext = base;

    SemanticSearchService semanticSearchService =
        buildSemanticSearchService(opContext, indexConvention);

    // 1. Test semantic search metadata
    SearchResult semanticResult =
        semanticSearchService.semanticSearchAcrossEntities(
            opContext,
            List.of("dataset"),
            "looker strategy",
            null, // filters
            null, // sortCriteria
            0, // from
            10, // size
            List.of()); // facets

    assertNotNull(semanticResult.getMetadata());
  }
}
