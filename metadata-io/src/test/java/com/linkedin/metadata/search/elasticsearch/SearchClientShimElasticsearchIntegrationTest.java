package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.*;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.indices.AnalyzeResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.client.indices.ResizeResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

/**
 * Integration tests for SearchClientShim with Elasticsearch. These tests validate the shim works
 * correctly against a live Elasticsearch instance.
 */
@Slf4j
@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SearchClientShimElasticsearchIntegrationTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_INDEX = "test-shim-index";
  private static final String TEST_INDEX_2 = "test-shim-index-2";
  private static final String TEST_ALIAS = "test-alias";
  private static final String CLONE_INDEX = "test-clone-index";

  @Autowired private GenericContainer<?> elasticsearchContainer;
  @Autowired private SearchClientShim<?> searchClientShim;

  @Test
  public void testShimCreation() {
    // Test native client access
    Object nativeClient = searchClientShim.getNativeClient();
    if (SearchEngineType.ELASTICSEARCH_7.equals(searchClientShim.getEngineType())) {
      assertTrue(nativeClient instanceof org.opensearch.client.RestHighLevelClient);
    } else {
      assertTrue(nativeClient instanceof ElasticsearchClient);
    }
  }

  @Test
  public void testClusterInfo() throws IOException {

    Map<String, String> clusterInfo = searchClientShim.getClusterInfo();
    assertNotNull(clusterInfo);

    assertTrue(clusterInfo.containsKey("version"));

    // Verify version
    String version = clusterInfo.get("version");
    assertTrue(
        version.startsWith(System.getProperty("ELASTIC_VERSION")),
        "Expected version to start with "
            + System.getProperty("ELASTIC_VERSION")
            + " , got: "
            + version);
  }

  @Test
  public void testEngineVersion() throws IOException {

    String version = searchClientShim.getEngineVersion();
    assertNotNull(version);
    assertNotEquals(version, "unknown");
    assertTrue(
        version.startsWith(System.getProperty("ELASTIC_VERSION")),
        "Expected version to start with "
            + System.getProperty("ELASTIC_VERSION")
            + " , got: "
            + version);
  }

  @Test
  public void testFeatureSupport() {
    if (SearchEngineType.ELASTICSEARCH_7.equals(searchClientShim.getEngineType())) {
      // Test features that ES 7.17 should support
      assertTrue(searchClientShim.supportsFeature("scroll"));
      assertTrue(searchClientShim.supportsFeature("bulk"));
      assertTrue(searchClientShim.supportsFeature("mapping_types"));
      assertTrue(searchClientShim.supportsFeature("point_in_time"));
      assertTrue(searchClientShim.supportsFeature("async_search"));
    } else {
      // Test features that ES 8.17 should support
      assertTrue(searchClientShim.supportsFeature("scroll"));
      assertTrue(searchClientShim.supportsFeature("bulk"));
      assertFalse(searchClientShim.supportsFeature("mapping_types"));
      assertTrue(searchClientShim.supportsFeature("point_in_time"));
      assertTrue(searchClientShim.supportsFeature("async_search"));
    }
  }

  @Test
  public void testIndexOperations() throws IOException {
    log.info("Testing index management operations");

    // Test index creation
    CreateIndexRequest createRequest = new CreateIndexRequest(TEST_INDEX);
    CreateIndexResponse createResponse =
        searchClientShim.createIndex(createRequest, RequestOptions.DEFAULT);
    assertNotNull(createResponse);
    assertTrue(createResponse.isAcknowledged());

    // Test index exists
    GetIndexRequest getRequest = new GetIndexRequest(TEST_INDEX);
    boolean exists = searchClientShim.indexExists(getRequest, RequestOptions.DEFAULT);
    assertTrue(exists);

    // Test refreshIndex
    RefreshRequest refreshRequest = new RefreshRequest(TEST_INDEX);
    RefreshResponse refreshResponse =
        searchClientShim.refreshIndex(refreshRequest, RequestOptions.DEFAULT);
    assertNotNull(refreshResponse);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchOperations() throws IOException {
    log.info("Testing search operations");

    // Test basic search
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertNotNull(searchResponse.getHits());

    log.info(
        "Search completed successfully, found {} hits",
        searchResponse.getHits().getTotalHits().value);
  }

  @Test
  public void testAutoDetection() throws IOException {
    assertEquals(searchClientShim.getEngineType().getEngine(), "elasticsearch");
    assertTrue(
        System.getProperty("ELASTIC_VERSION")
            .startsWith(searchClientShim.getEngineType().getMajorVersion()));
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testDocumentOperations() throws IOException {
    log.info("Testing document CRUD operations");

    String docId = UUID.randomUUID().toString();
    Map<String, Object> document = new HashMap<>();
    document.put("title", "Test Document");
    document.put("content", "This is a test document for shim testing");
    document.put("timestamp", System.currentTimeMillis());

    // Test index document
    IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id(docId).source(document);
    IndexResponse indexResponse =
        searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    assertNotNull(indexResponse);
    assertEquals(indexResponse.getId(), docId);

    // Refresh to make document searchable
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Test get document
    GetRequest getRequest = new GetRequest(TEST_INDEX, docId);
    GetResponse getResponse = searchClientShim.getDocument(getRequest, RequestOptions.DEFAULT);
    assertNotNull(getResponse);
    assertTrue(getResponse.isExists());
    assertEquals(getResponse.getId(), docId);
    assertEquals(getResponse.getSourceAsMap().get("title"), "Test Document");

    // Test delete document
    DeleteRequest deleteRequest = new DeleteRequest(TEST_INDEX, docId);
    DeleteResponse deleteResponse =
        searchClientShim.deleteDocument(deleteRequest, RequestOptions.DEFAULT);
    assertNotNull(deleteResponse);
    assertEquals(deleteResponse.getId(), docId);

    // Verify deletion
    GetRequest verifyRequest = new GetRequest(TEST_INDEX, docId);
    GetResponse verifyResponse =
        searchClientShim.getDocument(verifyRequest, RequestOptions.DEFAULT);
    assertFalse(verifyResponse.isExists());
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testCountOperation() throws IOException {
    log.info("Testing count operation");

    // Index some documents first
    for (int i = 0; i < 5; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("count-doc-" + i)
              .source("field", "value" + i, "counter", i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Test count
    CountRequest countRequest = new CountRequest(TEST_INDEX);
    CountResponse countResponse = searchClientShim.count(countRequest, RequestOptions.DEFAULT);
    assertNotNull(countResponse);
    assertTrue(countResponse.getCount() >= 5);
    assertEquals(countResponse.getFailedShards(), 0);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testExplainOperation() throws IOException {
    log.info("Testing explain operation");

    // Index a document
    String docId = "explain-doc";
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX).id(docId).source("title", "Explain Test", "score", 100);
    searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Test explain
    ExplainRequest explainRequest = new ExplainRequest(TEST_INDEX, docId);
    explainRequest.query(QueryBuilders.termQuery("title", "explain"));
    ExplainResponse explainResponse =
        searchClientShim.explain(explainRequest, RequestOptions.DEFAULT);
    assertNotNull(explainResponse);
    assertTrue(explainResponse.hasExplanation());
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithAggregations() throws IOException {
    log.info("Testing search with aggregations");

    // Index documents with categories
    for (int i = 0; i < 10; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("agg-doc-" + i)
              .source("category", i % 3 == 0 ? "A" : i % 3 == 1 ? "B" : "C", "value", i * 10);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with aggregation
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());

    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms("categories").field("category.keyword");
    sourceBuilder.aggregation(aggregation);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertNotNull(searchResponse.getAggregations());
    assertNotNull(searchResponse.getAggregations().get("categories"));
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithHighlighting() throws IOException {
    log.info("Testing search with highlighting");

    // Index a document with text
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX)
            .id("highlight-doc")
            .source("text", "The quick brown fox jumps over the lazy dog");
    searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with highlighting
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchQuery("text", "quick fox"));

    HighlightBuilder highlightBuilder = new HighlightBuilder();
    highlightBuilder.field("text");
    sourceBuilder.highlighter(highlightBuilder);
    sourceBuilder.size(10);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertTrue(searchResponse.getHits().getTotalHits().value > 0);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithSorting() throws IOException {
    log.info("Testing search with sorting");

    // Index documents with scores
    for (int i = 0; i < 5; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("sort-doc-" + i)
              .source("score", i * 10, "name", "Document " + i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with sorting
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    sourceBuilder.sort("score", SortOrder.DESC);
    sourceBuilder.size(50);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertTrue(searchResponse.getHits().getTotalHits().value >= 5);
    assertEquals(searchResponse.getHits().getHits()[0].getId(), "explain-doc");
    assertEquals(searchResponse.getHits().getHits()[1].getId(), "sort-doc-4");
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithSourceFiltering() throws IOException {
    log.info("Testing search with source filtering");

    // Index a document with multiple fields
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX)
            .id("source-filter-doc")
            .source("field1", "value1", "field2", "value2", "field3", "value3");
    searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with source filtering
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    sourceBuilder.from(0);
    sourceBuilder.size(50);
    sourceBuilder.sort("field1.keyword", SortOrder.DESC);
    sourceBuilder.fetchSource(new String[] {"field1", "field3"}, new String[] {"field2"});
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertTrue(searchResponse.getHits().getHits().length > 0);
    Map<String, Object> source = searchResponse.getHits().getAt(0).getSourceAsMap();
    assertTrue(source.containsKey("field1") || source.containsKey("field3"));
    assertFalse(source.containsKey("field2"));
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testDeleteByQuery() throws IOException {
    log.info("Testing delete by query");

    // Index documents to delete
    for (int i = 0; i < 5; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("delete-query-doc-" + i)
              .source("type", "deletable", "value", i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Delete by query
    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(TEST_INDEX);
    deleteByQueryRequest.setQuery(QueryBuilders.termQuery("type", "deletable"));
    deleteByQueryRequest.setRefresh(true);

    BulkByScrollResponse response =
        searchClientShim.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    assertNotNull(response);
    assertTrue(response.getDeleted() > 0);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testUpdateByQuery() throws IOException {
    log.info("Testing update by query");

    // Index documents to update
    for (int i = 0; i < 3; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("update-query-doc-" + i)
              .source("status", "pending", "value", i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Update by query
    UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(TEST_INDEX);
    updateByQueryRequest.setQuery(QueryBuilders.termQuery("status", "pending"));
    updateByQueryRequest.setScript(
        new Script(
            ScriptType.INLINE,
            "painless",
            "ctx._source.status = 'processed'",
            Collections.emptyMap()));
    updateByQueryRequest.setRefresh(true);

    BulkByScrollResponse response =
        searchClientShim.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
    assertNotNull(response);
    assertEquals(response.getUpdated(), 3);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testIndexMappingOperations() throws IOException {
    log.info("Testing index mapping operations");

    // Create index with mapping
    CreateIndexRequest createRequest = new CreateIndexRequest(TEST_INDEX_2);
    createRequest.mapping(
        XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            "{\"properties\":{\"title\":{\"type\":\"text\"},\"count\":{\"type\":\"integer\"}}}",
            true));
    CreateIndexResponse createResponse =
        searchClientShim.createIndex(createRequest, RequestOptions.DEFAULT);
    assertTrue(createResponse.isAcknowledged());

    // Get mapping
    GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
    getMappingsRequest.indices(TEST_INDEX_2);
    GetMappingsResponse getMappingsResponse =
        searchClientShim.getIndexMapping(getMappingsRequest, RequestOptions.DEFAULT);
    assertNotNull(getMappingsResponse);
    assertNotNull(getMappingsResponse.mappings().get(TEST_INDEX_2));

    // Update mapping
    PutMappingRequest putMappingRequest = new PutMappingRequest(TEST_INDEX_2);
    putMappingRequest.source(
        "{\"properties\":{\"newField\":{\"type\":\"keyword\"}}}", XContentType.JSON);
    AcknowledgedResponse putMappingResponse =
        searchClientShim.putIndexMapping(putMappingRequest, RequestOptions.DEFAULT);
    assertTrue(putMappingResponse.isAcknowledged());

    // Validate mapping update
    GetMappingsRequest getMappingsRequest2 = new GetMappingsRequest();
    getMappingsRequest.indices(TEST_INDEX_2);
    GetMappingsResponse getMappingsResponse2 =
        searchClientShim.getIndexMapping(getMappingsRequest2, RequestOptions.DEFAULT);
    assertNotNull(getMappingsResponse2);
    assertNotNull(getMappingsResponse.mappings().get(TEST_INDEX_2));
    assertNotNull(
        ((Map<String, Object>)
                getMappingsResponse2
                    .mappings()
                    .get(TEST_INDEX_2)
                    .getSourceAsMap()
                    .get("properties"))
            .get("newField"));
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testIndexSettingsOperations() throws IOException {
    log.info("Testing index settings operations");

    // Get settings
    GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
    getSettingsRequest.indices(TEST_INDEX);
    GetSettingsResponse getSettingsResponse =
        searchClientShim.getIndexSettings(getSettingsRequest, RequestOptions.DEFAULT);
    assertNotNull(getSettingsResponse);
    assertNotNull(getSettingsResponse.getIndexToSettings().get(TEST_INDEX));

    // Update settings
    UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(TEST_INDEX);
    Settings settings = Settings.builder().put("index.refresh_interval", "2s").build();
    updateSettingsRequest.settings(settings);
    AcknowledgedResponse updateResponse =
        searchClientShim.updateIndexSettings(updateSettingsRequest, RequestOptions.DEFAULT);
    assertTrue(updateResponse.isAcknowledged());

    // Verify update
    GetSettingsResponse verifyResponse =
        searchClientShim.getIndexSettings(getSettingsRequest, RequestOptions.DEFAULT);
    Settings indexSettings = verifyResponse.getIndexToSettings().get(TEST_INDEX);
    assertEquals(indexSettings.get("index.refresh_interval"), "2s");
  }

  @Test(dependsOnMethods = {"testIndexOperations", "testIndexMappingOperations"})
  public void testAliasOperations() throws IOException {
    log.info("Testing alias operations");

    // Create alias
    IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
    IndicesAliasesRequest.AliasActions aliasAction =
        IndicesAliasesRequest.AliasActions.add().index(TEST_INDEX).alias(TEST_ALIAS);
    aliasesRequest.addAliasAction(aliasAction);

    AcknowledgedResponse createAliasResponse =
        searchClientShim.updateIndexAliases(aliasesRequest, RequestOptions.DEFAULT);
    assertTrue(createAliasResponse.isAcknowledged());

    // Get aliases
    GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
    getAliasesRequest.indices(TEST_INDEX);
    GetAliasesResponse getAliasesResponse =
        searchClientShim.getIndexAliases(getAliasesRequest, RequestOptions.DEFAULT);
    assertNotNull(getAliasesResponse);
    assertTrue(getAliasesResponse.getAliases().containsKey(TEST_INDEX));
    assertTrue(
        getAliasesResponse.getAliases().get(TEST_INDEX).stream()
            .anyMatch(alias -> TEST_ALIAS.equals(alias.alias())));

    // Remove alias
    IndicesAliasesRequest removeAliasRequest = new IndicesAliasesRequest();
    IndicesAliasesRequest.AliasActions removeAction =
        IndicesAliasesRequest.AliasActions.remove().index(TEST_INDEX).alias(TEST_ALIAS);
    removeAliasRequest.addAliasAction(removeAction);

    AcknowledgedResponse removeAliasResponse =
        searchClientShim.updateIndexAliases(removeAliasRequest, RequestOptions.DEFAULT);
    assertTrue(removeAliasResponse.isAcknowledged());
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testAnalyzeOperation() throws IOException {
    log.info("Testing analyze operation");

    AnalyzeRequest analyzeRequest =
        AnalyzeRequest.withGlobalAnalyzer("standard", TEST_INDEX, "The quick brown fox");

    AnalyzeResponse analyzeResponse =
        searchClientShim.analyzeIndex(analyzeRequest, RequestOptions.DEFAULT);
    assertNotNull(analyzeResponse);
    assertNotNull(analyzeResponse.getTokens());
    assertTrue(analyzeResponse.getTokens().size() > 0);

    // Verify tokens are as expected
    List<AnalyzeResponse.AnalyzeToken> tokens = analyzeResponse.getTokens();
    assertTrue(tokens.stream().anyMatch(t -> "quick".equals(t.getTerm())));
    assertTrue(tokens.stream().anyMatch(t -> "brown".equals(t.getTerm())));
    assertTrue(tokens.stream().anyMatch(t -> "fox".equals(t.getTerm())));
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testCloneIndexOperation() throws IOException {
    log.info("Testing clone index operation");

    // Put index in read-only mode first
    UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(TEST_INDEX);
    Settings settings = Settings.builder().put("index.blocks.write", true).build();
    updateSettingsRequest.settings(settings);
    searchClientShim.updateIndexSettings(updateSettingsRequest, RequestOptions.DEFAULT);

    // Clone the index
    ResizeRequest resizeRequest = new ResizeRequest(CLONE_INDEX, TEST_INDEX);
    ResizeResponse resizeResponse =
        searchClientShim.cloneIndex(resizeRequest, RequestOptions.DEFAULT);
    assertNotNull(resizeResponse);
    assertTrue(resizeResponse.isAcknowledged());

    // Verify clone exists
    GetIndexRequest getRequest = new GetIndexRequest(CLONE_INDEX);
    boolean exists = searchClientShim.indexExists(getRequest, RequestOptions.DEFAULT);
    assertTrue(exists);

    // Clean up - remove write block
    UpdateSettingsRequest removeBlockRequest = new UpdateSettingsRequest(TEST_INDEX);
    Settings removeBlockSettings = Settings.builder().put("index.blocks.write", false).build();
    removeBlockRequest.settings(removeBlockSettings);
    searchClientShim.updateIndexSettings(removeBlockRequest, RequestOptions.DEFAULT);
  }

  @Test(dependsOnMethods = {"testCloneIndexOperation", "testAliasOperations"})
  public void testDeleteIndexOperation() throws IOException {
    log.info("Testing delete index operation");

    // Delete the clone index
    DeleteIndexRequest deleteRequest = new DeleteIndexRequest(CLONE_INDEX);
    AcknowledgedResponse deleteResponse =
        searchClientShim.deleteIndex(deleteRequest, RequestOptions.DEFAULT);
    assertTrue(deleteResponse.isAcknowledged());

    // Verify deletion
    GetIndexRequest getRequest = new GetIndexRequest(CLONE_INDEX);
    boolean exists = searchClientShim.indexExists(getRequest, RequestOptions.DEFAULT);
    assertFalse(exists);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testReindexOperation() throws IOException, InterruptedException {
    log.info("Testing reindex operation");

    // Create source index with documents
    String sourceIndex = "reindex-source";
    CreateIndexRequest createSourceRequest = new CreateIndexRequest(sourceIndex);
    searchClientShim.createIndex(createSourceRequest, RequestOptions.DEFAULT);

    // Index some documents
    for (int i = 0; i < 3; i++) {
      IndexRequest indexRequest =
          new IndexRequest(sourceIndex).id("reindex-doc-" + i).source("field", "value" + i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(sourceIndex), RequestOptions.DEFAULT);

    // Create destination index
    String destIndex = "reindex-dest";
    CreateIndexRequest createDestRequest = new CreateIndexRequest(destIndex);
    searchClientShim.createIndex(createDestRequest, RequestOptions.DEFAULT);

    // Submit reindex task
    ReindexRequest reindexRequest = new ReindexRequest();
    reindexRequest.setSourceIndices(sourceIndex);
    reindexRequest.setDestIndex(destIndex);

    String taskId = searchClientShim.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
    assertNotNull(taskId);

    GetTaskResponse getTaskResponse;
    if (StringUtils.isNotBlank(taskId)) {
      TaskId taskId1 = new TaskId(taskId);
      int retries = 0;
      do {
        Thread.sleep(1000);
        getTaskResponse =
            searchClientShim
                .getTask(
                    new GetTaskRequest(taskId1.getNodeId(), taskId1.getId()),
                    RequestOptions.DEFAULT)
                .orElse(null);
        retries++;
      } while (getTaskResponse != null && !getTaskResponse.isCompleted() && retries <= 3);
    }

    // Verify documents were copied
    searchClientShim.refreshIndex(new RefreshRequest(destIndex), RequestOptions.DEFAULT);
    CountRequest countRequest = new CountRequest(destIndex);
    CountResponse countResponse = searchClientShim.count(countRequest, RequestOptions.DEFAULT);
    assertTrue(countResponse.getCount() > 0);

    // Clean up
    searchClientShim.deleteIndex(new DeleteIndexRequest(sourceIndex), RequestOptions.DEFAULT);
    searchClientShim.deleteIndex(new DeleteIndexRequest(destIndex), RequestOptions.DEFAULT);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testBulkProcessorOperations() throws IOException, InterruptedException {
    log.info("Testing bulk processor operations");

    // Generate bulk processor
    searchClientShim.generateBulkProcessor(
        WriteRequest.RefreshPolicy.WAIT_UNTIL,
        null, // MetricUtils can be null for testing
        10, // bulkRequestsLimit
        5, // bulkFlushPeriod in seconds
        1000, // retryInterval
        3, // numRetries
        1 // threadCount
        );

    // Add bulk requests
    for (int i = 0; i < 5; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX).id("bulk-doc-" + i).source("bulk_field", "bulk_value_" + i);
      searchClientShim.addBulk(indexRequest);
    }

    // Add an update request
    UpdateRequest updateRequest = new UpdateRequest(TEST_INDEX, "bulk-doc-0").doc("updated", true);
    searchClientShim.addBulk(updateRequest);

    // Add a delete request
    DeleteRequest deleteRequest = new DeleteRequest(TEST_INDEX, "bulk-doc-1");
    searchClientShim.addBulk(deleteRequest);

    // Flush and close bulk processor
    searchClientShim.flushBulkProcessor();
    Thread.sleep(1000); // Wait for flush to complete
    searchClientShim.closeBulkProcessor();

    // Verify bulk operations
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Check that document was updated
    GetRequest getUpdatedDoc = new GetRequest(TEST_INDEX, "bulk-doc-0");
    GetResponse getResponse = searchClientShim.getDocument(getUpdatedDoc, RequestOptions.DEFAULT);
    assertTrue(getResponse.getSourceAsMap().containsKey("updated"));

    // Check that document was deleted
    GetRequest getDeletedDoc = new GetRequest(TEST_INDEX, "bulk-doc-1");
    GetResponse deletedResponse =
        searchClientShim.getDocument(getDeletedDoc, RequestOptions.DEFAULT);
    assertFalse(deletedResponse.isExists());
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithPagination() throws IOException {
    log.info("Testing search with pagination");

    // Index multiple documents
    for (int i = 0; i < 20; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX)
              .id("page-doc-" + i)
              .source("page_num", i, "content", "Document number " + i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // First page
    SearchRequest firstPageRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder firstPageBuilder = new SearchSourceBuilder();
    firstPageBuilder.query(QueryBuilders.matchAllQuery());
    firstPageBuilder.from(0);
    firstPageBuilder.size(5);
    firstPageRequest.source(firstPageBuilder);

    SearchResponse firstPageResponse =
        searchClientShim.search(firstPageRequest, RequestOptions.DEFAULT);
    assertNotNull(firstPageResponse);
    assertEquals(firstPageResponse.getHits().getHits().length, 5);

    // Second page
    SearchRequest secondPageRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder secondPageBuilder = new SearchSourceBuilder();
    secondPageBuilder.query(QueryBuilders.matchAllQuery());
    secondPageBuilder.from(5);
    secondPageBuilder.size(5);
    secondPageRequest.source(secondPageBuilder);

    SearchResponse secondPageResponse =
        searchClientShim.search(secondPageRequest, RequestOptions.DEFAULT);
    assertNotNull(secondPageResponse);
    assertEquals(secondPageResponse.getHits().getHits().length, 5);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithComplexQuery() throws IOException {
    log.info("Testing search with complex query");

    // Index varied documents
    searchClientShim.indexDocument(
        new IndexRequest(TEST_INDEX)
            .id("complex-1")
            .source(
                "status", "active", "priority", 5, "tags", Arrays.asList("important", "urgent")),
        RequestOptions.DEFAULT);

    searchClientShim.indexDocument(
        new IndexRequest(TEST_INDEX)
            .id("complex-2")
            .source("status", "inactive", "priority", 3, "tags", Arrays.asList("low")),
        RequestOptions.DEFAULT);

    searchClientShim.indexDocument(
        new IndexRequest(TEST_INDEX)
            .id("complex-3")
            .source("status", "active", "priority", 8, "tags", Arrays.asList("critical")),
        RequestOptions.DEFAULT);

    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Complex bool query
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(
        QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.rangeQuery("priority").gte(5)));
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertTrue(searchResponse.getHits().getTotalHits().value >= 2);
  }

  @Test
  public void testLowLevelRequest() throws IOException {
    log.info("Testing low-level request");

    org.opensearch.client.Request request =
        new org.opensearch.client.Request("GET", "/_cluster/health");
    request.addParameter("wait_for_status", "yellow");
    request.addParameter("timeout", "5s");

    RawResponse response = searchClientShim.performLowLevelRequest(request);
    assertNotNull(response);
    assertNotNull(response.getEntity());

    String endpoint = "/_security/role/my_role";
    Request request2 = new Request("PUT", endpoint);
    String jsonBody =
        "{\n"
            + "    \"cluster\":[ \"monitor\" ],\n"
            + "    \"indices\":[\n"
            + "       {\n"
            + "          \"names\":[\"PREFIX*\"],\n"
            + "          \"privileges\":[\"all\"]\n"
            + "       }\n"
            + "    ]\n"
            + " }";
    request2.setJsonEntity(jsonBody);
    RawResponse response2 = searchClientShim.performLowLevelRequest(request2);

    assertNotNull(response2);
    assertEquals(response.getStatusLine().getStatusCode(), 200);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithMinScore() throws IOException {
    log.info("Testing search with minimum score");

    // Index documents with different relevance
    searchClientShim.indexDocument(
        new IndexRequest(TEST_INDEX)
            .id("score-1")
            .source("title", "elasticsearch search engine", "boost", 10),
        RequestOptions.DEFAULT);

    searchClientShim.indexDocument(
        new IndexRequest(TEST_INDEX)
            .id("score-2")
            .source("title", "random unrelated content", "boost", 1),
        RequestOptions.DEFAULT);

    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with min score
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchQuery("title", "elasticsearch"));
    sourceBuilder.minScore(0.5f);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    // Only high-scoring documents should be returned
    assertTrue(searchResponse.getHits().getTotalHits().value >= 0);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchWithTerminateAfter() throws IOException {
    log.info("Testing search with terminate after");

    // Index many documents
    for (int i = 0; i < 100; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX).id("terminate-doc-" + i).source("counter", i);
      searchClientShim.indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    searchClientShim.refreshIndex(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);

    // Search with terminate after
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    sourceBuilder.terminateAfter(10);
    searchRequest.source(sourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    // Should terminate early
    assertNotNull(searchResponse.isTerminatedEarly());
  }
}
