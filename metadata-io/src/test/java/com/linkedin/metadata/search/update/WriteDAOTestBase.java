package com.linkedin.metadata.search.update;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.elasticsearch.update.OrderedSearchFields;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Slf4j
public abstract class WriteDAOTestBase extends AbstractTestNGSpringContextTests {
  protected static final String TEST_RELATIONSHIP_TYPE = "IsAssociatedWith";

  @Autowired private ESBulkProcessor bulkProcessor;

  protected abstract SearchClientShim<?> getSearchClient();

  protected abstract OperationContext getOperationContext();

  protected abstract ElasticSearchGraphService getGraphService();

  protected abstract ESWriteDAO getEsWriteDAO();

  @Test
  public void testDeleteByQueryAsyncOnGraphIndex() throws Exception {
    String testRunId = UUID.randomUUID().toString();
    String indexName =
        getOperationContext()
            .getSearchContext()
            .getIndexConvention()
            .getIndexName(
                getOperationContext(), SearchComponent.GRAPH, ElasticSearchGraphService.INDEX_NAME);

    // Create fewer edges for async test
    int totalEdges = 8;
    createTestEdges(testRunId, totalEdges);

    // Verify creation
    long initialCount = countTestEdges(indexName, testRunId);
    assertEquals(initialCount, totalEdges);

    // Build query
    TermQueryBuilder query =
        QueryBuilders.termQuery(
            "destination.urn",
            "urn:li:" + DATASET_ENTITY_NAME + ":(urn:li:dataPlatform:test," + testRunId + ",PROD)");

    // Submit async delete
    CompletableFuture<String> taskFuture =
        getEsWriteDAO()
            .deleteByQueryAsync(
                getOperationContext(), SearchComponent.GRAPH, indexName, query, null);
    String taskSubmission = taskFuture.get(30, TimeUnit.SECONDS);

    assertNotNull(taskSubmission);

    // Parse task ID
    String[] taskParts = taskSubmission.split(":");
    TaskId taskId = new TaskId(taskParts[0], Long.parseLong(taskParts[1]));

    GetTaskRequest getTaskRequest = new GetTaskRequest(taskId.getNodeId(), taskId.getId());
    getTaskRequest.setWaitForCompletion(true);
    getTaskRequest.setTimeout(TimeValue.timeValueMinutes(1));
    assertTrue(
        getSearchClient()
            .getTask(getTaskRequest, RequestOptions.DEFAULT)
            .map(response -> response.isCompleted())
            .orElse(false),
        "Async task should complete successfully");

    // Wait and verify deletion
    Thread.sleep(2000);
    long finalCount = countTestEdges(indexName, testRunId);
    assertEquals(finalCount, 0, "All test edges should be deleted asynchronously");
  }

  @Test
  public void testOrderedSearchFieldsOnlyMoveForward() throws Exception {
    // Writes are only allowed to entity indices, so use the ingestion source index.
    String indexName =
        getOperationContext()
            .getSearchContext()
            .getIndexConvention()
            .getIndexName(
                getOperationContext(),
                getOperationContext()
                    .getEntityRegistry()
                    .getEntitySpec(INGESTION_SOURCE_ENTITY_NAME));
    String docId = "ordered-search-fields-" + UUID.randomUUID();
    // As in the index strategies, the rest of the document is written before the script runs.
    getEsWriteDAO()
        .upsertDocument(
            getOperationContext(),
            INGESTION_SOURCE_ENTITY_NAME,
            "{\"urn\":\"urn:li:dataHubIngestionSource:" + docId + "\"}",
            docId);

    applyRunEvent(indexName, docId, 200L, "RUNNING", null, Map.of("status", "RUNNING"));
    // An older run finishing later keeps the newer run's status but records its success.
    applyRunEvent(indexName, docId, 100L, "SUCCESS", 100L, Map.of("status", "SUCCESS"));
    Map<String, Object> afterOlder = document(indexName, docId);
    assertEquals(((Number) afterOlder.get("lastRunRequestedAtMillis")).longValue(), 200L);
    assertEquals(afterOlder.get("lastRunStatus"), "RUNNING");
    assertEquals(((Number) afterOlder.get("lastSuccessfulRunRequestedAtMillis")).longValue(), 100L);
    assertEquals(aspect(afterOlder).get("status"), "RUNNING");

    // A later status of the same run replaces it.
    applyRunEvent(indexName, docId, 200L, "FAILURE", null, Map.of("status", "FAILURE"));
    Map<String, Object> afterSameRun = document(indexName, docId);
    assertEquals(afterSameRun.get("lastRunStatus"), "FAILURE");
    assertEquals(
        ((Number) afterSameRun.get("lastSuccessfulRunRequestedAtMillis")).longValue(), 100L);
    assertEquals(aspect(afterSameRun).get("status"), "FAILURE");
  }

  private void applyRunEvent(
      String indexName,
      String docId,
      long requestedAt,
      String status,
      Long successfulRequestedAt,
      Map<String, Object> aspectFields)
      throws Exception {
    ObjectNode document = new ObjectMapper().createObjectNode();
    document.put("lastRunRequestedAtMillis", requestedAt);
    document.put("lastRunStatus", status);
    if (successfulRequestedAt != null) {
      document.put("lastSuccessfulRunRequestedAtMillis", successfulRequestedAt);
    }
    document
        .putObject("_aspects")
        .set(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, new ObjectMapper().valueToTree(aspectFields));
    OrderedSearchFields.Update update =
        OrderedSearchFields.split(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, document).orElseThrow();
    getEsWriteDAO()
        .applyScriptUpdateByIndexName(
            getOperationContext(),
            indexName,
            docId,
            update.script(),
            update.params(),
            update.upsert());
    bulkProcessor.flushAndWait(Duration.ofSeconds(30));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> aspect(Map<String, Object> document) {
    return (Map<String, Object>)
        ((Map<String, Object>) document.get("_aspects"))
            .get(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME);
  }

  private Map<String, Object> document(String indexName, String docId) throws Exception {
    for (int attempt = 0; attempt < 20; attempt++) {
      GetResponse response =
          getSearchClient()
              .getDocument(
                  getOperationContext(), new GetRequest(indexName, docId), RequestOptions.DEFAULT);
      if (response.isExists()) {
        return response.getSourceAsMap();
      }
      Thread.sleep(250);
    }
    throw new AssertionError("Document " + docId + " was not written to " + indexName);
  }

  private void createTestEdges(String testRunId, int count) throws InterruptedException {
    for (int i = 0; i < count; i++) {
      try {
        // Create unique dataset URN for source
        Urn destUrn =
            Urn.createFromString(
                "urn:li:"
                    + DATASET_ENTITY_NAME
                    + ":(urn:li:dataPlatform:test,"
                    + testRunId
                    + ",PROD)");

        // Create query URN for destination
        Urn sourceUrn =
            Urn.createFromString(
                "urn:li:" + QUERY_ENTITY_NAME + ":test_query_" + testRunId + "_" + i);

        Edge edge =
            new Edge(
                sourceUrn,
                destUrn,
                TEST_RELATIONSHIP_TYPE,
                System.currentTimeMillis(),
                Urn.createFromString("urn:li:corpuser:test"),
                System.currentTimeMillis(),
                Urn.createFromString("urn:li:corpuser:test"),
                Map.of());

        getGraphService().addEdge(getOperationContext(), edge);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create test edge", e);
      }
    }
    bulkProcessor.flush();

    // Wait for indexing
    Thread.sleep(2000);
  }

  private long countTestEdges(String indexName, String testRunId) throws Exception {
    CountRequest countRequest = new CountRequest(indexName);
    countRequest.query(
        QueryBuilders.termQuery(
            "destination.urn",
            "urn:li:"
                + DATASET_ENTITY_NAME
                + ":(urn:li:dataPlatform:test,"
                + testRunId
                + ",PROD)"));

    CountResponse response =
        getSearchClient().count(getOperationContext(), countRequest, RequestOptions.DEFAULT);
    return response.getCount();
  }
}
