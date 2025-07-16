package com.linkedin.metadata.search.update;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.QUERY_ENTITY_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@Slf4j
public abstract class WriteDAOTestBase extends AbstractTestNGSpringContextTests {
  protected static final String TEST_RELATIONSHIP_TYPE = "IsAssociatedWith";

  @Autowired private ESBulkProcessor bulkProcessor;

  protected abstract RestHighLevelClient getSearchClient();

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
            .getIndexName(ElasticSearchGraphService.INDEX_NAME);

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
    var taskFuture = getEsWriteDAO().deleteByQueryAsync(indexName, query, null);
    var taskSubmission = taskFuture.get(30, TimeUnit.SECONDS);

    assertNotNull(taskSubmission);
    assertNotNull(taskSubmission.getTask());

    // Parse task ID
    String[] taskParts = taskSubmission.getTask().split(":");
    TaskId taskId = new TaskId(taskParts[0], Long.parseLong(taskParts[1]));

    // Monitor the task
    ESWriteDAO.DeleteByQueryResult monitorResult =
        getEsWriteDAO().monitorDeleteByQueryTask(taskId, Duration.ofMinutes(1), indexName, query);

    assertTrue(monitorResult.isSuccess(), "Async task should complete successfully");

    // Wait and verify deletion
    Thread.sleep(2000);
    long finalCount = countTestEdges(indexName, testRunId);
    assertEquals(finalCount, 0, "All test edges should be deleted asynchronously");
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

        getGraphService().addEdge(edge);
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

    CountResponse response = getSearchClient().count(countRequest, RequestOptions.DEFAULT);
    return response.getCount();
  }
}
