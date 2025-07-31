package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESBulkProcessorTest {

  @Mock private RestHighLevelClient mockSearchClient;
  @Mock private MetricUtils mockMetricUtils;
  @Mock private BulkByScrollResponse mockBulkByScrollResponse;
  @Mock private TaskSubmissionResponse mockTaskSubmissionResponse;
  @Mock private BulkResponse mockBulkResponse;

  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testESBulkProcessorBuilder() {
    ESBulkProcessor test = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();
    assertNotNull(test);
  }

  @Test
  public void testESBulkProcessorBuilderWithNullMetrics() {
    // Test that processor works with null MetricUtils
    ESBulkProcessor test = ESBulkProcessor.builder(mockSearchClient, null).build();
    assertNotNull(test);
  }

  @Test
  public void testESBulkProcessorBuilderWithCustomParameters() {
    ESBulkProcessor test =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils)
            .async(true)
            .batchDelete(true)
            .bulkRequestsLimit(1000)
            .bulkFlushPeriod(5)
            .numRetries(5)
            .retryInterval(2L)
            .defaultTimeout(TimeValue.timeValueMinutes(5))
            .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .build();

    assertNotNull(test);
    assertEquals(test.getWriteRequestRefreshPolicy(), WriteRequest.RefreshPolicy.IMMEDIATE);
  }

  @Test
  public void testAddRequest() {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    IndexRequest indexRequest = new IndexRequest("test-index").id("1");
    processor.add(indexRequest);

    verify(mockMetricUtils, times(1))
        .increment(eq(processor.getClass()), eq("num_elasticSearch_writes"), eq(1d));
  }

  @Test
  public void testAddRequestWithNullMetrics() {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, null).build();

    IndexRequest indexRequest = new IndexRequest("test-index").id("1");
    // Should not throw exception even with null metrics
    processor.add(indexRequest);
  }

  @Test
  public void testUpdateByQuerySuccess() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(100L);
    when(mockSearchClient.updateByQuery(
            any(UpdateByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    Script script =
        new Script(ScriptType.INLINE, "painless", "ctx._source.field = 'value'", Map.of());
    QueryBuilder query = QueryBuilders.matchAllQuery();

    Optional<BulkByScrollResponse> result = processor.updateByQuery(script, query, "test-index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockBulkByScrollResponse);
    verify(mockMetricUtils, times(1))
        .increment(eq(processor.getClass()), eq("num_elasticSearch_writes"), eq(100d));
  }

  @Test
  public void testUpdateByQueryFailure() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    IOException exception = new IOException("Update failed");
    when(mockSearchClient.updateByQuery(
            any(UpdateByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(exception);

    Script script =
        new Script(ScriptType.INLINE, "painless", "ctx._source.field = 'value'", Map.of());
    QueryBuilder query = QueryBuilders.matchAllQuery();

    Optional<BulkByScrollResponse> result = processor.updateByQuery(script, query, "test-index");

    assertFalse(result.isPresent());
    verify(mockMetricUtils, times(1))
        .exceptionIncrement(eq(ESBulkProcessor.class), eq("update_by_query"), eq(exception));
  }

  @Test
  public void testUpdateByQueryWithNullMetrics() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, null).build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(100L);
    when(mockSearchClient.updateByQuery(
            any(UpdateByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    Script script =
        new Script(ScriptType.INLINE, "painless", "ctx._source.field = 'value'", Map.of());
    QueryBuilder query = QueryBuilders.matchAllQuery();

    // Should not throw exception even with null metrics
    Optional<BulkByScrollResponse> result = processor.updateByQuery(script, query, "test-index");

    assertTrue(result.isPresent());
  }

  @Test
  public void testDeleteByQuerySuccess() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(50L);
    when(mockSearchClient.deleteByQuery(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    Optional<BulkByScrollResponse> result = processor.deleteByQuery(query, "test-index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockBulkByScrollResponse);
    verify(mockMetricUtils, times(1))
        .increment(eq(processor.getClass()), eq("num_elasticSearch_writes"), eq(50d));
  }

  @Test
  public void testDeleteByQueryFailure() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    IOException exception = new IOException("Delete failed");
    when(mockSearchClient.deleteByQuery(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(exception);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    Optional<BulkByScrollResponse> result = processor.deleteByQuery(query, "test-index");

    assertFalse(result.isPresent());
    verify(mockMetricUtils, times(1))
        .exceptionIncrement(eq(ESBulkProcessor.class), eq("delete_by_query"), eq(exception));
  }

  @Test
  public void testDeleteByQueryWithNullMetrics() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, null).build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(50L);
    when(mockSearchClient.deleteByQuery(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Should not throw exception even with null metrics
    Optional<BulkByScrollResponse> result = processor.deleteByQuery(query, "test-index");

    assertTrue(result.isPresent());
  }

  @Test
  public void testDeleteByQueryWithCustomParameters() throws IOException {
    ESBulkProcessor processor =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils)
            .bulkRequestsLimit(200)
            .numRetries(5)
            .retryInterval(2L)
            .build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(75L);
    when(mockSearchClient.deleteByQuery(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    QueryBuilder query = QueryBuilders.matchAllQuery();
    TimeValue timeout = TimeValue.timeValueMinutes(10);

    Optional<BulkByScrollResponse> result =
        processor.deleteByQuery(query, false, 200, timeout, "test-index");

    assertTrue(result.isPresent());
    verify(mockSearchClient)
        .deleteByQuery(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testDeleteByQueryAsyncSuccess() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockTaskSubmissionResponse);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    Optional<TaskSubmissionResponse> result =
        processor.deleteByQueryAsync(query, true, 100, null, "test-index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockTaskSubmissionResponse);
    verify(mockMetricUtils, times(1))
        .increment(eq(processor.getClass()), eq("num_elasticSearch_batches_submitted"), eq(1d));
  }

  @Test
  public void testDeleteByQueryAsyncFailure() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    IOException exception = new IOException("Submit task failed");
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(exception);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    Optional<TaskSubmissionResponse> result =
        processor.deleteByQueryAsync(query, true, 100, null, "test-index");

    assertFalse(result.isPresent());
    verify(mockMetricUtils, times(1))
        .exceptionIncrement(
            eq(ESBulkProcessor.class), eq("submit_delete_by_query_task"), eq(exception));
  }

  @Test
  public void testDeleteByQueryAsyncWithNullMetrics() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, null).build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockTaskSubmissionResponse);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Should not throw exception even with null metrics
    Optional<TaskSubmissionResponse> result =
        processor.deleteByQueryAsync(query, true, 100, null, "test-index");

    assertTrue(result.isPresent());
  }

  @Test
  public void testDeleteByQueryAsyncWithTimeout() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockTaskSubmissionResponse);

    QueryBuilder query = QueryBuilders.matchAllQuery();
    TimeValue timeout = TimeValue.timeValueMinutes(30);

    Optional<TaskSubmissionResponse> result =
        processor.deleteByQueryAsync(query, false, 500, timeout, "test-index-1", "test-index-2");

    assertTrue(result.isPresent());
    verify(mockSearchClient)
        .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testFlush() {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();
    processor.flush();
    // Verify flush doesn't throw exception
  }

  @Test
  public void testClose() throws IOException {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();
    processor.close();
    // Verify close doesn't throw exception
  }

  @Test
  public void testMultipleOperationsWithMixedRequests() {
    ESBulkProcessor processor = ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).build();

    // Add various types of requests
    processor.add(new IndexRequest("index1").id("1"));
    processor.add(new UpdateRequest("index1", "2"));
    processor.add(new DeleteRequest("index1", "3"));

    verify(mockMetricUtils, times(3))
        .increment(eq(processor.getClass()), eq("num_elasticSearch_writes"), eq(1d));
  }

  @Test
  public void testBatchDeleteBehavior() throws IOException {
    ESBulkProcessor processor =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).batchDelete(true).build();

    when(mockBulkByScrollResponse.getTotal()).thenReturn(25L);
    when(mockSearchClient.deleteByQuery(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockBulkByScrollResponse);

    QueryBuilder query = QueryBuilders.matchAllQuery();

    Optional<BulkByScrollResponse> result = processor.deleteByQuery(query, "test-index");

    assertTrue(result.isPresent());
    // With batchDelete=true, flush should not be called before delete
  }
}
