package com.linkedin.metadata.search.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESBulkProcessorTest {

  private RestHighLevelClient mockSearchClient;
  private MetricUtils mockMetricUtils;
  private ESBulkProcessor esBulkProcessor;

  @BeforeMethod
  public void setup() {
    mockSearchClient = mock(RestHighLevelClient.class);
    mockMetricUtils = mock(MetricUtils.class);

    esBulkProcessor =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils)
            .async(false)
            .bulkRequestsLimit(100)
            .bulkFlushPeriod(1)
            .numRetries(3)
            .retryInterval(1L)
            .defaultTimeout(TimeValue.timeValueMinutes(1))
            .threadCount(1)
            .build();
  }

  @Test
  public void testBuilder() {
    assertNotNull(esBulkProcessor);
    assertEquals(esBulkProcessor.getWriteRequestRefreshPolicy(), null);
  }

  @Test
  public void testBuilderWithCustomSettings() {
    ESBulkProcessor customProcessor =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils)
            .async(true)
            .bulkRequestsLimit(500)
            .bulkFlushPeriod(5)
            .numRetries(5)
            .retryInterval(2L)
            .defaultTimeout(TimeValue.timeValueMinutes(2))
            .threadCount(4)
            .build();

    assertNotNull(customProcessor);
  }

  @Test
  public void testBuilderWithMultipleThreads() {
    // Test the new threadCount parameter
    ESBulkProcessor multiThreadProcessor =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils).threadCount(4).build();

    assertNotNull(multiThreadProcessor);
  }

  @Test
  public void testAddRequest() {
    IndexRequest request = new IndexRequest("test_index").id("test_id").source("field", "value");

    ESBulkProcessor result = esBulkProcessor.add(request);

    assertNotNull(result);
    assertEquals(result, esBulkProcessor);
    verify(mockMetricUtils, times(1))
        .increment(ESBulkProcessor.class, "num_elasticSearch_writes", 1);
  }

  @Test
  public void testAddRequestWithUrn() {
    String urn = "urn:li:dataset:1";
    IndexRequest request = new IndexRequest("test_index").id("test_id").source("field", "value");

    ESBulkProcessor result = esBulkProcessor.add(urn, request);

    assertNotNull(result);
    assertEquals(result, esBulkProcessor);
    verify(mockMetricUtils, times(1))
        .increment(ESBulkProcessor.class, "num_elasticSearch_writes", 1);
  }

  @Test
  public void testAddMultipleRequests() {
    IndexRequest request1 =
        new IndexRequest("test_index").id("test_id_1").source("field", "value1");
    IndexRequest request2 =
        new IndexRequest("test_index").id("test_id_2").source("field", "value2");

    esBulkProcessor.add(request1);
    esBulkProcessor.add(request2);

    verify(mockMetricUtils, times(2))
        .increment(ESBulkProcessor.class, "num_elasticSearch_writes", 1);
  }

  @Test
  public void testAddDeleteRequest() {
    DeleteRequest request = new DeleteRequest("test_index", "test_id");

    ESBulkProcessor result = esBulkProcessor.add(request);

    assertNotNull(result);
    assertEquals(result, esBulkProcessor);
    verify(mockMetricUtils, times(1))
        .increment(ESBulkProcessor.class, "num_elasticSearch_writes", 1);
  }

  @Test
  public void testAddUpdateRequest() {
    UpdateRequest request = new UpdateRequest("test_index", "test_id").doc("field", "new_value");

    ESBulkProcessor result = esBulkProcessor.add(request);

    assertNotNull(result);
    assertEquals(result, esBulkProcessor);
    verify(mockMetricUtils, times(1))
        .increment(ESBulkProcessor.class, "num_elasticSearch_writes", 1);
  }

  @Test
  public void testDeleteByQuery() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockSearchClient.deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    var result = esBulkProcessor.deleteByQuery(queryBuilder, "test_index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockResponse);
    verify(mockSearchClient, times(1))
        .deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testDeleteByQueryWithRefresh() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockSearchClient.deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    var result = esBulkProcessor.deleteByQuery(queryBuilder, true, "test_index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockResponse);
    verify(mockSearchClient, times(1))
        .deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testUpdateByQuery() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");
    Script script =
        new Script(
            ScriptType.INLINE,
            "painless",
            "ctx._source.field = 'new_value'",
            java.util.Collections.emptyMap());
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockSearchClient.updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    var result = esBulkProcessor.updateByQuery(script, queryBuilder, "test_index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockResponse);
    verify(mockSearchClient, times(1))
        .updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testUpdateByQueryWithRefresh() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");
    Script script =
        new Script(
            ScriptType.INLINE,
            "painless",
            "ctx._source.field = 'new_value'",
            java.util.Collections.emptyMap());
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);

    when(mockSearchClient.updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    var result = esBulkProcessor.updateByQuery(script, queryBuilder, "test_index");

    assertTrue(result.isPresent());
    assertEquals(result.get(), mockResponse);
    verify(mockSearchClient, times(1))
        .updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testClose() throws IOException {
    esBulkProcessor.close();

    // Verify that close doesn't throw exceptions
    // The actual verification would depend on the implementation details
  }

  @Test
  public void testFlush() throws IOException {
    esBulkProcessor.flush();

    // Verify that flush doesn't throw exceptions
    // The actual verification would depend on the implementation details
  }

  @Test
  public void testGetWriteRequestRefreshPolicy() {
    ESBulkProcessor processorWithPolicy =
        ESBulkProcessor.builder(mockSearchClient, mockMetricUtils)
            .writeRequestRefreshPolicy(
                org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
            .build();

    assertEquals(
        processorWithPolicy.getWriteRequestRefreshPolicy(),
        org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE);
  }

  @Test
  public void testDeleteByQueryWithException() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");

    when(mockSearchClient.deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Test exception"));

    var result = esBulkProcessor.deleteByQuery(queryBuilder, "test_index");

    assertTrue(result.isEmpty());
    verify(mockSearchClient, times(1))
        .deleteByQuery(any(DeleteByQueryRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testUpdateByQueryWithException() throws IOException {
    QueryBuilder queryBuilder = new TermQueryBuilder("field", "value");
    Script script =
        new Script(
            ScriptType.INLINE,
            "painless",
            "ctx._source.field = 'new_value'",
            java.util.Collections.emptyMap());

    when(mockSearchClient.updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Test exception"));

    var result = esBulkProcessor.updateByQuery(script, queryBuilder, "test_index");

    assertTrue(result.isEmpty());
    verify(mockSearchClient, times(1))
        .updateByQuery(any(UpdateByQueryRequest.class), any(RequestOptions.class));
  }
}
