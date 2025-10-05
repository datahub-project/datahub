package com.linkedin.metadata.search.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BulkListenerTest {

  private MetricUtils mockMetricUtils;
  private BulkListener bulkListener;

  @BeforeMethod
  public void setup() {
    mockMetricUtils = mock(MetricUtils.class);
    // Create BulkListener directly with the mock to ensure it's used
    bulkListener = new BulkListener("test-processor", null, mockMetricUtils);
  }

  @Test
  public void testConstructor() {
    assertNotNull(bulkListener);
  }

  @Test
  public void testGetInstanceWithRefreshPolicy() {
    BulkListener listenerWithPolicy =
        BulkListener.getInstance(
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, mockMetricUtils);
    assertNotNull(listenerWithPolicy);
  }

  @Test
  public void testGetInstanceWithProcessorIndex() {
    // Test the new getInstance method with processor index
    BulkListener listenerWithIndex =
        BulkListener.getInstance(
            1, org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, mockMetricUtils);
    assertNotNull(listenerWithIndex);
  }

  @Test
  public void testGetInstanceWithMultipleProcessors() {
    // Test creating multiple listeners with different processor indices
    BulkListener listener1 = BulkListener.getInstance(0, null, mockMetricUtils);
    BulkListener listener2 = BulkListener.getInstance(1, null, mockMetricUtils);
    BulkListener listener3 = BulkListener.getInstance(2, null, mockMetricUtils);

    assertNotNull(listener1);
    assertNotNull(listener2);
    assertNotNull(listener3);

    // They should be different instances
    assertTrue(listener1 != listener2);
    assertTrue(listener2 != listener3);
  }

  @Test
  public void testBeforeBulk() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));
    bulkRequest.add(new DeleteRequest("test_index", "test_id2"));
    bulkRequest.add(new UpdateRequest("test_index", "test_id3").doc("field", "new_value"));

    bulkListener.beforeBulk(1L, bulkRequest);

    // Verify that beforeBulk doesn't throw exceptions
    // The actual verification would depend on the implementation details
    assertNotNull(bulkListener);
  }

  @Test
  public void testAfterBulkSuccess() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));

    BulkResponse bulkResponse = createMockBulkResponse(true);

    bulkListener.afterBulk(1L, bulkRequest, bulkResponse);

    // Verify metrics are recorded for successful operations using the actual metric naming scheme
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "index_created", 1);
  }

  @Test
  public void testAfterBulkFailure() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));

    BulkResponse bulkResponse = createMockBulkResponse(false);

    bulkListener.afterBulk(1L, bulkRequest, bulkResponse);

    // Verify metrics are recorded for failed operations using the actual metric naming scheme
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "index_bad_request", 1);
  }

  @Test
  public void testAfterBulkWithMixedResults() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));
    bulkRequest.add(new IndexRequest("test_index").id("test_id2").source("field", "value2"));

    BulkResponse bulkResponse = createMockBulkResponseWithMixedResults();

    bulkListener.afterBulk(1L, bulkRequest, bulkResponse);

    // Verify metrics are recorded for both successful and failed operations using the actual metric
    // naming scheme
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "index_created", 1);
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "index_bad_request", 1);
  }

  @Test
  public void testAfterBulkWithEmptyRequest() {
    BulkRequest bulkRequest = new BulkRequest();
    BulkResponse bulkResponse = createMockBulkResponseWithEmptyItems();

    bulkListener.afterBulk(1L, bulkRequest, bulkResponse);

    // Verify no metrics are recorded for empty requests
    verify(mockMetricUtils, never()).increment(BulkListener.class, "index_created", 1);
  }

  @Test
  public void testAfterBulkWithMultipleOperations() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));
    bulkRequest.add(new DeleteRequest("test_index", "test_id2"));
    bulkRequest.add(new UpdateRequest("test_index", "test_id3").doc("field", "new_value"));

    BulkResponse bulkResponse = createMockBulkResponseWithMultipleOperations();

    bulkListener.afterBulk(1L, bulkRequest, bulkResponse);

    // Verify metrics are recorded for all operations using the actual metric naming scheme
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "index_created", 1);
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "delete_ok", 1);
    verify(mockMetricUtils, times(1)).increment(BulkListener.class, "update_ok", 1);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testAfterBulkWithNullResponse() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));

    // Test with null response - should throw NullPointerException
    bulkListener.afterBulk(1L, bulkRequest, (BulkResponse) null);
  }

  @Test
  public void testAfterBulkWithException() {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("test_index").id("test_id").source("field", "value"));

    Exception testException = new RuntimeException("Test exception");

    bulkListener.afterBulk(1L, bulkRequest, testException);

    // Verify metrics are recorded for failed operations due to exceptions using the actual metric
    // naming scheme
    verify(mockMetricUtils, times(1))
        .exceptionIncrement(BulkListener.class, "index_exception", testException);
  }

  @Test
  public void testSingletonBehavior() {
    // Test that the same configuration returns the same instance
    BulkListener listener1 = BulkListener.getInstance(mockMetricUtils);
    BulkListener listener2 = BulkListener.getInstance(mockMetricUtils);

    assertEquals(listener1, listener2);
  }

  @Test
  public void testDifferentConfigurationsReturnDifferentInstances() {
    BulkListener listener1 = BulkListener.getInstance(mockMetricUtils);
    BulkListener listener2 =
        BulkListener.getInstance(
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE, mockMetricUtils);

    assertTrue(listener1 != listener2);
  }

  private BulkResponse createMockBulkResponse(boolean success) {
    BulkResponse mockResponse = mock(BulkResponse.class);
    BulkItemResponse[] items = new BulkItemResponse[1];

    if (success) {
      IndexResponse indexResponse = mock(IndexResponse.class);
      when(indexResponse.getResult())
          .thenReturn(org.opensearch.action.DocWriteResponse.Result.CREATED);
      BulkItemResponse mockItem = mock(BulkItemResponse.class);
      when(mockItem.status()).thenReturn(org.opensearch.core.rest.RestStatus.CREATED);
      when(mockItem.getResponse()).thenReturn(indexResponse);
      when(mockItem.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.INDEX);
      items[0] = mockItem;
    } else {
      BulkItemResponse mockItem = mock(BulkItemResponse.class);
      when(mockItem.status()).thenReturn(org.opensearch.core.rest.RestStatus.BAD_REQUEST);
      when(mockItem.getFailure())
          .thenReturn(
              new BulkItemResponse.Failure(
                  "test_index", "test_id", new RuntimeException("Test failure")));
      when(mockItem.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.INDEX);
      items[0] = mockItem;
    }

    when(mockResponse.getItems()).thenReturn(items);
    when(mockResponse.hasFailures()).thenReturn(!success);
    when(mockResponse.getTook())
        .thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(100));
    when(mockResponse.getIngestTookInMillis()).thenReturn(50L);

    return mockResponse;
  }

  private BulkResponse createMockBulkResponseWithEmptyItems() {
    BulkResponse mockResponse = mock(BulkResponse.class);
    BulkItemResponse[] items = new BulkItemResponse[0]; // Empty array

    when(mockResponse.getItems()).thenReturn(items);
    when(mockResponse.hasFailures()).thenReturn(false);
    when(mockResponse.getTook())
        .thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(100));
    when(mockResponse.getIngestTookInMillis()).thenReturn(50L);

    return mockResponse;
  }

  private BulkResponse createMockBulkResponseWithMultipleOperations() {
    BulkResponse mockResponse = mock(BulkResponse.class);
    BulkItemResponse[] items = new BulkItemResponse[3];

    // First item (INDEX) succeeds
    IndexResponse indexResponse = mock(IndexResponse.class);
    when(indexResponse.getResult())
        .thenReturn(org.opensearch.action.DocWriteResponse.Result.CREATED);
    BulkItemResponse mockItem1 = mock(BulkItemResponse.class);
    when(mockItem1.status()).thenReturn(org.opensearch.core.rest.RestStatus.CREATED);
    when(mockItem1.getResponse()).thenReturn(indexResponse);
    when(mockItem1.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.INDEX);
    items[0] = mockItem1;

    // Second item (DELETE) succeeds
    DeleteResponse deleteResponse = mock(DeleteResponse.class);
    when(deleteResponse.getResult())
        .thenReturn(org.opensearch.action.DocWriteResponse.Result.DELETED);
    BulkItemResponse mockItem2 = mock(BulkItemResponse.class);
    when(mockItem2.status()).thenReturn(org.opensearch.core.rest.RestStatus.OK);
    when(mockItem2.getResponse()).thenReturn(deleteResponse);
    when(mockItem2.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.DELETE);
    items[1] = mockItem2;

    // Third item (UPDATE) succeeds
    UpdateResponse updateResponse = mock(UpdateResponse.class);
    when(updateResponse.getResult())
        .thenReturn(org.opensearch.action.DocWriteResponse.Result.UPDATED);
    BulkItemResponse mockItem3 = mock(BulkItemResponse.class);
    when(mockItem3.status()).thenReturn(org.opensearch.core.rest.RestStatus.OK);
    when(mockItem3.getResponse()).thenReturn(updateResponse);
    when(mockItem3.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.UPDATE);
    items[2] = mockItem3;

    when(mockResponse.getItems()).thenReturn(items);
    when(mockResponse.hasFailures()).thenReturn(false);
    when(mockResponse.getTook())
        .thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(100));
    when(mockResponse.getIngestTookInMillis()).thenReturn(50L);

    return mockResponse;
  }

  private BulkResponse createMockBulkResponseWithMixedResults() {
    BulkResponse mockResponse = mock(BulkResponse.class);
    BulkItemResponse[] items = new BulkItemResponse[2];

    // First item succeeds
    IndexResponse indexResponse = mock(IndexResponse.class);
    when(indexResponse.getResult())
        .thenReturn(org.opensearch.action.DocWriteResponse.Result.CREATED);
    BulkItemResponse mockItem1 = mock(BulkItemResponse.class);
    when(mockItem1.status()).thenReturn(org.opensearch.core.rest.RestStatus.CREATED);
    when(mockItem1.getResponse()).thenReturn(indexResponse);
    when(mockItem1.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.INDEX);
    items[0] = mockItem1;

    // Second item fails
    BulkItemResponse mockItem2 = mock(BulkItemResponse.class);
    when(mockItem2.status()).thenReturn(org.opensearch.core.rest.RestStatus.BAD_REQUEST);
    when(mockItem2.getFailure())
        .thenReturn(
            new BulkItemResponse.Failure(
                "test_index", "test_id2", new RuntimeException("Test failure")));
    when(mockItem2.getOpType()).thenReturn(org.opensearch.action.DocWriteRequest.OpType.INDEX);
    items[1] = mockItem2;

    when(mockResponse.getItems()).thenReturn(items);
    when(mockResponse.hasFailures()).thenReturn(true);
    when(mockResponse.getTook())
        .thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(100));
    when(mockResponse.getIngestTookInMillis()).thenReturn(50L);

    return mockResponse;
  }
}
