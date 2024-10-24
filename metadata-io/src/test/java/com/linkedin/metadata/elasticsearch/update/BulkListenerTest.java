package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.mockito.Mockito;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DocumentMissingException;
import org.testng.annotations.Test;

public class BulkListenerTest {

  @Test
  public void testConstructor() {
    BulkListener test = BulkListener.getInstance();
    assertNotNull(test);
    assertEquals(test, BulkListener.getInstance());
    assertNotEquals(test, BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE));
  }

  @Test
  public void testDefaultPolicy() {
    BulkListener test = BulkListener.getInstance();

    BulkRequest mockRequest1 = Mockito.mock(BulkRequest.class);
    test.beforeBulk(0L, mockRequest1);
    verify(mockRequest1, times(0)).setRefreshPolicy(any(WriteRequest.RefreshPolicy.class));

    BulkRequest mockRequest2 = Mockito.mock(BulkRequest.class);
    test = BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE);
    test.beforeBulk(0L, mockRequest2);
    verify(mockRequest2, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
  }

  @Test
  public void testException() {
    BulkListener test = BulkListener.getInstance();

    BulkRequest mockRequest = Mockito.mock(BulkRequest.class);
    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    BulkItemResponse bulkItemResponse = Mockito.mock(BulkItemResponse.class);
    BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {bulkItemResponse};
    BulkItemResponse.Failure mockFailure = Mockito.mock(BulkItemResponse.Failure.class);
    DocumentMissingException mockException = Mockito.mock(DocumentMissingException.class);
    Mockito.when(mockResponse.hasFailures()).thenReturn(true);
    Mockito.when(mockResponse.getItems()).thenReturn(bulkItemResponses);
    Mockito.when(bulkItemResponse.getFailure()).thenReturn(mockFailure);
    Mockito.when(mockFailure.getCause()).thenReturn(mockException);
    Mockito.when(bulkItemResponse.getOpType()).thenReturn(DocWriteRequest.OpType.UPDATE);
    Mockito.when(bulkItemResponse.status()).thenReturn(RestStatus.NOT_FOUND);
    Mockito.when(mockResponse.getTook()).thenReturn(new TimeValue(1L));
    test.afterBulk(0L, mockRequest, mockResponse);
    String metricName =
        DocWriteRequest.OpType.UPDATE.getLowercase()
            + "_"
            + RestStatus.NOT_FOUND.name().toLowerCase()
            + "_"
            + "document_missing";
    assertEquals(MetricUtils.counter(BulkListener.class, metricName).getCount(), 1L);
  }
}
