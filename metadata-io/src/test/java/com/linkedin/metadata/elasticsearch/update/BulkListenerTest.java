package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkListenerTest {

  @Test
  public void testConstructor() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener test =
        BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);
    assertNotNull(test);
    assertEquals(
        test, BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils));
    assertNotEquals(
        test, BulkListener.getInstance(1, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils));
  }

  @Test
  public void testDefaultPolicy() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener test =
        BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);

    BulkRequest mockRequest1 = mock(BulkRequest.class);
    test.beforeBulk(0L, mockRequest1);
    verify(mockRequest1, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

    BulkRequest mockRequest2 = mock(BulkRequest.class);
    test = BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);
    test.beforeBulk(0L, mockRequest2);
    verify(mockRequest2, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
  }

  @Test
  public void testMissingDocumentItemIncrementsTheUpdateMissingCounter() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener listener = new BulkListener(null, metricUtils);
    BulkRequest request = new BulkRequest().add(new UpdateRequest("datasetindex_v2", "urn-1"));
    BulkItemResponse missing = mock(BulkItemResponse.class);
    when(missing.getIndex()).thenReturn("datasetindex_v2");
    when(missing.getId()).thenReturn("urn-1");
    when(missing.getOpType()).thenReturn(DocWriteRequest.OpType.UPDATE);
    when(missing.status()).thenReturn(RestStatus.NOT_FOUND);
    when(missing.isFailed()).thenReturn(true);
    when(missing.getFailureMessage()).thenReturn("document_missing_exception");
    BulkResponse response = mock(BulkResponse.class);
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {missing});
    when(response.hasFailures()).thenReturn(true);
    when(response.getTook()).thenReturn(TimeValue.timeValueMillis(5));
    when(response.buildFailureMessage()).thenReturn("failure");

    listener.afterBulk(1L, request, response);

    verify(metricUtils).increment(BulkListener.class, "search.semantic.update_missing_document", 1);
  }

  @Test
  public void testMissingDocumentRequestIncrementsTheUpdateMissingCounter() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener listener = new BulkListener(null, metricUtils);
    BulkRequest request = new BulkRequest().add(new UpdateRequest("datasetindex_v2", "urn-1"));

    listener.afterBulk(1L, request, new RuntimeException("document_missing_exception"));

    verify(metricUtils).increment(BulkListener.class, "search.semantic.update_missing_document", 1);
  }
}
