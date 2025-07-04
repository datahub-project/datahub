package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.support.WriteRequest;
import org.testng.annotations.Test;

public class BulkListenerTest {

  @Test
  public void testConstructor() {
    BulkListener test = BulkListener.getInstance(mock(MetricUtils.class));
    assertNotNull(test);
    assertEquals(test, BulkListener.getInstance(mock(MetricUtils.class)));
    assertNotEquals(
        test,
        BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE, mock(MetricUtils.class)));
  }

  @Test
  public void testDefaultPolicy() {
    BulkListener test = BulkListener.getInstance(mock(MetricUtils.class));

    BulkRequest mockRequest1 = mock(BulkRequest.class);
    test.beforeBulk(0L, mockRequest1);
    verify(mockRequest1, times(0)).setRefreshPolicy(any(WriteRequest.RefreshPolicy.class));

    BulkRequest mockRequest2 = mock(BulkRequest.class);
    test = BulkListener.getInstance(WriteRequest.RefreshPolicy.IMMEDIATE, mock(MetricUtils.class));
    test.beforeBulk(0L, mockRequest2);
    verify(mockRequest2, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
  }
}
