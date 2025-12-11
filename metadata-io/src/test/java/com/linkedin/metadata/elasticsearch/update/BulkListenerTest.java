/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.elasticsearch.update;

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
}
