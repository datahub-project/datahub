package com.linkedin.metadata.search.utils;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchTookMetricsTest {

  private MetricUtils metricUtils;
  private SearchResponse response;

  @BeforeMethod
  public void setup() {
    metricUtils = mock(MetricUtils.class);
    response = mock(SearchResponse.class);
  }

  @Test
  public void testRecordsTookAndOutsideTookTaggedByOperation() throws Exception {
    when(response.getTook()).thenReturn(TimeValue.timeValueMillis(7));

    assertSame(
        SearchTookMetrics.time(metricUtils, SearchOperation.ENTITY_SEARCH, () -> response),
        response);

    verify(metricUtils)
        .recordTimer(
            SearchTookMetrics.SEARCH_TOOK_METRIC,
            TimeUnit.MILLISECONDS.toNanos(7),
            SearchTookMetrics.OPERATION_TAG,
            "entitySearch");
    verify(metricUtils)
        .recordTimer(
            eq(SearchTookMetrics.SEARCH_OUTSIDE_TOOK_METRIC),
            anyLong(),
            eq(SearchTookMetrics.OPERATION_TAG),
            eq("entitySearch"));
  }

  @Test
  public void testSkipsMetricsWhenResponseHasNoTook() throws Exception {
    assertSame(
        SearchTookMetrics.time(metricUtils, SearchOperation.ENTITY_SEARCH, () -> response),
        response);

    verifyNoInteractions(metricUtils);
  }
}
