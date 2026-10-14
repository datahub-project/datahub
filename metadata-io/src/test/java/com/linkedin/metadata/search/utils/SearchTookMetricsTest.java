package com.linkedin.metadata.search.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchTookMetricsTest {

  private final SearchRequest request = new SearchRequest("test-index");
  private SimpleMeterRegistry registry;
  private OperationContext opContext;
  private SearchClientShim<?> client;
  private SearchResponse response;

  @BeforeMethod
  public void setup() throws Exception {
    registry = new SimpleMeterRegistry();
    opContext = mock(OperationContext.class);
    when(opContext.getMetricUtils())
        .thenReturn(Optional.of(MetricUtils.builder().registry(registry).build()));
    client = mock(SearchClientShim.class);
    response = mock(SearchResponse.class);
    when(client.search(any(), eq(request), eq(RequestOptions.DEFAULT))).thenReturn(response);
  }

  // Operation names are unique per test because MetricUtils caches timers JVM-wide.
  @Test
  public void testRecordsTookAndOutsideTookTaggedByOperation() throws Exception {
    when(response.getTook()).thenReturn(TimeValue.timeValueMillis(7));

    assertSame(SearchTookMetrics.search(client, opContext, request, "tookTest"), response);

    Timer took = timer(SearchTookMetrics.SEARCH_TOOK_METRIC, "tookTest");
    assertEquals(took.count(), 1);
    assertEquals(took.totalTime(TimeUnit.MILLISECONDS), 7.0);
    assertEquals(timer(SearchTookMetrics.SEARCH_OUTSIDE_TOOK_METRIC, "tookTest").count(), 1);
  }

  @Test
  public void testSkipsMetricsWhenResponseHasNoTook() throws Exception {
    assertSame(SearchTookMetrics.search(client, opContext, request, "noTookTest"), response);

    assertNull(
        registry
            .find(SearchTookMetrics.SEARCH_TOOK_METRIC)
            .tag(SearchTookMetrics.OPERATION_TAG, "noTookTest")
            .timer());
  }

  private Timer timer(String name, String operation) {
    return registry.get(name).tag(SearchTookMetrics.OPERATION_TAG, operation).timer();
  }
}
