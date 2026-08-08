package com.linkedin.metadata.kafka.listener;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.utils.HookExecutionContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class InstrumentedClientProxyTest {

  interface SampleClient {
    String read();
  }

  @AfterMethod
  public void tearDown() {
    HookExecutionContext.clear();
  }

  @Test
  public void testCountsAndDelegatesWhenInsideHook() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    SampleClient delegate = mock(SampleClient.class);
    when(delegate.read()).thenReturn("value");

    SampleClient proxy =
        InstrumentedClientProxy.wrap(SampleClient.class, delegate, () -> metricUtils);

    HookExecutionContext.set("SampleHook");
    String result = proxy.read();

    assertEquals(result, "value");
    verify(metricUtils)
        .incrementMicrometer(
            eq(MetricUtils.DATAHUB_HOOK_EXTERNAL_READS),
            eq(1.0),
            eq(MetricUtils.HOOK_TAG),
            eq("SampleHook"),
            eq(InstrumentedClientProxy.CLIENT_TAG),
            eq("SampleClient"));
  }

  @Test
  public void testDoesNotCountOutsideHook() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    SampleClient delegate = mock(SampleClient.class);
    when(delegate.read()).thenReturn("value");

    SampleClient proxy =
        InstrumentedClientProxy.wrap(SampleClient.class, delegate, () -> metricUtils);

    String result = proxy.read();

    assertEquals(result, "value");
    verify(metricUtils, never())
        .incrementMicrometer(
            ArgumentMatchers.anyString(), ArgumentMatchers.anyDouble(), ArgumentMatchers.any());
  }
}
