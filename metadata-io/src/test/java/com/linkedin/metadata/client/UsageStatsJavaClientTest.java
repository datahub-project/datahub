package com.linkedin.metadata.client;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.UsageServiceUtil;
import com.linkedin.usage.UsageTimeRange;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageStatsJavaClientTest {

  private TimeseriesAspectService _timeseriesAspectService;
  private UsageClientCacheConfig _usageClientCacheConfig;
  private OperationContext _opContext;
  private MockedStatic<UsageServiceUtil> _utils;
  private Instant _now;
  private Instant _monthAgo;
  private MockedStatic<Instant> _instant;

  @BeforeMethod
  public void setupTest() {
    _timeseriesAspectService = Mockito.mock(TimeseriesAspectService.class);
    _usageClientCacheConfig = Mockito.mock(UsageClientCacheConfig.class);
    _opContext = Mockito.mock(OperationContext.class);
    _utils = Mockito.mockStatic(UsageServiceUtil.class);
    _now = Instant.now(Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC));
    _monthAgo = Instant.now(Clock.fixed(Instant.parse("2024-12-01T00:00:00Z"), ZoneOffset.UTC));
    _instant = Mockito.mockStatic(Instant.class);
  }

  @AfterMethod
  public void closeTest() {
    _utils.close();
    _instant.close();
  }

  @Test
  public void testQueryRangeShouldBeCalledWhenNoStartTimeMillisProvided() throws IOException {
    _instant.when(Instant::now).thenReturn(_now);
    UsageStatsJavaClient client =
        new UsageStatsJavaClient(_timeseriesAspectService, _usageClientCacheConfig);

    try {
      client.getUsageStatsNoCache(_opContext, "resource", UsageTimeRange.MONTH, null, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get test results", e);
    }

    _utils.verify(
        () ->
            UsageServiceUtil.queryRange(
                Mockito.eq(_opContext),
                Mockito.eq(_timeseriesAspectService),
                Mockito.eq("resource"),
                Mockito.eq(WindowDuration.DAY),
                Mockito.eq(UsageTimeRange.MONTH),
                Mockito.eq(null)),
        Mockito.times(1));
  }

  @Test
  public void testQueryShouldBeCalledWhenStartTimeMillisProvided() throws IOException {
    _instant.when(Instant::now).thenReturn(_now);
    UsageStatsJavaClient client =
        new UsageStatsJavaClient(_timeseriesAspectService, _usageClientCacheConfig);

    try {
      client.getUsageStatsNoCache(
          _opContext, "resource", UsageTimeRange.MONTH, _monthAgo.toEpochMilli(), null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get test results", e);
    }

    _utils.verify(
        () ->
            UsageServiceUtil.query(
                Mockito.eq(_opContext),
                Mockito.eq(_timeseriesAspectService),
                Mockito.eq("resource"),
                Mockito.eq(WindowDuration.DAY),
                Mockito.eq(_monthAgo.toEpochMilli()),
                Mockito.eq(_now.toEpochMilli()),
                Mockito.eq(null),
                Mockito.eq(null)),
        Mockito.times(1));
  }
}
