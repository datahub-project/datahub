package com.linkedin.metadata.client;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.UsageServiceUtil;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
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

  @BeforeMethod
  public void setupTest() {
    _timeseriesAspectService = Mockito.mock(TimeseriesAspectService.class);
    _usageClientCacheConfig = Mockito.mock(UsageClientCacheConfig.class);
    _opContext = Mockito.mock(OperationContext.class);
    _utils = Mockito.mockStatic(UsageServiceUtil.class);
    _now = Instant.parse("2025-01-01T00:00:00Z");
    _monthAgo = Instant.parse("2024-12-01T00:00:00Z");
    // Aligned to a 5m boundary, so canonicalization is a no-op here and these stay delegation
    // assertions rather than rounding assertions.
    Mockito.when(_opContext.canonicalNow()).thenReturn(canonicalizerAt(_now).now());
  }

  /** An enabled canonicalizer pinned to a fixed instant. */
  private static QueryTimeCanonicalizer canonicalizerAt(Instant fixedNow) {
    return QueryTimeCanonicalizer.fromConfig(
        QueryCanonicalizationConfiguration.builder()
            .enabled(true)
            .time(
                TimeCanonicalizationConfiguration.builder()
                    .enabled(true)
                    .bucketSize("5m")
                    .timezone("UTC")
                    .rounding("EXPAND")
                    .build())
            .build(),
        null,
        Clock.fixed(fixedNow, ZoneOffset.UTC));
  }

  @AfterMethod
  public void closeTest() {
    _utils.close();
  }

  @Test
  public void testQueryRangeShouldBeCalledWhenNoStartTimeMillisProvided() throws IOException {
    UsageStatsJavaClient client =
        new UsageStatsJavaClient(_timeseriesAspectService, _usageClientCacheConfig, null);

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
    UsageStatsJavaClient client =
        new UsageStatsJavaClient(_timeseriesAspectService, _usageClientCacheConfig, null);

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
