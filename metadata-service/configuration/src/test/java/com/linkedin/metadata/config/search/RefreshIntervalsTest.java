package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class RefreshIntervalsTest {

  @Test
  public void testUnsetOverrideJsonIsEmpty() {
    assertTrue(RefreshIntervals.parseSecondsMap(null).isEmpty());
    assertTrue(RefreshIntervals.parseSecondsMap("").isEmpty());
    assertTrue(RefreshIntervals.parseSecondsMap("null").isEmpty());
    assertTrue(RefreshIntervals.parseSecondsMap("#{null}").isEmpty());
  }

  @Test
  public void testFractionalOverrideIsRejected() {
    try {
      RefreshIntervals.parseSecondsMap("{\"dataset\": 1.5}");
      throw new AssertionError("expected a fractional override to fail");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("dataset"));
    }
  }

  @Test
  public void testOverlayMergesServiceFieldsAndMaps() {
    IndexConfiguration defaults =
        IndexConfiguration.builder()
            .refreshIntervals(
                RefreshIntervals.builder()
                    .entitySeconds(3)
                    .graphSeconds(3)
                    .systemMetadataSeconds(3)
                    .timeseriesSeconds(60)
                    .usageSeconds(60)
                    .entities("{\"dataset\": 10}")
                    .build())
            .build();
    SearchClusterIndexSettings overlay =
        SearchClusterIndexSettings.builder()
            .refreshIntervals(
                RefreshIntervals.builder().timeseriesSeconds(5).entities("{\"chart\": 7}").build())
            .build();

    RefreshIntervals merged = overlay.applyTo(defaults).getRefreshIntervals();
    assertEquals(merged.getEntitySeconds(), Integer.valueOf(3));
    assertEquals(merged.getTimeseriesSeconds(), Integer.valueOf(5));
    assertEquals(merged.getUsageSeconds(), Integer.valueOf(60));
    assertEquals(merged.entityOverrides().get("dataset"), Integer.valueOf(10));
    assertEquals(merged.entityOverrides().get("chart"), Integer.valueOf(7));
  }

  @Test
  public void testOverlayAppliesWhenDefaultsAreAbsent() {
    SearchClusterIndexSettings overlay =
        SearchClusterIndexSettings.builder()
            .refreshIntervals(RefreshIntervals.allServices(9))
            .build();
    assertEquals(overlay.applyTo(null).getRefreshIntervals().getUsageSeconds(), Integer.valueOf(9));
  }
}
