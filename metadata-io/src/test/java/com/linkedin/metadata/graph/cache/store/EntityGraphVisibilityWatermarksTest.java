package com.linkedin.metadata.graph.cache.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class EntityGraphVisibilityWatermarksTest {

  @Test
  public void keepsWritesSortedAndDropsOnesOlderThanTheRetention() {
    long[] result = EntityGraphDistributedStore.addWatermark(new long[] {100L, 500L}, 300L, 200L);

    assertEquals(result, new long[] {300L, 500L});
  }

  @Test
  public void capMergesTheClosestNeighboursSoNoLaterWriteIsLost() {
    long[] watermarks = null;
    for (int i = 0; i < 40; i++) {
      watermarks = EntityGraphDistributedStore.addWatermark(watermarks, 1_000L + i * 10L, 0L);
    }

    assertEquals(watermarks.length, 16);
    assertEquals(watermarks[watermarks.length - 1], 1_000L + 39 * 10L);
    for (int i = 1; i < watermarks.length; i++) {
      assertTrue(watermarks[i] > watermarks[i - 1]);
    }
  }
}
