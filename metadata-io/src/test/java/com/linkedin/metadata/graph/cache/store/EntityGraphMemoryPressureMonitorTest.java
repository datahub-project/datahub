package com.linkedin.metadata.graph.cache.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import org.testng.annotations.Test;

public class EntityGraphMemoryPressureMonitorTest {

  private static final int TRIGGER = 85;
  private static final int HYSTERESIS = 5;

  @Test
  public void entersPressureAtTriggerThreshold() {
    assertTrue(
        EntityGraphMemoryPressureMonitor.updateUnderPressure(false, TRIGGER, TRIGGER, HYSTERESIS));
  }

  @Test
  public void staysBelowTriggerUntilCrossed() {
    assertFalse(
        EntityGraphMemoryPressureMonitor.updateUnderPressure(
            false, TRIGGER - 1, TRIGGER, HYSTERESIS));
  }

  @Test
  public void holdsPressureInHysteresisBandUntilClearThreshold() {
    assertTrue(
        EntityGraphMemoryPressureMonitor.updateUnderPressure(
            true, TRIGGER - 1, TRIGGER, HYSTERESIS));
    assertTrue(
        EntityGraphMemoryPressureMonitor.updateUnderPressure(
            true,
            EntityGraphMemoryPressureMonitor.clearThresholdPercent(TRIGGER, HYSTERESIS),
            TRIGGER,
            HYSTERESIS));
  }

  @Test
  public void exitsPressureBelowClearThreshold() {
    assertFalse(
        EntityGraphMemoryPressureMonitor.updateUnderPressure(
            true,
            EntityGraphMemoryPressureMonitor.clearThresholdPercent(TRIGGER, HYSTERESIS) - 1,
            TRIGGER,
            HYSTERESIS));
  }

  @Test
  public void disabledConfigDoesNotStartExecutor() {
    EntityGraphCacheProperties properties = memoryPressureProperties(false, "EVICT_LOCAL_LRU");
    EntityGraphCacheService cacheService = mock(EntityGraphCacheService.class);
    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);

    try (EntityGraphMemoryPressureMonitor monitor =
        new EntityGraphMemoryPressureMonitor(properties, cacheService, registry)) {
      // close should be safe when executor was never started
    }
  }

  @Test
  public void checkHeapEvictsAllLocalViewsWhenConfigured() throws Exception {
    EntityGraphCacheProperties properties = memoryPressureProperties(true, "EVICT_ALL_LOCAL");
    EntityGraphCacheService cacheService = mock(EntityGraphCacheService.class);
    EntityGraphLocalViewCache localViews = mock(EntityGraphLocalViewCache.class);
    when(cacheService.getLocalViews()).thenReturn(localViews);
    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);

    try (EntityGraphMemoryPressureMonitor monitor =
        new EntityGraphMemoryPressureMonitor(properties, cacheService, registry)) {
      java.lang.reflect.Method checkHeap =
          EntityGraphMemoryPressureMonitor.class.getDeclaredMethod("checkHeap");
      checkHeap.setAccessible(true);
      checkHeap.invoke(monitor);
    }

    verify(localViews).evictAll();
  }

  @Test
  public void checkHeapEvictsLruPerGraphByDefault() throws Exception {
    EntityGraphCacheProperties properties = memoryPressureProperties(true, "EVICT_LOCAL_LRU");
    EntityGraphCacheService cacheService = mock(EntityGraphCacheService.class);
    EntityGraphLocalViewCache localViews = mock(EntityGraphLocalViewCache.class);
    when(cacheService.getLocalViews()).thenReturn(localViews);
    EntityGraphRegistry registry = mock(EntityGraphRegistry.class);
    when(registry.getGraphsById()).thenReturn(java.util.Map.of("domain", mock()));

    try (EntityGraphMemoryPressureMonitor monitor =
        new EntityGraphMemoryPressureMonitor(properties, cacheService, registry)) {
      java.lang.reflect.Method checkHeap =
          EntityGraphMemoryPressureMonitor.class.getDeclaredMethod("checkHeap");
      checkHeap.setAccessible(true);
      checkHeap.invoke(monitor);
    }

    verify(localViews).evictLruForGraph("domain", 1);
  }

  @Test
  public void clearThresholdNeverNegative() {
    assertEquals(EntityGraphMemoryPressureMonitor.clearThresholdPercent(3, 10), 0);
  }

  private static EntityGraphCacheProperties memoryPressureProperties(
      boolean enabled, String action) {
    return EntityGraphCacheProperties.builder()
        .eviction(
            EntityGraphCacheProperties.Eviction.builder()
                .memoryPressure(
                    EntityGraphCacheProperties.MemoryPressure.builder()
                        .enabled(enabled)
                        .checkIntervalSeconds(3600)
                        .heapUsageThresholdPercent(1)
                        .hysteresisPercent(0)
                        .cooldownSeconds(0)
                        .action(action)
                        .build())
                .build())
        .build();
  }
}
