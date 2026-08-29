package com.linkedin.metadata.systemmetadata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.cache.KeyAspectEntityCountCacheConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCache;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KeyAspectEntityCountServiceTest {

  private EntityRegistry entityRegistry;
  private SystemMetadataService systemMetadataService;
  private KeyAspectEntityCountService service;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    entityRegistry = mock(EntityRegistry.class);
    systemMetadataService = mock(SystemMetadataService.class);
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(false);
    KeyAspectEntityCountCache cache = new KeyAspectEntityCountCache(cacheConfig, null);
    service = new KeyAspectEntityCountService(entityRegistry, systemMetadataService, cache, 200);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    EntitySpec chartSpec = mock(EntitySpec.class);
    when(chartSpec.getKeyAspectName()).thenReturn("chartKey");
    EntitySpec datasetSpec = mock(EntitySpec.class);
    when(datasetSpec.getKeyAspectName()).thenReturn("datasetKey");
    when(entityRegistry.getEntitySpec("chart")).thenReturn(chartSpec);
    when(entityRegistry.getEntitySpec("dataset")).thenReturn(datasetSpec);
    when(entityRegistry.getEntitySpecs())
        .thenReturn(Map.of("chart", chartSpec, "dataset", datasetSpec));
  }

  @Test
  public void getCountsUsesBatchQueryForMultipleTypes() {
    when(systemMetadataService.countByKeyAspects(
            eq(opContext), eq(List.of("chartKey", "datasetKey"))))
        .thenReturn(
            Map.of(
                "chartKey", KeyAspectCount.builder().activeCount(2L).softDeletedCount(1L).build(),
                "datasetKey",
                    KeyAspectCount.builder().activeCount(5L).softDeletedCount(0L).build()));

    KeyAspectEntityCountResult result =
        service.getCounts(opContext, List.of("dataset", "chart"), true);

    assertEquals(result.getCounts().size(), 2);
    assertEquals(result.getRequestedTypes(), List.of("chart", "dataset"));
    verify(systemMetadataService).countByKeyAspects(opContext, List.of("chartKey", "datasetKey"));
  }

  @Test
  public void getCountForEntityTypeUsesSingleAspectQuery() {
    when(systemMetadataService.countByKeyAspect(opContext, "chartKey"))
        .thenReturn(KeyAspectCount.builder().activeCount(3L).softDeletedCount(2L).build());

    KeyAspectEntityCountResult result = service.getCountForEntityType(opContext, "chart", true);

    assertEquals(result.getCounts().size(), 1);
    assertEquals(result.getCounts().get(0).getActiveCount(), 3L);
    assertEquals(result.getCounts().get(0).getSoftDeletedCount(), 2L);
    verify(systemMetadataService).countByKeyAspect(opContext, "chartKey");
    verify(systemMetadataService, Mockito.never()).countByKeyAspects(any(), any());
  }

  @Test
  public void unknownEntityTypeThrows() {
    expectThrows(
        IllegalArgumentException.class,
        () -> service.getCounts(opContext, List.of("unknown"), true));
  }

  @Test
  public void getCountsWithNullTypesUsesAllRegistryTypes() {
    when(systemMetadataService.countByKeyAspects(
            eq(opContext), eq(List.of("chartKey", "datasetKey"))))
        .thenReturn(
            Map.of(
                "chartKey", KeyAspectCount.builder().activeCount(1L).softDeletedCount(0L).build(),
                "datasetKey",
                    KeyAspectCount.builder().activeCount(2L).softDeletedCount(0L).build()));

    KeyAspectEntityCountResult result = service.getCounts(opContext, null, true);

    assertEquals(result.getRequestedTypes(), List.of("chart", "dataset"));
    assertEquals(result.getCounts().size(), 2);
  }

  @Test
  public void getCountsNormalizesEntityTypeCase() {
    when(systemMetadataService.countByKeyAspect(opContext, "chartKey"))
        .thenReturn(KeyAspectCount.builder().activeCount(1L).softDeletedCount(0L).build());

    KeyAspectEntityCountResult result =
        service.getCounts(opContext, List.of("Chart", "chart"), true);

    assertEquals(result.getRequestedTypes(), List.of("chart"));
    verify(systemMetadataService).countByKeyAspect(opContext, "chartKey");
  }

  @Test
  public void tooManyEntityTypesThrows() {
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(false);
    KeyAspectEntityCountCache cache = new KeyAspectEntityCountCache(cacheConfig, null);
    service = new KeyAspectEntityCountService(entityRegistry, systemMetadataService, cache, 1);

    expectThrows(
        IllegalArgumentException.class,
        () -> service.getCounts(opContext, List.of("chart", "dataset"), true));
  }

  @Test
  public void missingAspectCountDefaultsToZero() {
    when(systemMetadataService.countByKeyAspects(
            eq(opContext), eq(List.of("chartKey", "datasetKey"))))
        .thenReturn(
            Map.of(
                "chartKey", KeyAspectCount.builder().activeCount(1L).softDeletedCount(0L).build()));

    KeyAspectEntityCountResult result =
        service.getCounts(opContext, List.of("chart", "dataset"), true);

    assertEquals(result.getCounts().get(1).getActiveCount(), 0L);
    assertEquals(result.getCounts().get(1).getSoftDeletedCount(), 0L);
  }

  @Test
  public void cacheEnabledReturnsCachedResult() {
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(true);
    cacheConfig.setTtlSeconds(3600);
    KeyAspectEntityCountCache cache = new KeyAspectEntityCountCache(cacheConfig, null);
    service = new KeyAspectEntityCountService(entityRegistry, systemMetadataService, cache, 200);

    when(systemMetadataService.countByKeyAspect(opContext, "chartKey"))
        .thenReturn(KeyAspectCount.builder().activeCount(9L).softDeletedCount(0L).build());

    KeyAspectEntityCountResult first = service.getCounts(opContext, List.of("chart"), false);
    KeyAspectEntityCountResult second = service.getCounts(opContext, List.of("chart"), false);

    assertFalse(first.isCacheHit());
    assertTrue(second.isCacheHit());
    verify(systemMetadataService, Mockito.times(1)).countByKeyAspect(opContext, "chartKey");
  }
}
