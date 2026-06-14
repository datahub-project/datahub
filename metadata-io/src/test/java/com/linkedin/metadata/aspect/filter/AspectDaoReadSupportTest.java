package com.linkedin.metadata.aspect.filter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadContext;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadFilter;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectDaoReadSupportTest {

  private EntityRegistry entityRegistry;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityRegistry = Mockito.mock(EntityRegistry.class);
    opContext = Mockito.mock(OperationContext.class);
    Mockito.when(opContext.getEntityRegistry()).thenReturn(entityRegistry);
    Mockito.when(opContext.isSystemAuth()).thenReturn(false);
  }

  @Test
  public void testBatchGetPreFilterSkipsLoader() {
    AspectReadFilter denyFilter = denyAllFilter();
    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);
    AtomicInteger loadCount = new AtomicInteger();

    Map<EntityAspectIdentifier, EntityAspect> result =
        AspectDaoReadSupport.batchGet(
            opContext,
            Set.of(key),
            false,
            ReadIntent.READ,
            (allowedKeys, forUpdate) -> {
              loadCount.incrementAndGet();
              return Map.of();
            });

    assertTrue(result.isEmpty());
    assertEquals(loadCount.get(), 0);
  }

  @Test
  public void testGetAspectPreFilterSkipsLoader() {
    AspectReadFilter denyFilter = denyAllFilter();
    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);
    AtomicInteger loadCount = new AtomicInteger();

    EntityAspect result =
        AspectDaoReadSupport.getAspect(
            opContext,
            key,
            ReadIntent.READ,
            k -> {
              loadCount.incrementAndGet();
              return new EntityAspect();
            });

    assertNull(result);
    assertEquals(loadCount.get(), 0);
  }

  @Test
  public void testGetAspectsInRangePreFilterSkipsLoader() {
    AspectReadFilter denyFilter = denyAllFilter();
    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    AtomicInteger loadCount = new AtomicInteger();

    List<EntityAspect> result =
        AspectDaoReadSupport.getAspectsInRange(
            opContext,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"),
            Set.of("schemaMetadata"),
            ReadIntent.READ,
            allowedAspectNames -> {
              loadCount.incrementAndGet();
              return List.of();
            });

    assertTrue(result.isEmpty());
    assertEquals(loadCount.get(), 0);
  }

  private static AspectReadFilter denyAllFilter() {
    return new AspectReadFilter() {
      private AspectPluginConfig config =
          AspectPluginConfig.builder()
              .className("DenyAll")
              .enabled(true)
              .supportedOperations(List.of("READ", "EXISTS"))
              .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
              .build();

      @Override
      public AspectPluginConfig getConfig() {
        return config;
      }

      @Override
      public AspectReadFilter setConfig(AspectPluginConfig config) {
        this.config = config;
        return this;
      }

      @Override
      public boolean isAllowed(AspectReadContext context, EntityRegistry registry) {
        return false;
      }
    };
  }
}
