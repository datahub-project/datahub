package com.linkedin.metadata.aspect.filter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadContext;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadFilter;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.policy.SystemDataPolicyIndex;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectReadGuardTest {

  private EntityRegistry entityRegistry;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    SystemDataPolicyIndex.clearCache();
    entityRegistry = Mockito.mock(EntityRegistry.class);
    opContext = Mockito.mock(OperationContext.class);
    Mockito.when(opContext.getEntityRegistry()).thenReturn(entityRegistry);
    Mockito.when(opContext.isSystemAuth()).thenReturn(false);
  }

  @Test
  public void testFilterKeysRemovesDeniedKeys() {
    AspectReadFilter denyFilter = denyAllFilter();
    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);

    assertTrue(
        AspectReadGuard.filterKeys(opContext, entityRegistry, Set.of(key), ReadIntent.READ)
            .isEmpty());
  }

  @Test
  public void testFilterAspectNamesRemovesDeniedNames() {
    AspectReadFilter denyFilter = denyAllFilter();
    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    assertTrue(
        AspectReadGuard.filterAspectNames(
                opContext,
                entityRegistry,
                UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"),
                Set.of("schemaMetadata"),
                ReadIntent.READ)
            .isEmpty());
  }

  @Test
  public void testSystemAuthBypassesFiltering() {
    Mockito.when(opContext.isSystemAuth()).thenReturn(true);
    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(key.getUrn());
    aspect.setAspect(key.getAspect());

    Map<EntityAspectIdentifier, EntityAspect> raw = Map.of(key, aspect);
    assertEquals(
        raw, AspectReadGuard.filterBatchGet(opContext, entityRegistry, raw, ReadIntent.READ));
  }

  @Test
  public void testDenyingFilterRemovesAspect() {
    AspectReadFilter denyFilter =
        new AspectReadFilter() {
          private AspectPluginConfig config =
              AspectPluginConfig.builder()
                  .className("DenyFilter")
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

    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(denyFilter));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(key.getUrn());
    aspect.setAspect(key.getAspect());

    Map<EntityAspectIdentifier, EntityAspect> filtered =
        AspectReadGuard.filterBatchGet(
            opContext, entityRegistry, Map.of(key, aspect), ReadIntent.READ);

    assertTrue(filtered.isEmpty());
  }

  @Test
  public void testExistsIntentDefersToReadFilters() {
    AspectReadFilter allowFilter =
        new AspectReadFilter() {
          private AspectPluginConfig config =
              AspectPluginConfig.builder()
                  .className("AllowFilter")
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
            return true;
          }
        };

    Mockito.when(entityRegistry.getAllAspectReadFilters()).thenReturn(List.of(allowFilter));

    EntityAspectIdentifier key =
        new EntityAspectIdentifier(
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)", "schemaMetadata", 0);
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(key.getUrn());
    aspect.setAspect(key.getAspect());

    assertEquals(
        Map.of(key, aspect),
        AspectReadGuard.filterBatchGet(
            opContext, entityRegistry, Map.of(key, aspect), ReadIntent.EXISTS));
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
