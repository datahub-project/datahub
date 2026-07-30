package com.linkedin.metadata.usage.registry;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.config.usage.overlay.UsageConfigurationOverlay;
import com.linkedin.metadata.config.usage.overlay.UsageOperationCostOverride;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageOperationsRegistryTest {

  @Test
  public void testLoadOssOnly() {
    UsageOperationsRegistry registry = ossRegistry();
    Assert.assertTrue(registry.getOperations().containsKey("metadata_read"));
  }

  @Test
  public void testRequireUnknownKeyThrows() {
    Assert.assertThrows(
        IllegalArgumentException.class, () -> ossRegistry().require("not_a_real_operation"));
  }

  @Test
  public void testRequireByEnum() {
    Assert.assertEquals(
        ossRegistry().require(UsageOperation.METADATA_READ).operation(),
        UsageOperation.METADATA_READ);
  }

  @Test
  public void testOverlayOverridesDefaultCostUnits() {
    UsageOperationsManifest manifest = new UsageOperationsLoader(yamlMapper()).loadBundled();
    UsageConfigurationOverlay overlay =
        new UsageConfigurationOverlay() {
          @Override
          public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
            return Map.of("metadata_write", () -> 99);
          }

          @Override
          public Map<String, UsageOperationsManifest.GraphqlClassification>
              getGraphqlClassificationOverrides() {
            return null;
          }
        };

    UsageOperationsRegistry registry = new UsageOperationsRegistry(manifest, overlay);
    Assert.assertEquals(registry.require("metadata_write").defaultCostUnits(), 99);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testOverlayRejectsUnknownOperationKey() {
    UsageOperationsManifest manifest = new UsageOperationsLoader(yamlMapper()).loadBundled();
    UsageConfigurationOverlay overlay =
        new UsageConfigurationOverlay() {
          @Override
          public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
            return Map.of("unknown_operation", () -> 1);
          }

          @Override
          public Map<String, UsageOperationsManifest.GraphqlClassification>
              getGraphqlClassificationOverrides() {
            return null;
          }
        };

    new UsageOperationsRegistry(manifest, overlay);
  }

  @Test
  public void testIsAllowedForApiWhenUnsetAllowsAll() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    definition.setActivityClass("read");
    definition.setDefaultCostUnits(1);
    manifest.setUsageOperations(Map.of("metadata_read", definition));
    UsageOperationsRegistry registry =
        new UsageOperationsRegistry(manifest, UsageConfigurationOverlay.empty());
    UsageOperationsRegistry.UsageOperationEntry entry = registry.require("metadata_read");
    Assert.assertTrue(entry.requestApis().isEmpty());
    Assert.assertTrue(registry.isAllowedForApi(entry, RequestContext.RequestAPI.GRAPHQL));
    Assert.assertTrue(registry.isAllowedForApi(entry, RequestContext.RequestAPI.OPENAPI));
  }

  @Test
  public void testContributesToActiveWritersRequiresPositiveCost() {
    Assert.assertTrue(ossRegistry().require("metadata_write").contributesToActiveWriters());
    Assert.assertFalse(ossRegistry().require("other_write").contributesToActiveWriters());
  }

  @Test
  public void testMetadataIngestAllowsMessagingApi() {
    UsageOperationsRegistry.UsageOperationEntry entry = ossRegistry().require("metadata_ingest");
    Assert.assertTrue(ossRegistry().isAllowedForApi(entry, RequestContext.RequestAPI.MESSAGING));
  }

  @Test
  public void testMcpQueryAllowsMcpApiOnly() {
    UsageOperationsRegistry.UsageOperationEntry entry = ossRegistry().require("mcp_query");
    Assert.assertTrue(ossRegistry().isAllowedForApi(entry, RequestContext.RequestAPI.MCP));
    Assert.assertFalse(ossRegistry().isAllowedForApi(entry, RequestContext.RequestAPI.OPENAPI));
  }

  @Test
  public void testIsAllowedForApiWhenRestricted() {
    UsageOperationsRegistry.UsageOperationEntry entry = ossRegistry().require("metadata_read");
    Assert.assertTrue(ossRegistry().isAllowedForApi(entry, RequestContext.RequestAPI.RESTLI));
    Assert.assertFalse(ossRegistry().isAllowedForApi(entry, RequestContext.RequestAPI.TEST));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidRequestApiThrowsDuringLoad() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    definition.setActivityClass("read");
    definition.setDefaultCostUnits(1);
    definition.setRequestApis(List.of("not_valid"));
    manifest.setUsageOperations(Map.of("metadata_read", definition));
    new UsageOperationsRegistry(manifest, UsageConfigurationOverlay.empty());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGraphqlBuilderRejectsUnknownOverlayKey() {
    UsageOperationsManifest manifest = new UsageOperationsLoader(yamlMapper()).loadBundled();
    UsageConfigurationOverlay overlay =
        new UsageConfigurationOverlay() {
          @Override
          public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
            return null;
          }

          @Override
          public Map<String, UsageOperationsManifest.GraphqlClassification>
              getGraphqlClassificationOverrides() {
            UsageOperationsManifest.GraphqlClassification graphql =
                new UsageOperationsManifest.GraphqlClassification();
            graphql.setNames(List.of("orphanOp"));
            return Map.of("not_in_manifest", graphql);
          }
        };

    GraphqlUsageClassificationRegistryBuilder.fromManifest(manifest, overlay);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGraphqlBuilderRejectsDuplicateNames() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition readDef = definitionWithName("dupOp");
    UsageOperationsManifest.UsageOperationDefinition writeDef = definitionWithName("dupOp");
    manifest.setUsageOperations(Map.of("metadata_read", readDef, "metadata_write", writeDef));

    GraphqlUsageClassificationRegistryBuilder.fromManifest(manifest);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGraphqlBuilderRejectsInvalidPattern() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    graphql.setPatterns(List.of("["));
    definition.setGraphql(graphql);
    manifest.setUsageOperations(Map.of("metadata_read", definition));

    GraphqlUsageClassificationRegistryBuilder.fromManifest(manifest);
  }

  private static UsageOperationsManifest.UsageOperationDefinition definitionWithName(String name) {
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    graphql.setNames(List.of(name));
    definition.setGraphql(graphql);
    return definition;
  }

  private static UsageOperationsRegistry ossRegistry() {
    return UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
  }

  private static YAMLMapper yamlMapper() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return yamlMapper;
  }
}
