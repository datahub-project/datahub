package com.linkedin.metadata.config.usage.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.config.usage.overlay.UsageConfigurationOverlay;
import com.linkedin.metadata.config.usage.overlay.UsageOperationCostOverride;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageGraphqlManifestCollectorTest {

  private static ObjectMapper yamlMapper() {
    return UsageYamlMapper.create();
  }

  @Test
  public void testNamesFromBundledManifest() {
    UsageOperationsManifest manifest = new UsageOperationsLoader(yamlMapper()).loadBundled();
    Set<String> names = UsageGraphqlManifestCollector.names(manifest);
    Assert.assertFalse(names.isEmpty());
    Assert.assertTrue(names.contains("search"));
  }

  @Test
  public void testNamesReturnsEmptyWhenUsageOperationsNull() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    manifest.setUsageOperations(null);
    Assert.assertEquals(UsageGraphqlManifestCollector.names(manifest), Set.of());
  }

  @Test
  public void testOverlayGraphqlNamesCollectsOverrideNames() {
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
            graphql.setNames(List.of("overlayOnlyOp"));
            return Map.of("metadata_read", graphql);
          }
        };

    Set<String> names = UsageGraphqlManifestCollector.overlayGraphqlNames(overlay);
    Assert.assertEquals(names, Set.of("overlayOnlyOp"));
  }

  @Test
  public void testOverlayGraphqlNamesSkipsNullEntries() {
    Map<String, UsageOperationsManifest.GraphqlClassification> overrides = new HashMap<>();
    overrides.put("metadata_read", null);
    UsageConfigurationOverlay overlay =
        new UsageConfigurationOverlay() {
          @Override
          public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
            return null;
          }

          @Override
          public Map<String, UsageOperationsManifest.GraphqlClassification>
              getGraphqlClassificationOverrides() {
            return overrides;
          }
        };

    Assert.assertEquals(UsageGraphqlManifestCollector.overlayGraphqlNames(overlay), Set.of());
  }

  @Test
  public void testValidateGraphqlNamesDisjointPassesWhenNoOverlap() {
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
            graphql.setNames(List.of("uniqueOverlayOnlyName"));
            return Map.of("metadata_read", graphql);
          }
        };

    UsageGraphqlManifestCollector.validateGraphqlNamesDisjoint(manifest, overlay);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateGraphqlNamesDisjointFailsOnOverlap() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    UsageOperationsManifest.GraphqlClassification bundledGraphql =
        new UsageOperationsManifest.GraphqlClassification();
    bundledGraphql.setNames(List.of("sharedName"));
    definition.setGraphql(bundledGraphql);
    manifest.setUsageOperations(Map.of("metadata_read", definition));

    UsageConfigurationOverlay overlay =
        new UsageConfigurationOverlay() {
          @Override
          public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
            return null;
          }

          @Override
          public Map<String, UsageOperationsManifest.GraphqlClassification>
              getGraphqlClassificationOverrides() {
            UsageOperationsManifest.GraphqlClassification overlayGraphql =
                new UsageOperationsManifest.GraphqlClassification();
            overlayGraphql.setNames(List.of("sharedName"));
            return Map.of("metadata_read", overlayGraphql);
          }
        };

    UsageGraphqlManifestCollector.validateGraphqlNamesDisjoint(manifest, overlay);
  }
}
