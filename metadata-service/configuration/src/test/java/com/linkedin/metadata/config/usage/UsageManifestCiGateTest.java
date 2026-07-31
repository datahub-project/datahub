package com.linkedin.metadata.config.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import io.datahubproject.metadata.context.usage.UsageOperation;
import org.testng.Assert;
import org.testng.annotations.Test;

/** OSS CI gate: UsageOperation enum keys must match usage_operations.yaml. */
public class UsageManifestCiGateTest {

  private static ObjectMapper yamlMapper() {
    return UsageYamlMapper.create();
  }

  @Test
  public void testUsageOperationsYamlMatchesEnum() {
    UsageOperationsLoader loader = new UsageOperationsLoader(yamlMapper());
    UsageOperationsManifest manifest = loader.loadBundled();
    for (UsageOperation operation : UsageOperation.values()) {
      Assert.assertTrue(
          manifest.getUsageOperations().containsKey(operation.key()),
          "Missing usage_operations key for enum: " + operation.key());
    }
    Assert.assertEquals(
        manifest.getUsageOperations().size(),
        UsageOperation.values().length,
        "usage_operations.yaml key count must match UsageOperation enum");
  }

  @Test
  public void testUsageOperationsYamlDefinesDefaultCostUnits() {
    UsageOperationsLoader loader = new UsageOperationsLoader(yamlMapper());
    UsageOperationsManifest manifest = loader.loadBundled();
    for (var entry : manifest.getUsageOperations().entrySet()) {
      Assert.assertNotNull(
          entry.getValue().getDefaultCostUnits(),
          "Missing default_cost_units for " + entry.getKey());
      Assert.assertTrue(
          entry.getValue().getDefaultCostUnits() >= 0,
          "default_cost_units must be >= 0 for " + entry.getKey());
    }
  }
}
