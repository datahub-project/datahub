package com.linkedin.metadata.config.usage.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageOperationsLoaderTest {

  @Test
  public void testLoadBundled() {
    UsageOperationsLoader loader = new UsageOperationsLoader(yamlMapper());
    UsageOperationsManifest manifest = loader.loadBundled();
    Assert.assertFalse(manifest.getUsageOperations().isEmpty());
  }

  @Test
  public void testConstructorSetsSnakeCaseWhenUnset() {
    ObjectMapper mapper = new ObjectMapper();
    Assert.assertNull(mapper.getPropertyNamingStrategy());
    new UsageOperationsLoader(mapper);
    Assert.assertNotNull(mapper.getPropertyNamingStrategy());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateRejectsEmptyManifest() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    manifest.setUsageOperations(Map.of());
    new UsageOperationsLoader(yamlMapper()).validate(manifest);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateRejectsMissingActivityClass() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    definition.setDefaultCostUnits(1);
    manifest.setUsageOperations(Map.of("metadata_read", definition));
    new UsageOperationsLoader(yamlMapper()).validate(manifest);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateRejectsMissingDefaultCostUnits() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    definition.setActivityClass("read");
    manifest.setUsageOperations(Map.of("metadata_read", definition));
    new UsageOperationsLoader(yamlMapper()).validate(manifest);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateRejectsNegativeDefaultCostUnits() {
    UsageOperationsManifest manifest = new UsageOperationsManifest();
    UsageOperationsManifest.UsageOperationDefinition definition =
        new UsageOperationsManifest.UsageOperationDefinition();
    definition.setActivityClass("read");
    definition.setDefaultCostUnits(-1);
    manifest.setUsageOperations(Map.of("metadata_read", definition));
    new UsageOperationsLoader(yamlMapper()).validate(manifest);
  }

  private static ObjectMapper yamlMapper() {
    return UsageYamlMapper.create();
  }
}
