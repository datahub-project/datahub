package com.linkedin.metadata.config.usage.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.manifest.UsageMetricRegistryManifest;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageMetricRegistryLoaderTest {

  @Test
  public void testLoadBundled() {
    UsageMetricRegistryLoader loader = new UsageMetricRegistryLoader(yamlMapper());
    UsageMetricRegistryManifest manifest = loader.loadBundled();
    Assert.assertNotNull(manifest.getMetricRegistry());
    Assert.assertFalse(manifest.getMetricRegistry().isEmpty());
  }

  @Test
  public void testConstructorSetsSnakeCaseWhenUnset() {
    ObjectMapper mapper = new ObjectMapper();
    Assert.assertNull(mapper.getPropertyNamingStrategy());
    new UsageMetricRegistryLoader(mapper);
    Assert.assertNotNull(mapper.getPropertyNamingStrategy());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateRejectsNullMetricRegistry() {
    UsageMetricRegistryManifest manifest = new UsageMetricRegistryManifest();
    manifest.setMetricRegistry(null);
    new UsageMetricRegistryLoader(yamlMapper()).validate(manifest);
  }

  private static ObjectMapper yamlMapper() {
    return UsageYamlMapper.create();
  }
}
