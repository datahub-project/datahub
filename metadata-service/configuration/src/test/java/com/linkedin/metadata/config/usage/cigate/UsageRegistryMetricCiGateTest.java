package com.linkedin.metadata.config.usage.cigate;

import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Always-on usage registry metric emit path validation. */
public class UsageRegistryMetricCiGateTest {

  @Test
  public void testGraphqlClassifiedOperationsEmitSupportedMetrics() {
    UsageOperationsLoader loader = new UsageOperationsLoader(UsageYamlMapper.create());
    UsageMetricEmitPathValidator validator = UsageMetricEmitPathValidator.fromBundled(loader);
    List<String> failures = validator.validateGraphqlClassifiedOperations(loader.loadBundled());
    Assert.assertTrue(
        failures.isEmpty(),
        "GraphQL-classified operations missing metric emit path:\n" + String.join("\n", failures));
  }
}
