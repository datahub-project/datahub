package com.linkedin.metadata.config.usage.cigate;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageRegistryGateChangeMatcherTest {

  private static final UsageRegistryCiGateProfile PROFILE = OssUsageRegistryCiGateProfile.INSTANCE;

  @Test
  public void testOpenApiGatePathMatchesExemptionSnapshot() {
    Assert.assertTrue(
        UsageRegistryGateChangeMatcher.isOpenApiGatePath(
            "metadata-service/configuration/src/test/resources/openapi_usage_exemptions.snapshot.yaml",
            PROFILE));
  }

  @Test
  public void testGraphqlGatePathMatchesExemptionSnapshot() {
    Assert.assertTrue(
        UsageRegistryGateChangeMatcher.isGraphqlGatePath(
            "metadata-service/configuration/src/test/resources/graphql_usage_exemptions.snapshot.yaml",
            PROFILE));
  }

  @Test
  public void testGraphqlGatePathMatchesUsageOperationsYaml() {
    Assert.assertTrue(
        UsageRegistryGateChangeMatcher.isGraphqlGatePath(
            OssUsageRegistryCiGateProfile.USAGE_OPERATIONS_YAML, PROFILE));
  }

  @Test
  public void testRestLiGatePathMatchesExemptionSnapshot() {
    Assert.assertTrue(
        UsageRegistryGateChangeMatcher.isRestLiGatePath(
            "metadata-service/configuration/src/test/resources/restli_usage_exemptions.snapshot.yaml",
            PROFILE));
  }

  @Test
  public void testOpenApiGatePathIgnoresUnrelatedConfigurationChanges() {
    Assert.assertFalse(
        UsageRegistryGateChangeMatcher.isOpenApiGatePath(
            "metadata-service/configuration/build.gradle", PROFILE));
  }

  @Test
  public void testGraphqlGatePathRequiresGraphqlSuffixForSchemaPaths() {
    Assert.assertTrue(
        UsageRegistryGateChangeMatcher.isGraphqlGatePath(
            "datahub-graphql-core/src/main/resources/schema.graphql", PROFILE));
    Assert.assertFalse(
        UsageRegistryGateChangeMatcher.isGraphqlGatePath(
            "datahub-graphql-core/src/main/resources/application.yaml", PROFILE));
  }
}
