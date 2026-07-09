package com.linkedin.gms.factory.usage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.testng.annotations.Test;

/** Guards OSS vs Acryl usage aggregation factory boundaries. */
public class UsageAggregationFactoryPlacementTest {

  private static final Path OSS_FACTORY =
      Path.of("src/main/java/com/linkedin/gms/factory/usage/UsageAggregationFactory.java");
  private static final Path BILLING_FACTORY =
      Path.of(
          "src/main/java/com/linkedin/gms/factory/usage/billing/BillingUsageExtensionFactory.java");

  @Test
  public void usageAggregationFactoryHasNoBillingPackageImports() throws IOException {
    for (String line : Files.readAllLines(OSS_FACTORY)) {
      if (line.startsWith("import ") && line.contains(".usage.billing.")) {
        fail("OSS factory must not import billing package: " + line);
      }
    }
  }

  @Test
  public void billingExtensionFactoryLivesUnderBillingPackage() throws Exception {
    if (!Files.exists(BILLING_FACTORY)) {
      return; // SaaS billing overlay not present in OSS.
    }
    Class<?> billingFactory =
        Class.forName("com.linkedin.gms.factory.usage.billing.BillingUsageExtensionFactory");
    assertTrue(
        billingFactory.getPackageName().endsWith(".usage.billing"),
        "Billing extension factory must live under com.linkedin.gms.factory.usage.billing");
  }

  @Test
  public void billingExtensionRegistersSingleUsageMetricContributorBean() throws IOException {
    if (!Files.exists(BILLING_FACTORY)) {
      return; // SaaS billing overlay not present in OSS.
    }
    List<String> lines = Files.readAllLines(BILLING_FACTORY);
    long contributorBeanMethods =
        lines.stream()
            .filter(line -> line.strip().startsWith("public BillingUsageMetricContributor"))
            .count();
    assertEquals(
        contributorBeanMethods,
        1,
        "Billing extension must register exactly one UsageMetricContributor bean");
    assertFalse(
        lines.stream().anyMatch(line -> line.contains("usageMetricContributor(")),
        "Billing extension must not register a duplicate usageMetricContributor wrapper bean");
  }

  @Test
  public void usageAggregationFactoryRegistersSingleSessionContextEnricherBean()
      throws IOException {
    List<String> lines = Files.readAllLines(OSS_FACTORY);
    long enricherBeanMethods =
        lines.stream()
            .filter(
                line ->
                    line.strip().startsWith("public UsageMetricsSessionEnricher")
                        || line.strip().startsWith("public SessionContextEnricher"))
            .count();
    assertEquals(
        enricherBeanMethods,
        1,
        "Usage aggregation must register exactly one SessionContextEnricher bean");
    assertFalse(
        lines.stream().anyMatch(line -> line.contains("sessionContextEnricher(")),
        "Usage aggregation must not register a duplicate sessionContextEnricher wrapper bean");
  }
}
