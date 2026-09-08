package com.linkedin.metadata.config.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PgAnalyticsSetupOptionsTest {

  @Test
  public void disabledReturnsNull() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    assertNull(props.buildPgAnalyticsOptions());
  }

  @Test
  public void defaultStoreAndFamilyRouting() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.setSchema("public");
    props.getPgAnalytics().setEnabled(true);
    props.getPgAnalytics().setDefaultStore("default");
    props.getPgAnalytics().setTablePrefix("metadata_analytics");
    props.getPgAnalytics().setInputLagSeconds(900);
    props.getPgAnalytics().getPartitioning().setPartmanPartitionInterval("1 day");
    props.getPgAnalytics().getPartitioning().setPartmanPremake(4);
    props.getPgAnalytics().getRetention().setRawMaxAgeSeconds(7776000);
    props.getPgAnalytics().getRetention().setHourlyMaxAgeSeconds(15552000);
    props.getPgAnalytics().getRetention().setDailyMaxAgeSeconds(46656000);
    props.getPgAnalytics().getRetention().setMonthlyMaxAgeSeconds(94608000);

    props.validateForUse(DatabaseType.POSTGRES);
    PgAnalyticsSetupOptions options = props.buildPgAnalyticsOptions();
    assertNotNull(options);
    assertEquals(options.getDefaultStoreName(), "default");
    assertEquals(
        options.resolveStore("datahub_usage").qualifiedEventTable(),
        "public.metadata_analytics_event");

    props.getPgAnalytics().getRouting().put("api_usage", "missing");
    expectThrows(IllegalStateException.class, () -> props.validateForUse(DatabaseType.POSTGRES));
  }

  @Test
  public void productStore_routesDatahubUsageWithLongerRawRetention() {
    PostgresSqlSetupProperties props = enabledDefaults();
    PostgresSqlSetupProperties.PgAnalytics.StoreConfig product =
        new PostgresSqlSetupProperties.PgAnalytics.StoreConfig();
    product.setTablePrefix("metadata_analytics_product");
    PostgresSqlSetupProperties.PgAnalytics.Retention productRetention =
        new PostgresSqlSetupProperties.PgAnalytics.Retention();
    productRetention.setRawMaxAgeSeconds(31536000);
    productRetention.setHourlyMaxAgeSeconds(15552000);
    productRetention.setDailyMaxAgeSeconds(46656000);
    productRetention.setMonthlyMaxAgeSeconds(94608000);
    product.setRetention(productRetention);
    props.getPgAnalytics().getStores().put("product", product);
    props.getPgAnalytics().getRouting().put("datahub_usage", "product");

    props.validateForUse(DatabaseType.POSTGRES);
    PgAnalyticsSetupOptions options = props.buildPgAnalyticsOptions();
    assertNotNull(options);
    assertEquals(
        options.resolveStore("datahub_usage").qualifiedEventTable(),
        "public.metadata_analytics_product_event");
    assertEquals(options.resolveStore("datahub_usage").getRawMaxAgeSeconds(), 31536000);
    assertEquals(
        options.resolveStore("api_usage").qualifiedEventTable(), "public.metadata_analytics_event");
    assertEquals(options.resolveStore("api_usage").getRawMaxAgeSeconds(), 7776000);
    assertEquals(
        options.resolveStore("system_usage").qualifiedEventTable(),
        "public.metadata_analytics_event");
  }

  @Test
  public void namedStore_partialMaintenanceInheritsCronEnabled() {
    PostgresSqlSetupProperties props = enabledDefaults();
    props.getPgAnalytics().getMaintenance().setCronEnabled(true);
    props.getPgAnalytics().getMaintenance().setIntervalSeconds(3600);

    PostgresSqlSetupProperties.PgAnalytics.StoreConfig named =
        new PostgresSqlSetupProperties.PgAnalytics.StoreConfig();
    PostgresSqlSetupProperties.PgAnalytics.Maintenance m =
        new PostgresSqlSetupProperties.PgAnalytics.Maintenance();
    m.setIntervalSeconds(7200); // cronEnabled left null → inherit true
    named.setMaintenance(m);
    props.getPgAnalytics().getStores().put("other", named);

    // Skip validateForUse — cronEnabled=true also requires pgCron admin JDBC URL.
    PgAnalyticsSetupOptions options = props.buildPgAnalyticsOptions();
    assertNotNull(options);
    assertEquals(options.getStores().get("other").isMaintenanceCronEnabled(), true);
    assertEquals(options.getStores().get("other").getMaintenanceIntervalSeconds(), 7200);
  }

  @Test
  public void namedStore_rejectsInvalidRetention() {
    PostgresSqlSetupProperties props = enabledDefaults();
    PostgresSqlSetupProperties.PgAnalytics.StoreConfig named =
        new PostgresSqlSetupProperties.PgAnalytics.StoreConfig();
    PostgresSqlSetupProperties.PgAnalytics.Retention r =
        new PostgresSqlSetupProperties.PgAnalytics.Retention();
    r.setRawMaxAgeSeconds(30); // below 60 when set
    named.setRetention(r);
    props.getPgAnalytics().getStores().put("other", named);

    expectThrows(IllegalStateException.class, () -> props.validateForUse(DatabaseType.POSTGRES));
  }

  private static PostgresSqlSetupProperties enabledDefaults() {
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.setSchema("public");
    props.getPgAnalytics().setEnabled(true);
    props.getPgAnalytics().setDefaultStore("default");
    props.getPgAnalytics().setTablePrefix("metadata_analytics");
    props.getPgAnalytics().setInputLagSeconds(900);
    props.getPgAnalytics().getPartitioning().setPartmanPartitionInterval("1 day");
    props.getPgAnalytics().getPartitioning().setPartmanPremake(4);
    props.getPgAnalytics().getRetention().setRawMaxAgeSeconds(7776000);
    props.getPgAnalytics().getRetention().setHourlyMaxAgeSeconds(15552000);
    props.getPgAnalytics().getRetention().setDailyMaxAgeSeconds(46656000);
    props.getPgAnalytics().getRetention().setMonthlyMaxAgeSeconds(94608000);
    return props;
  }
}
