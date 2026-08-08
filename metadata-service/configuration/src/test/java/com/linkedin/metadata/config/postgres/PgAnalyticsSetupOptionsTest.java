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
}
