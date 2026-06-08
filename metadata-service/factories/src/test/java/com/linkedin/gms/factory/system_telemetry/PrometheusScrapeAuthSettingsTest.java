package com.linkedin.gms.factory.system_telemetry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PrometheusScrapeAuthSettingsTest {

  @Test
  public void testAuthDisabledWhenPrometheusExportDisabled() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(false, null, null, null);
    assertFalse(settings.isEnabled());
  }

  @Test
  public void testAuthAutoEnabledWhenPrometheusExportEnabled() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(true, null, null, null);
    assertTrue(settings.isEnabled());
    assertEquals(settings.getUsername(), PrometheusScrapeAuthSettings.DEFAULT_USERNAME);
    assertEquals(settings.getPassword(), PrometheusScrapeAuthSettings.DEFAULT_PASSWORD);
    assertTrue(settings.isUsingDefaultPassword());
  }

  @Test
  public void testExplicitAuthDisabledOverridesPrometheusExport() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(true, false, null, null);
    assertFalse(settings.isEnabled());
  }

  @Test
  public void testExplicitAuthEnabledOverridesPrometheusExport() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(false, true, null, null);
    assertTrue(settings.isEnabled());
  }

  @Test
  public void testCustomCredentialsOverrideDefaults() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(true, null, "scraper", "s3cret");
    assertEquals(settings.getUsername(), "scraper");
    assertEquals(settings.getPassword(), "s3cret");
    assertFalse(settings.isUsingDefaultPassword());
  }

  @Test
  public void testBlankPasswordUsesDefault() {
    PrometheusScrapeAuthSettings settings =
        PrometheusScrapeAuthSettings.resolve(true, null, "scraper", "   ");
    assertEquals(settings.getPassword(), PrometheusScrapeAuthSettings.DEFAULT_PASSWORD);
    assertTrue(settings.isUsingDefaultPassword());
  }
}
