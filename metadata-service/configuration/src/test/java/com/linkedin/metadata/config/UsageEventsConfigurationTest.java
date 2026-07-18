package com.linkedin.metadata.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageEventsConfigurationTest {

  @Test
  public void usePostgresqlFalseWhenImplementationUnset() {
    UsageEventsConfiguration u = new UsageEventsConfiguration();
    Assert.assertFalse(u.usePostgresql());
  }

  @Test
  public void usePostgresqlCaseInsensitive() {
    UsageEventsConfiguration u = new UsageEventsConfiguration();
    u.setImplementation("POSTGRES");
    Assert.assertTrue(u.usePostgresql());
  }
}
