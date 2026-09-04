package com.linkedin.gms.factory.analytics;

import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PgAnalyticsBackendGuardTest {

  @Test
  public void rejectsEnabledWithElasticsearchUsageEvents() {
    expectThrows(IllegalStateException.class, () -> PgAnalyticsBackendGuard.validate(true, false));
  }

  @Test
  public void rejectsPostgresUsageEventsWhenDisabled() {
    expectThrows(IllegalStateException.class, () -> PgAnalyticsBackendGuard.validate(false, true));
  }

  @Test
  public void allowsDisabledElasticsearchSoT() {
    PgAnalyticsBackendGuard.validate(false, false);
  }

  @Test
  public void allowsExclusivePostgresSoT() {
    PgAnalyticsBackendGuard.validate(true, true);
  }
}
