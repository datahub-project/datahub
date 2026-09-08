package com.linkedin.gms.factory.graph;

import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PgGraphBackendGuardTest {

  @Test
  public void rejectsEnabledWithElasticsearchImplementation() {
    expectThrows(IllegalStateException.class, () -> PgGraphBackendGuard.validate(true, false));
  }

  @Test
  public void rejectsPostgresImplementationWhenDisabled() {
    expectThrows(IllegalStateException.class, () -> PgGraphBackendGuard.validate(false, true));
  }

  @Test
  public void allowsDisabledElasticsearchSoT() {
    PgGraphBackendGuard.validate(false, false);
  }

  @Test
  public void allowsExclusivePostgresSoT() {
    PgGraphBackendGuard.validate(true, true);
  }
}
