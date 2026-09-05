package com.linkedin.gms.factory.systemmetadata;

import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

public class PgSystemMetadataBackendGuardTest {

  @Test
  public void rejectsEnabledWithElasticsearchImplementation() {
    expectThrows(
        IllegalStateException.class, () -> PgSystemMetadataBackendGuard.validate(true, false));
  }

  @Test
  public void rejectsPostgresImplementationWhenDisabled() {
    expectThrows(
        IllegalStateException.class, () -> PgSystemMetadataBackendGuard.validate(false, true));
  }

  @Test
  public void allowsDisabledElasticsearchSoT() {
    PgSystemMetadataBackendGuard.validate(false, false);
  }

  @Test
  public void allowsExclusivePostgresSoT() {
    PgSystemMetadataBackendGuard.validate(true, true);
  }
}
