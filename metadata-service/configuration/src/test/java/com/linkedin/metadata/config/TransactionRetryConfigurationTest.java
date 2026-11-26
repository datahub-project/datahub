package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class TransactionRetryConfigurationTest {

  @Test
  public void testBuilder_emptyBuild_usesFieldDefaults() {
    TransactionRetryConfiguration config = TransactionRetryConfiguration.builder().build();
    assertEquals(
        config.getBackoffSqlStates(), TransactionRetryConfiguration.DEFAULT_BACKOFF_SQL_STATES);
    assertEquals(
        config.getBackoffVendorCodes(), TransactionRetryConfiguration.DEFAULT_BACKOFF_VENDOR_CODES);
    assertEquals(
        config.getInitialBackoffMs(), TransactionRetryConfiguration.DEFAULT_INITIAL_BACKOFF_MS);
    assertEquals(config.getMaxBackoffMs(), TransactionRetryConfiguration.DEFAULT_MAX_BACKOFF_MS);
    assertEquals(
        config.getRetryAfterSeconds(), TransactionRetryConfiguration.DEFAULT_RETRY_AFTER_SECONDS);
  }

  @Test
  public void testNoArgConstructor_usesFieldDefaults() {
    TransactionRetryConfiguration config = new TransactionRetryConfiguration();
    assertEquals(
        config.getBackoffSqlStates(), TransactionRetryConfiguration.DEFAULT_BACKOFF_SQL_STATES);
    assertEquals(
        config.getBackoffVendorCodes(), TransactionRetryConfiguration.DEFAULT_BACKOFF_VENDOR_CODES);
    assertEquals(
        config.getInitialBackoffMs(), TransactionRetryConfiguration.DEFAULT_INITIAL_BACKOFF_MS);
    assertEquals(config.getMaxBackoffMs(), TransactionRetryConfiguration.DEFAULT_MAX_BACKOFF_MS);
    assertEquals(
        config.getRetryAfterSeconds(), TransactionRetryConfiguration.DEFAULT_RETRY_AFTER_SECONDS);
  }
}
