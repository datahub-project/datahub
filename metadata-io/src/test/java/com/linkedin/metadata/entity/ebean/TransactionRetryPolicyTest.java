package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.TransactionRetryConfiguration;
import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import org.testng.annotations.Test;

public class TransactionRetryPolicyTest {

  private TransactionRetryPolicy policyWithoutJitter() {
    return new TransactionRetryPolicy(
        TransactionRetryConfiguration.builder()
            .backoffSqlStates("40001,40P01")
            .backoffVendorCodes("1213")
            .initialBackoffMs(50)
            .maxBackoffMs(1000)
            .build(),
        false);
  }

  @Test
  public void testShouldBackoff_Deadlock() {
    TransactionRetryPolicy policy = policyWithoutJitter();
    SQLException sql = new SQLException("deadlock", "40001", 1213);
    assertTrue(policy.shouldBackoff(new PersistenceException(sql)));
  }

  @Test
  public void testShouldBackoff_NonMatch() {
    TransactionRetryPolicy policy = policyWithoutJitter();
    SQLException sql = new SQLException("dup", "23000", 1062);
    assertFalse(policy.shouldBackoff(new PersistenceException(sql)));
  }

  @Test
  public void testBackoffMillis_ExponentialCapped() {
    TransactionRetryPolicy policy = policyWithoutJitter();
    assertEquals(policy.backoffMillis(0), 50L);
    assertEquals(policy.backoffMillis(1), 100L);
    assertEquals(policy.backoffMillis(2), 200L);
    assertEquals(policy.backoffMillis(3), 400L);
    assertEquals(policy.backoffMillis(4), 800L);
    assertEquals(policy.backoffMillis(5), 1000L);
    assertEquals(policy.backoffMillis(10), 1000L);
  }

  @Test
  public void testParseVendorCodes_IgnoresInvalidTokens() {
    TransactionRetryPolicy policy =
        new TransactionRetryPolicy(
            TransactionRetryConfiguration.builder()
                .backoffSqlStates("40001")
                .backoffVendorCodes("1213,not-a-number,42")
                .initialBackoffMs(50)
                .maxBackoffMs(1000)
                .build(),
            false);
    SQLException valid = new SQLException("deadlock", null, 1213);
    assertTrue(policy.shouldBackoff(valid));
    SQLException other = new SQLException("other", null, 42);
    assertTrue(policy.shouldBackoff(other));
    SQLException unmatched = new SQLException("nope", null, 999);
    assertFalse(policy.shouldBackoff(unmatched));
  }

  @Test
  public void testRetryAfterSeconds_fromConfig() {
    TransactionRetryPolicy policy =
        new TransactionRetryPolicy(
            TransactionRetryConfiguration.builder().retryAfterSeconds(7).build(), false);
    assertEquals(policy.getRetryAfterSeconds(), 7L);
  }

  @Test
  public void testRetryAfterSeconds_builderDefault() {
    TransactionRetryPolicy policy =
        new TransactionRetryPolicy(TransactionRetryConfiguration.builder().build(), false);
    assertEquals(
        policy.getRetryAfterSeconds(), TransactionRetryConfiguration.DEFAULT_RETRY_AFTER_SECONDS);
  }
}
