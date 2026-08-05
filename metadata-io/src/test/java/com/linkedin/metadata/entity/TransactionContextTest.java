package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.ebean.DuplicateKeyException;
import org.testng.annotations.Test;

public class TransactionContextTest {

  @Test
  public void testEmpty_nullMaxRetries_defaultsToThree() {
    TransactionContext ctx = TransactionContext.empty(null);
    assertEquals(ctx.maxRetries(), TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY);
    assertEquals(ctx.maxRetries(), 3);
  }

  @Test
  public void testEmpty_explicitMaxRetries_preserved() {
    assertEquals(TransactionContext.empty(5).maxRetries(), 5);
  }

  @Test
  public void testShouldFallbackToDatabaseMaxVersionRequiresThresholdAndDuplicateKey() {
    TransactionContext tx = TransactionContext.empty();
    assertFalse(tx.shouldFallbackToDatabaseMaxVersion());

    tx.addException(new DuplicateKeyException("dup1", null));
    assertFalse(tx.shouldFallbackToDatabaseMaxVersion());

    tx.addException(new DuplicateKeyException("dup2", null));
    assertFalse(
        tx.shouldFallbackToDatabaseMaxVersion(),
        "threshold is "
            + TransactionContext.DUPLICATE_KEY_MAX_VERSION_FALLBACK_AFTER_FAILURES
            + " DuplicateKey failures");

    tx.addException(new RuntimeException("not duplicate key"));
    assertEquals(tx.getFailedAttempts(), 3);
    assertFalse(
        tx.shouldFallbackToDatabaseMaxVersion(),
        "last exception must be DuplicateKey even after enough failures");

    tx.addException(new DuplicateKeyException("dup3", null));
    assertTrue(tx.shouldFallbackToDatabaseMaxVersion());
  }

  @Test
  public void testSuccessClearsFailuresAndDisablesFallback() {
    TransactionContext tx = TransactionContext.empty();
    for (int i = 0; i < TransactionContext.DUPLICATE_KEY_MAX_VERSION_FALLBACK_AFTER_FAILURES; i++) {
      tx.addException(new DuplicateKeyException("dup" + i, null));
    }
    assertTrue(tx.shouldFallbackToDatabaseMaxVersion());

    tx.success();
    assertEquals(tx.getFailedAttempts(), 0);
    assertFalse(tx.shouldFallbackToDatabaseMaxVersion());
  }
}
