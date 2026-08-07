package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;

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
}
