package com.datahub.authentication.token;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class TokenStoreFailuresTest {

  @Test
  public void storeFailuresAnywhereInTheCauseChainAreTransient() {
    assertTrue(TokenStoreFailures.isStoreUnavailable(new PersistenceException("pool exhausted")));
    assertTrue(
        TokenStoreFailures.isStoreUnavailable(
            new RuntimeException("wrapped", new SQLException("Out of memory"))));
    assertTrue(
        TokenStoreFailures.isStoreUnavailable(
            new IllegalStateException("wrapped", new TimeoutException("cache read"))));
  }

  @Test
  public void otherFailuresAreNotTransient() {
    assertFalse(TokenStoreFailures.isStoreUnavailable(null));
    assertFalse(TokenStoreFailures.isStoreUnavailable(new IllegalArgumentException("bad urn")));
    assertFalse(
        TokenStoreFailures.isStoreUnavailable(
            new RuntimeException("corrupt", new IllegalStateException("bad aspect"))));
  }

  @Test
  public void selfReferencingCauseChainTerminates() {
    final RuntimeException a = new RuntimeException("a");
    final RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    assertFalse(TokenStoreFailures.isStoreUnavailable(b));
  }
}
