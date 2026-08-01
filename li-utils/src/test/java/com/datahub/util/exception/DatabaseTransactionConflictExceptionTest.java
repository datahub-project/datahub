package com.datahub.util.exception;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.sql.SQLException;
import org.testng.annotations.Test;

public class DatabaseTransactionConflictExceptionTest {

  @Test
  public void testCarriesConflictMetadata() {
    SQLException cause = new SQLException("Deadlock found", "40001", 1213);
    DatabaseTransactionConflictException exception =
        new DatabaseTransactionConflictException("Failed after 3 retries", "40001", cause, 5L);

    assertEquals(exception.getCode(), DatabaseTransactionConflictException.CODE);
    assertEquals(exception.getSqlState(), "40001");
    assertEquals(exception.getRetryAfterSeconds(), 5L);
    assertTrue(exception.isRetryable());
    assertSame(exception.getCause(), cause);
    // Must remain a RetryLimitReached so existing generic handling still catches it.
    assertTrue(exception instanceof RetryLimitReached);
  }

  @Test
  public void testNonPositiveRetryAfterFallsBackToDefault() {
    DatabaseTransactionConflictException zero =
        new DatabaseTransactionConflictException("msg", "40P01", null, 0L);
    assertEquals(
        zero.getRetryAfterSeconds(),
        DatabaseTransactionConflictException.DEFAULT_RETRY_AFTER_SECONDS);

    DatabaseTransactionConflictException negative =
        new DatabaseTransactionConflictException("msg", "40P01", null, -3L);
    assertEquals(
        negative.getRetryAfterSeconds(),
        DatabaseTransactionConflictException.DEFAULT_RETRY_AFTER_SECONDS);
  }

  @Test
  public void testShorterConstructorsDefaultRetryAfterAndAllowNullSqlState() {
    DatabaseTransactionConflictException twoArg =
        new DatabaseTransactionConflictException("msg", null);
    assertNull(twoArg.getSqlState());
    assertEquals(
        twoArg.getRetryAfterSeconds(),
        DatabaseTransactionConflictException.DEFAULT_RETRY_AFTER_SECONDS);

    SQLException cause = new SQLException("serialization failure", "40001");
    DatabaseTransactionConflictException threeArg =
        new DatabaseTransactionConflictException("msg", "40001", cause);
    assertSame(threeArg.getCause(), cause);
    assertEquals(
        threeArg.getRetryAfterSeconds(),
        DatabaseTransactionConflictException.DEFAULT_RETRY_AFTER_SECONDS);
  }
}
