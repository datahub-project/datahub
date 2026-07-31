package com.linkedin.metadata.resources.restli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.datahub.util.exception.RetryLimitReached;
import com.linkedin.metadata.dao.throttle.DatabaseTransactionConflictRestLiServiceException;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import org.testng.annotations.Test;

public class RestliUtilsTest {

  @Test
  public void testToTask_DatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw conflict;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_DatabaseTransactionConflict_customRetryAfter() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001", null, 9L);

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw conflict;
                    }));

    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "9");
  }

  @Test
  public void testToTask_WrappedDatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40P01");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new RuntimeException("wrapper", conflict);
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_DoubleWrappedDatabaseTransactionConflict_Returns503() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new RuntimeException(
                          "outer", new RuntimeException("inner", conflict));
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(thrown.getCause(), conflict);
    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertTrue(thrown.hasErrorDetails());
    assertEquals(thrown.getErrorDetails().get("retryable"), true);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
    DatabaseTransactionConflictRestLiServiceException conflictEx =
        (DatabaseTransactionConflictRestLiServiceException) thrown;
    assertEquals(
        conflictEx.getResponseHeaders().get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testToTask_GenericRuntime_Returns500() {
    RuntimeException boom = new RuntimeException("boom");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw boom;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testToTask_PlainRetryLimitReached_Returns500_Not503() {
    RetryLimitReached retryExhausted = new RetryLimitReached("Failed to add after 3 retries");

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw retryExhausted;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testToTask_ConflictSubclassOfRetryLimitReached_Returns503() {
    // DatabaseTransactionConflictException extends RetryLimitReached — cause walk must match
    // the subclass before falling through to generic 500.
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");
    RuntimeException wrapper = new RuntimeException("wrapper", conflict);

    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw wrapper;
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertTrue(thrown instanceof DatabaseTransactionConflictRestLiServiceException);
  }
}
