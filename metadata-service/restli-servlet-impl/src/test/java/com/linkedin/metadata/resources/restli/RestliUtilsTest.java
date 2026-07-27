package com.linkedin.metadata.resources.restli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.datahub.util.exception.DatabaseTransactionConflictException;
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
}
