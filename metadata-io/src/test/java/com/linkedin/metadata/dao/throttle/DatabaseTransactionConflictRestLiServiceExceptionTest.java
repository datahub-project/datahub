package com.linkedin.metadata.dao.throttle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.restli.common.HttpStatus;
import java.util.Map;
import org.testng.annotations.Test;

public class DatabaseTransactionConflictRestLiServiceExceptionTest {

  @Test
  public void testWrapsConflictAndBuildsHeaders() {
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");

    DatabaseTransactionConflictRestLiServiceException exception =
        new DatabaseTransactionConflictRestLiServiceException(conflict);

    assertSame(exception.getConflictException(), conflict);
    assertEquals(exception.getStatus(), HttpStatus.S_503_SERVICE_UNAVAILABLE);
    assertEquals(exception.getMessage(), conflict.getMessage());
    assertEquals(exception.getCode(), DatabaseTransactionConflictException.CODE);
    assertEquals(exception.getErrorDetails().get("retryable"), true);

    Map<String, String> headers = exception.getResponseHeaders();
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }
}
