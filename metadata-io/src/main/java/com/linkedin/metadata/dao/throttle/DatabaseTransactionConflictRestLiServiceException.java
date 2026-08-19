package com.linkedin.metadata.dao.throttle;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Rest.li 503 for exhausted transaction-conflict retries. {@link RestLiServiceException} has no
 * header API; {@link com.linkedin.metadata.filter.RestliThrottleResponseFilter} applies {@link
 * #getResponseHeaders()} to the response.
 */
public class DatabaseTransactionConflictRestLiServiceException extends RestLiServiceException {

  private final DatabaseTransactionConflictException conflict;

  public DatabaseTransactionConflictRestLiServiceException(
      @Nonnull DatabaseTransactionConflictException conflict) {
    super(HttpStatus.S_503_SERVICE_UNAVAILABLE, conflict.getMessage(), conflict);
    this.conflict = conflict;
    setCode(conflict.getCode());
    DataMap errorDetails = new DataMap();
    errorDetails.put("retryable", conflict.isRetryable());
    setErrorDetails(errorDetails);
  }

  @Nonnull
  public DatabaseTransactionConflictException getConflictException() {
    return conflict;
  }

  @Nonnull
  public Map<String, String> getResponseHeaders() {
    return Map.of(
        ThrottleResponseHeaders.RETRY_AFTER, String.valueOf(conflict.getRetryAfterSeconds()));
  }
}
