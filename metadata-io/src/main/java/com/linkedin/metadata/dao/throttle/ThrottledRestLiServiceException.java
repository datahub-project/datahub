package com.linkedin.metadata.dao.throttle;

import com.linkedin.metadata.throttle.ThrottleResponseHeaderWriter;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.util.Map;
import javax.annotation.Nonnull;

/** Rest.li 429 response that preserves ingest throttle metadata for response headers. */
public class ThrottledRestLiServiceException extends RestLiServiceException {

  private final APIThrottleException throttleException;

  public ThrottledRestLiServiceException(@Nonnull APIThrottleException throttleException) {
    super(HttpStatus.S_429_TOO_MANY_REQUESTS, throttleException.getMessage(), throttleException);
    this.throttleException = throttleException;
  }

  @Nonnull
  public APIThrottleException getThrottleException() {
    return throttleException;
  }

  @Nonnull
  public Map<String, String> getThrottleResponseHeaders() {
    return ThrottleResponseHeaderWriter.createDenialHeaders(
        throttleException.getRuleId(),
        throttleException.getMechanismType(),
        throttleException.getSource(),
        throttleException.getDurationMs());
  }
}
