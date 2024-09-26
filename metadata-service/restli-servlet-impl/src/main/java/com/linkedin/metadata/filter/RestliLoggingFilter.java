package com.linkedin.metadata.filter;

import com.linkedin.metadata.restli.NonExceptionHttpErrorResponse;
import com.linkedin.restli.common.HttpMethod;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.filter.Filter;
import com.linkedin.restli.server.filter.FilterRequestContext;
import com.linkedin.restli.server.filter.FilterResponseContext;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestliLoggingFilter implements Filter {

  private static final String START_TIME = "startTime";

  @Override
  public CompletableFuture<Void> onRequest(final FilterRequestContext requestContext) {
    long currentTime = System.currentTimeMillis();
    requestContext.getFilterScratchpad().put(START_TIME, currentTime);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> onResponse(
      final FilterRequestContext requestContext, final FilterResponseContext responseContext) {
    logResponse(requestContext, responseContext);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> onError(
      Throwable th,
      final FilterRequestContext requestContext,
      final FilterResponseContext responseContext) {
    logResponse(requestContext, responseContext);
    if (!(th instanceof NonExceptionHttpErrorResponse)) {
      log.error("Rest.li error: ", th);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void logResponse(
      final FilterRequestContext requestContext, final FilterResponseContext responseContext) {
    long startTime = (long) requestContext.getFilterScratchpad().get(START_TIME);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    // Build Log Line
    HttpMethod httpMethod = requestContext.getMethodType().getHttpMethod();
    HttpStatus status = responseContext.getResponseData().getResponseEnvelope().getStatus();
    String method = requestContext.getMethod().getName();
    String uri = requestContext.getRequestURI().toString();

    log.info("{} {} - {} - {} - {}ms", httpMethod, uri, method, status.getCode(), duration);
  }
}
