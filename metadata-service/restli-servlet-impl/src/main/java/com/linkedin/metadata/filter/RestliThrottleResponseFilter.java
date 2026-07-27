package com.linkedin.metadata.filter;

import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.dao.throttle.DatabaseTransactionConflictRestLiServiceException;
import com.linkedin.metadata.dao.throttle.ThrottledRestLiServiceException;
import com.linkedin.metadata.throttle.ThrottleResponseHeaderWriter;
import com.linkedin.restli.server.filter.Filter;
import com.linkedin.restli.server.filter.FilterRequestContext;
import com.linkedin.restli.server.filter.FilterResponseContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/** Adds standard throttle and transaction-conflict response headers on Rest.li errors. */
public class RestliThrottleResponseFilter implements Filter {

  @Override
  public CompletableFuture<Void> onError(
      Throwable th,
      final FilterRequestContext requestContext,
      final FilterResponseContext responseContext) {
    APIThrottleException throttleException = extractThrottleException(th);
    if (throttleException != null) {
      Map<String, String> headers = responseContext.getResponseData().getHeaders();
      ThrottleResponseHeaderWriter.applyDenial(
          headers,
          throttleException.getRuleId(),
          throttleException.getMechanismType(),
          throttleException.getSource(),
          throttleException.getDurationMs());
      return CompletableFuture.completedFuture(null);
    }

    DatabaseTransactionConflictRestLiServiceException conflictException =
        extractDatabaseTransactionConflict(th);
    if (conflictException != null) {
      responseContext.getResponseData().getHeaders().putAll(conflictException.getResponseHeaders());
    }
    return CompletableFuture.completedFuture(null);
  }

  @Nullable
  private static DatabaseTransactionConflictRestLiServiceException
      extractDatabaseTransactionConflict(Throwable th) {
    if (th instanceof DatabaseTransactionConflictRestLiServiceException conflict) {
      return conflict;
    }
    if (th.getCause() instanceof DatabaseTransactionConflictRestLiServiceException conflict) {
      return conflict;
    }
    return null;
  }

  @Nullable
  private static APIThrottleException extractThrottleException(Throwable th) {
    if (th instanceof ThrottledRestLiServiceException throttledRestLiServiceException) {
      return throttledRestLiServiceException.getThrottleException();
    }
    if (th instanceof APIThrottleException apiThrottleException) {
      return apiThrottleException;
    }
    if (th.getCause() instanceof APIThrottleException apiThrottleException) {
      return apiThrottleException;
    }
    return null;
  }
}
