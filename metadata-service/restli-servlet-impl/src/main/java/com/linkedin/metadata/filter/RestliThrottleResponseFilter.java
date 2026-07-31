package com.linkedin.metadata.filter;

import com.linkedin.metadata.dao.throttle.DatabaseTransactionConflictRestLiServiceException;
import com.linkedin.restli.server.filter.Filter;
import com.linkedin.restli.server.filter.FilterRequestContext;
import com.linkedin.restli.server.filter.FilterResponseContext;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * Adds transaction-conflict response headers (e.g. {@code Retry-After}) on Rest.li errors. On
 * master this filter also writes throttle (429) debug headers; that throttle header machinery does
 * not exist on this branch, so only the transaction-conflict handling is backported.
 */
public class RestliThrottleResponseFilter implements Filter {

  @Override
  public CompletableFuture<Void> onError(
      Throwable th,
      final FilterRequestContext requestContext,
      final FilterResponseContext responseContext) {
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
    Throwable cur = th;
    while (cur != null) {
      if (cur instanceof DatabaseTransactionConflictRestLiServiceException conflict) {
        return conflict;
      }
      Throwable cause = cur.getCause();
      if (cause == null || cause == cur) {
        break;
      }
      cur = cause;
    }
    return null;
  }
}
