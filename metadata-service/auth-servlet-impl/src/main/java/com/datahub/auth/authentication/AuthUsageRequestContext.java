package com.datahub.auth.authentication;

import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;

import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.http.HttpHeaders;

/** Builds usage-instrumented {@link RequestContext} for auth endpoints. */
final class AuthUsageRequestContext {

  private AuthUsageRequestContext() {}

  @Nonnull
  static RequestContext.RequestContextBuilder openapiUsage(
      @Nonnull String actorUrn,
      @Nullable HttpHeaders headers,
      @Nonnull String requestId,
      @Nonnull UsageOperation usageOperation) {
    if (headers == null) {
      return openapiUsage(actorUrn, null, null, requestId, usageOperation);
    }
    List<String> forwarded = headers.getOrEmpty(X_FORWARDED_FOR);
    List<String> userAgents = headers.getOrEmpty(HttpHeaders.USER_AGENT);
    return openapiUsage(
        actorUrn,
        forwarded.isEmpty() ? null : forwarded.get(0),
        userAgents.isEmpty() ? null : userAgents.get(0),
        requestId,
        usageOperation);
  }

  @Nonnull
  static RequestContext.RequestContextBuilder openapiUsage(
      @Nonnull String actorUrn,
      @Nullable String xForwardedFor,
      @Nullable String userAgent,
      @Nonnull String requestId,
      @Nonnull UsageOperation usageOperation) {
    return RequestContext.builder()
        .actorUrn(actorUrn)
        .sourceIP("")
        .userAgent("")
        .requestAPI(RequestContext.RequestAPI.OPENAPI)
        .requestID(requestId)
        .withUsageOperation(usageOperation)
        .withClientHeaders(xForwardedFor, userAgent);
  }
}
