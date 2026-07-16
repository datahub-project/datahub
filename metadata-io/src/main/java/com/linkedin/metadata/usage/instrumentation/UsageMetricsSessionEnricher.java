package com.linkedin.metadata.usage.instrumentation;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.instrumentation.SessionContextEnricher;
import io.datahubproject.metadata.exception.OperationContextException;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UsageMetricsSessionEnricher implements SessionContextEnricher {

  private final UsageAggregationStore usageStore;
  private final boolean enabled;

  /**
   * Sessions that already received positive {@code output_bytes} via {@link
   * #recordResponseWithBytes} on an async worker thread. {@link #completeResponse} skips
   * re-counting on the servlet thread.
   */
  private final Set<OperationContext> outOfBandOutputBytesRecorded = ConcurrentHashMap.newKeySet();

  @Override
  public void enrichBeforeBuild(
      @Nonnull RequestContext.RequestContextBuilder requestBuilder,
      @Nonnull Authentication sessionAuthentication) {
    if (!enabled) {
      return;
    }

    String usageOperation = requestBuilder.peekUsageOperation();
    if (usageOperation == null || usageOperation.isEmpty()) {
      return;
    }

    if (requestBuilder.peekRequestAPI() == null) {
      return;
    }

    GraphQLOperationKind graphqlKind = requestBuilder.peekGraphqlOperationKind();

    String actorUrn =
        requestBuilder.peekActorUrn() != null
            ? requestBuilder.peekActorUrn()
            : sessionAuthentication.getActor().toUrnStr();
    String usageIdentity =
        sessionAuthentication.getActor() != null
            ? sessionAuthentication.getActor().toUrnStr()
            : actorUrn;
    AuthChannel authChannel = UsageAuthChannelResolver.resolve(sessionAuthentication, actorUrn);

    Long inputBytes = requestBuilder.peekInputBytes();
    boolean requestMaterialized = inputBytes != null;

    requestBuilder
        .usageIdentity(usageIdentity)
        .authChannel(authChannel)
        .inputBytes(inputBytes)
        .requestBodyMaterialized(requestMaterialized)
        .graphqlOperationKind(graphqlKind);
  }

  @Override
  public void onSessionReady(@Nonnull OperationContext sessionContext) {
    if (!enabled) {
      return;
    }
    if (sessionContext.getRequestContext().getUsageOperation() == null
        || sessionContext.getRequestContext().getUsageOperation().isEmpty()) {
      return;
    }
    if (sessionContext.getRequestContext().getUsageIdentity() == null
        || sessionContext.getRequestContext().getUsageIdentity().isEmpty()) {
      return;
    }
    try {
      if (usageStore.recordRequest(sessionContext)) {
        UsageRequestState.current().activeSessions().push(sessionContext);
      }
    } catch (RuntimeException e) {
      log.warn("Failed to record usage request metrics", e);
    }
  }

  /**
   * Completes response recording for all sessions opened on this thread during the request.
   *
   * <p>Nested {@link OperationContext#asSession} calls push onto a per-thread stack; the innermost
   * session receives {@code outputBytes}, outer sessions receive {@code null} so response bytes are
   * not double-counted.
   */
  public void completeResponse(@Nullable Long outputBytes) {
    if (!enabled) {
      return;
    }
    UsageRequestState state = UsageRequestState.currentOrNull();
    if (state == null) {
      return;
    }
    try {
      Deque<OperationContext> sessions = state.activeSessions();
      if (sessions.isEmpty()) {
        return;
      }
      boolean assignOutputBytes = true;
      while (!sessions.isEmpty()) {
        OperationContext sessionContext = sessions.pop();
        boolean alreadyRecordedOutOfBand = outOfBandOutputBytesRecorded.remove(sessionContext);
        Long bytesForSession = assignOutputBytes ? outputBytes : null;
        if (assignOutputBytes
            && bytesForSession != null
            && bytesForSession > 0
            && alreadyRecordedOutOfBand) {
          bytesForSession = null;
        }
        try {
          usageStore.recordResponse(sessionContext, bytesForSession);
        } catch (RuntimeException e) {
          log.warn("Failed to record usage response metrics", e);
        }
        assignOutputBytes = false;
      }
    } finally {
      UsageRequestState.clear();
    }
  }

  public void recordResponse(@Nonnull OperationContext sessionContext) {
    recordResponseWithBytes(sessionContext, null);
  }

  public void recordResponseWithBytes(
      @Nonnull OperationContext sessionContext, @Nullable Long outputBytes) {
    if (!enabled) {
      return;
    }
    try {
      usageStore.recordResponse(sessionContext, outputBytes);
    } catch (RuntimeException e) {
      log.warn("Failed to record usage response metrics", e);
    }
    if (outputBytes != null && outputBytes > 0) {
      outOfBandOutputBytesRecorded.add(sessionContext);
    }
  }

  /**
   * Records usage for servlet paths without {@link OperationContext#asSession}. Uses the system
   * context's {@code Authorizer.SYSTEM} instead of running the full authorizer chain.
   */
  public void recordTaggedServletRequest(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull RequestContext.RequestContextBuilder requestBuilder,
      @Nonnull Authentication sessionAuthentication) {
    if (!enabled) {
      return;
    }
    enrichBeforeBuild(requestBuilder, sessionAuthentication);
    RequestContext.RequestContextBuilder sessionRequestBuilder =
        requestBuilder.metricUtils(systemOperationContext.getMetricUtils().orElse(null));
    RequestContext builtRequestContext = sessionRequestBuilder.build();
    try {
      OperationContext sessionContext =
          systemOperationContext.toBuilder()
              .requestContext(builtRequestContext)
              .build(sessionAuthentication, true);
      onSessionReady(sessionContext);
    } catch (OperationContextException e) {
      throw new RuntimeException(e);
    }
  }
}
