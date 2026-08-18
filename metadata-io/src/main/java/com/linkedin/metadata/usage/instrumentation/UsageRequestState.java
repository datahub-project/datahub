package com.linkedin.metadata.usage.instrumentation;

import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider.CorpUserFlags;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Per-servlet-request ephemeral state for usage aggregation: active session stack, corp-user flag
 * memoization, and actor-class resolution cache. Cleared when {@link UsageMetricsSessionEnricher}
 * completes response recording for the request.
 */
public final class UsageRequestState {

  private static final ThreadLocal<UsageRequestState> CURRENT = new ThreadLocal<>();

  private final Deque<OperationContext> activeSessions = new ArrayDeque<>();
  private final Map<String, CorpUserFlags> corpUserFlags = new HashMap<>();
  private final Map<Object, UsageActorClass> actorClassCache = new HashMap<>();

  private UsageRequestState() {}

  /** Returns the current request state, creating it if absent. */
  public static UsageRequestState current() {
    UsageRequestState state = CURRENT.get();
    if (state == null) {
      state = new UsageRequestState();
      CURRENT.set(state);
    }
    return state;
  }

  @Nullable
  public static UsageRequestState currentOrNull() {
    return CURRENT.get();
  }

  /** Clears all per-request usage state for the current thread. */
  public static void clear() {
    CURRENT.remove();
  }

  public Deque<OperationContext> activeSessions() {
    return activeSessions;
  }

  public Map<String, CorpUserFlags> corpUserFlags() {
    return corpUserFlags;
  }

  public Map<Object, UsageActorClass> actorClassCache() {
    return actorClassCache;
  }
}
