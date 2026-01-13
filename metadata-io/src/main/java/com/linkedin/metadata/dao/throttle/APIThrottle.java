package com.linkedin.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.*;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class APIThrottle {
  private static final Set<String> AGENT_EXEMPTIONS = Set.of("Browser");

  private APIThrottle() {}

  /**
   * This method is expected to be called on sync ingest requests for both timeseries or versioned
   * aspects. The primary goal of this throttling is to prevent MCL lag from growing too large.
   *
   * <p>1. Async requests are never expected to be throttled here. 2. UI requests are not expected
   * to be throttled, so we'll try to detect browser vs non-browser activity. 3. Throttling
   * exceptions are expected to be caught by the API implementation and converted to a 429 http
   * status code
   *
   * @param opContext the operation context
   * @param throttleEvents the throttle state
   * @param isTimeseries whether the operation is for timeseries or not (throttled separately)
   */
  public static void evaluateSync(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<ThrottleEvent> throttleEvents,
      boolean isTimeseries) {
    Set<ThrottleType> filterTypes =
        Set.of(MANUAL, isTimeseries ? MCL_TIMESERIES_LAG : MCL_VERSIONED_LAG);
    evaluate(opContext, throttleEvents, filterTypes);
  }

  /**
   * This method is expected to be called on all ingestion requests, for the purpose of limiting
   * load on elasticsearch and kafka. This was developed for free trial clusters, where customers
   * are not paying for dedicated resources.
   *
   * <p>We still exempt internal calls and browser calls from this throttling, to avoid impacting
   * the user experience.
   */
  public static void evaluateProposal(
      @Nonnull OperationContext opContext, @Nonnull Collection<ThrottleEvent> throttleEvents) {
    Set<ThrottleType> filterTypes = Set.of(RATE_LIMIT);
    evaluate(opContext, throttleEvents, filterTypes);
  }

  private static boolean isExempt(@Nullable RequestContext requestContext) {
    // Exclude internal calls
    if (requestContext == null
        || requestContext.getAgentClass() == null
        || requestContext.getUserAgent().isEmpty()) {
      return true;
    }
    return AGENT_EXEMPTIONS.contains(requestContext.getAgentClass());
  }

  private static void evaluate(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<ThrottleEvent> throttleEvents,
      @Nonnull Set<ThrottleType> throttleTypes) {
    if (isExempt(opContext.getRequestContext())) {
      return;
    }

    Set<Long> eventMatchMaxWaitMs = eventMatchMaxWaitMs(throttleEvents, throttleTypes);
    if (!eventMatchMaxWaitMs.isEmpty()) {
      long retryAfter = eventMatchMaxWaitMs.stream().max(Comparator.naturalOrder()).orElse(-1L);
      log.debug(
          "Throttling API request with wait {} checking throttle types {}",
          retryAfter,
          throttleTypes);
      throw new APIThrottleException(retryAfter, "Throttled due to " + throttleEvents);
    }
  }

  private static Set<Long> eventMatchMaxWaitMs(
      @Nonnull Collection<ThrottleEvent> throttleEvents, Set<ThrottleType> throttleTypes) {
    return throttleEvents.stream()
        .map(e -> e.getActiveThrottleMaxWaitMs(throttleTypes))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }
}
