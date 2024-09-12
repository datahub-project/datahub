package com.linkedin.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MANUAL;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;

public class APIThrottle {
  private static final Set<String> AGENT_EXEMPTIONS = Set.of("Browser");
  private static final UserAgentAnalyzer UAA =
      UserAgentAnalyzer.newBuilder()
          .hideMatcherLoadStats()
          .withField(UserAgent.AGENT_CLASS)
          .withCache(1000)
          .build();

  private APIThrottle() {}

  /**
   * This method is expected to be called on sync ingest requests for both timeseries or versioned
   * aspects.
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
  public static void evaluate(
      @Nonnull OperationContext opContext,
      @Nullable Set<ThrottleEvent> throttleEvents,
      boolean isTimeseries) {

    Set<Long> eventMatchMaxWaitMs = eventMatchMaxWaitMs(throttleEvents, isTimeseries);

    if (!eventMatchMaxWaitMs.isEmpty() && !isExempt(opContext.getRequestContext())) {
      throw new APIThrottleException(
          eventMatchMaxWaitMs.stream().max(Comparator.naturalOrder()).orElse(-1L),
          "Throttled due to " + throttleEvents);
    }
  }

  private static boolean isExempt(@Nullable RequestContext requestContext) {
    // Exclude internal calls
    if (requestContext == null
        || requestContext.getUserAgent() == null
        || requestContext.getUserAgent().isEmpty()) {
      return true;
    }

    UserAgent ua = UAA.parse(requestContext.getUserAgent());
    return AGENT_EXEMPTIONS.contains(ua.get(UserAgent.AGENT_CLASS).getValue());
  }

  private static Set<Long> eventMatchMaxWaitMs(
      @Nullable Set<ThrottleEvent> throttleEvents, boolean isTimeseries) {
    if (throttleEvents == null) {
      return Set.of();
    }

    return throttleEvents.stream()
        .map(
            e ->
                e.getActiveThrottleMaxWaitMs(
                    Set.of(MANUAL, isTimeseries ? MCL_TIMESERIES_LAG : MCL_VERSIONED_LAG)))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }
}
