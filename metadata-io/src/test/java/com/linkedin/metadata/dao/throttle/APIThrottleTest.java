package com.linkedin.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MANUAL;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.RATE_LIMIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nl.basjes.parse.useragent.UserAgent;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class APIThrottleTest {
  private static final ThrottleEvent MANUAL_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(Map.of(MANUAL, true));
  private static final ThrottleEvent MCL_TIMESERIES_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(Map.of(MCL_TIMESERIES_LAG, true));
  private static final ThrottleEvent MCL_VERSIONED_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(Map.of(MCL_VERSIONED_LAG, true));
  private static final ThrottleEvent ALL_MCL_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(Map.of(MCL_TIMESERIES_LAG, true, MCL_VERSIONED_LAG, true));
  private static final ThrottleEvent ALL_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(
          Map.of(MANUAL, true, MCL_TIMESERIES_LAG, true, MCL_VERSIONED_LAG, true));
  private static final ThrottleEvent RATE_LIMIT_THROTTLED_EVENT =
      ThrottleEvent.booleanThrottle(Map.of(RATE_LIMIT, true));
  public static final Set<ThrottleEvent> ALL_EVENTS =
      Set.of(
          MANUAL_THROTTLED_EVENT,
          MCL_TIMESERIES_THROTTLED_EVENT,
          MCL_VERSIONED_THROTTLED_EVENT,
          ALL_MCL_THROTTLED_EVENT,
          ALL_THROTTLED_EVENT,
          RATE_LIMIT_THROTTLED_EVENT);

  private OperationContext opContext;
  private RequestContext mockRequestContext;

  @BeforeMethod
  public void init() {
    mockRequestContext = mock(RequestContext.class);
    RequestContext.RequestContextBuilder builder = mock(RequestContext.RequestContextBuilder.class);
    when(builder.metricUtils(any())).thenReturn(builder);
    when(builder.actorUrn(any())).thenReturn(builder);
    when(builder.sourceIP(any())).thenReturn(builder);
    when(builder.requestAPI(any())).thenReturn(builder);
    when(builder.requestID(any())).thenReturn(builder);
    when(builder.userAgent(any())).thenReturn(builder);
    when(builder.build()).thenReturn(mockRequestContext);
    opContext = TestOperationContexts.userContextNoSearchAuthorization(builder);
  }

  @Test
  public void testExemptions() {
    List<String> exemptions =
        List.of(
            "",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15");

    for (ThrottleEvent event : ALL_EVENTS) {
      when(mockRequestContext.getUserAgent()).thenReturn(null);
      when(mockRequestContext.getAgentClass()).thenReturn(null);
      try {
        APIThrottle.evaluateSync(opContext, Set.of(event), false);
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected! " + event);
      }
      try {
        APIThrottle.evaluateSync(opContext, Set.of(event), true);
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected! " + event);
      }

      // Browser tests
      for (String ua : exemptions) {
        try {
          when(mockRequestContext.getUserAgent()).thenReturn(ua);
          when(mockRequestContext.getAgentClass())
              .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
          APIThrottle.evaluateSync(opContext, Set.of(event), true);
        } catch (Exception ex) {
          Assert.fail("Exception was thrown and NOT expected! " + event);
        }
        try {
          when(mockRequestContext.getUserAgent()).thenReturn(ua);
          when(mockRequestContext.getAgentClass())
              .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
          APIThrottle.evaluateSync(opContext, Set.of(event), false);
        } catch (Exception ex) {
          Assert.fail("Exception was thrown and NOT expected! " + event);
        }
      }
    }
  }

  @Test
  public void testThrottleException() {
    List<String> applicable =
        List.of(
            "python-requests/2.28.2",
            "Apache-HttpClient/4.5.5 (Java/1.8.0_162)",
            "okhttp/4.9.3.7",
            "Go-http-client/1.1");

    for (ThrottleEvent event : ALL_EVENTS) {
      for (String ua : applicable) {
        // timeseries lag present
        if (event.getActiveThrottles().contains(MCL_TIMESERIES_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), true);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
        if (!event.getActiveThrottles().contains(MCL_TIMESERIES_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), true);
          } catch (Exception ex) {
            Assert.fail(String.format("Exception was thrown and NOT expected! %s %s", ua, event));
          }
        }

        // versioned lag present
        if (event.getActiveThrottles().contains(MCL_VERSIONED_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), false);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
        if (!event.getActiveThrottles().contains(MCL_VERSIONED_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), false);
          } catch (Exception ex) {
            Assert.fail(String.format("Exception was thrown and NOT expected! %s %s", ua, event));
          }
        }

        // manual throttle active
        if (event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), true);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            when(mockRequestContext.getAgentClass())
                .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
            APIThrottle.evaluateSync(opContext, Set.of(event), false);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
      }
    }
  }

  @Test
  public void testEvaluateProposalExemptions() {
    List<String> exemptions =
        List.of(
            "",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15");

    // Create a throttle event with positive wait time for testing
    ThrottleEvent rateLimitActiveEvent = ThrottleEvent.throttle(Map.of(RATE_LIMIT, 1000L));

    // Test null/empty user agent exemption
    when(mockRequestContext.getUserAgent()).thenReturn(null);
    when(mockRequestContext.getAgentClass()).thenReturn(null);
    try {
      APIThrottle.evaluateProposal(opContext, Set.of(rateLimitActiveEvent));
    } catch (Exception ex) {
      Assert.fail("Exception was thrown and NOT expected for null user agent!");
    }

    // Test browser exemptions
    for (String ua : exemptions) {
      try {
        when(mockRequestContext.getUserAgent()).thenReturn(ua);
        when(mockRequestContext.getAgentClass())
            .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
        APIThrottle.evaluateProposal(opContext, Set.of(rateLimitActiveEvent));
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected for browser user agent: " + ua);
      }
    }

    // Test that non-RATE_LIMIT events don't trigger throttling
    ThrottleEvent timeseriesThrottleEvent =
        ThrottleEvent.throttle(Map.of(MCL_TIMESERIES_LAG, 1000L));
    ThrottleEvent versionedThrottleEvent = ThrottleEvent.throttle(Map.of(MCL_VERSIONED_LAG, 1000L));
    ThrottleEvent manualThrottleEvent = ThrottleEvent.throttle(Map.of(MANUAL, 1000L));

    for (ThrottleEvent event :
        Set.of(timeseriesThrottleEvent, versionedThrottleEvent, manualThrottleEvent)) {
      try {
        when(mockRequestContext.getUserAgent()).thenReturn("python-requests/2.28.2");
        when(mockRequestContext.getAgentClass())
            .thenReturn(
                RequestContext.UAA
                    .parse("python-requests/2.28.2")
                    .get(UserAgent.AGENT_CLASS)
                    .getValue());
        APIThrottle.evaluateProposal(opContext, Set.of(event));
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected for non-RATE_LIMIT event: " + event);
      }
    }
  }

  @Test
  public void testEvaluateProposalThrottleException() {
    List<String> applicable =
        List.of(
            "python-requests/2.28.2",
            "Apache-HttpClient/4.5.5 (Java/1.8.0_162)",
            "okhttp/4.9.3.7",
            "Go-http-client/1.1");

    // Create a throttle event with a positive wait time (not booleanThrottle)
    ThrottleEvent rateLimitActiveEvent = ThrottleEvent.throttle(Map.of(RATE_LIMIT, 1000L));

    for (String ua : applicable) {
      // Should throw when RATE_LIMIT is active
      try {
        when(mockRequestContext.getUserAgent()).thenReturn(ua);
        when(mockRequestContext.getAgentClass())
            .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
        APIThrottle.evaluateProposal(opContext, Set.of(rateLimitActiveEvent));
        Assert.fail(String.format("Exception WAS expected for RATE_LIMIT throttle! %s", ua));
      } catch (APIThrottleException ignored) {
        // Expected
      }
    }

    // Should not throw when RATE_LIMIT is not active
    for (String ua : applicable) {
      ThrottleEvent nonRateLimitEvent = ThrottleEvent.throttle(Map.of(MCL_TIMESERIES_LAG, 1000L));
      try {
        when(mockRequestContext.getUserAgent()).thenReturn(ua);
        when(mockRequestContext.getAgentClass())
            .thenReturn(RequestContext.UAA.parse(ua).get(UserAgent.AGENT_CLASS).getValue());
        APIThrottle.evaluateProposal(opContext, Set.of(nonRateLimitEvent));
      } catch (Exception ex) {
        Assert.fail(
            String.format(
                "Exception was thrown and NOT expected for non-RATE_LIMIT event! %s", ua));
      }
    }
  }
}
