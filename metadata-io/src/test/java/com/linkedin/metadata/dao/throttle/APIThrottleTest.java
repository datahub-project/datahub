package com.linkedin.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MANUAL;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class APIThrottleTest {
  private static final ThrottleEvent MANUAL_THROTTLED_EVENT =
      ThrottleEvent.builder().throttled(Map.of(MANUAL, true)).build();
  private static final ThrottleEvent MCL_TIMESERIES_THROTTLED_EVENT =
      ThrottleEvent.builder().throttled(Map.of(MCL_TIMESERIES_LAG, true)).build();
  private static final ThrottleEvent MCL_VERSIONED_THROTTLED_EVENT =
      ThrottleEvent.builder().throttled(Map.of(MCL_VERSIONED_LAG, true)).build();
  private static final ThrottleEvent ALL_MCL_THROTTLED_EVENT =
      ThrottleEvent.builder()
          .throttled(Map.of(MCL_TIMESERIES_LAG, true, MCL_VERSIONED_LAG, true))
          .build();
  private static final ThrottleEvent ALL_THROTTLED_EVENT =
      ThrottleEvent.builder()
          .throttled(Map.of(MANUAL, true, MCL_TIMESERIES_LAG, true, MCL_VERSIONED_LAG, true))
          .build();
  public static final Set<ThrottleEvent> ALL_EVENTS =
      Set.of(
          MANUAL_THROTTLED_EVENT,
          MCL_TIMESERIES_THROTTLED_EVENT,
          MCL_VERSIONED_THROTTLED_EVENT,
          ALL_MCL_THROTTLED_EVENT,
          ALL_THROTTLED_EVENT);

  private OperationContext opContext;
  private RequestContext mockRequestContext;

  @BeforeMethod
  public void init() {
    mockRequestContext = mock(RequestContext.class);
    opContext = TestOperationContexts.userContextNoSearchAuthorization(mockRequestContext);
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
      try {
        APIThrottle.evaluate(opContext, Set.of(event), false);
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected! " + event);
      }
      try {
        APIThrottle.evaluate(opContext, Set.of(event), true);
      } catch (Exception ex) {
        Assert.fail("Exception was thrown and NOT expected! " + event);
      }

      // Browser tests
      for (String ua : exemptions) {
        try {
          when(mockRequestContext.getUserAgent()).thenReturn(ua);
          APIThrottle.evaluate(opContext, Set.of(event), true);
        } catch (Exception ex) {
          Assert.fail("Exception was thrown and NOT expected! " + event);
        }
        try {
          when(mockRequestContext.getUserAgent()).thenReturn(ua);
          APIThrottle.evaluate(opContext, Set.of(event), false);
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
            APIThrottle.evaluate(opContext, Set.of(event), true);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
        if (!event.getActiveThrottles().contains(MCL_TIMESERIES_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            APIThrottle.evaluate(opContext, Set.of(event), true);
          } catch (Exception ex) {
            Assert.fail(String.format("Exception was thrown and NOT expected! %s %s", ua, event));
          }
        }

        // versioned lag present
        if (event.getActiveThrottles().contains(MCL_VERSIONED_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            APIThrottle.evaluate(opContext, Set.of(event), false);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
        if (!event.getActiveThrottles().contains(MCL_VERSIONED_LAG)
            && !event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            APIThrottle.evaluate(opContext, Set.of(event), false);
          } catch (Exception ex) {
            Assert.fail(String.format("Exception was thrown and NOT expected! %s %s", ua, event));
          }
        }

        // manual throttle active
        if (event.getActiveThrottles().contains(MANUAL)) {
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            APIThrottle.evaluate(opContext, Set.of(event), true);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
          try {
            when(mockRequestContext.getUserAgent()).thenReturn(ua);
            APIThrottle.evaluate(opContext, Set.of(event), false);
            Assert.fail(String.format("Exception WAS expected! %s %s", ua, event));
          } catch (Exception ignored) {
          }
        }
      }
    }
  }
}
