package com.linkedin.datahub.graphql.utils;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import com.linkedin.datahub.graphql.resolvers.auth.AccessTokenUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.testng.annotations.Test;

public class AccessTokenUtilTest {

  private void assertDurationMapping(AccessTokenDuration duration, long expectedMs) {
    Optional<Long> result = AccessTokenUtil.mapDurationToMs(duration);
    assertTrue(result.isPresent());
    assertEquals(expectedMs, result.get().longValue());
  }

  @Test
  public void testMapDurationToMs() {
    assertDurationMapping(
        AccessTokenDuration.ONE_HOUR, Duration.of(1, ChronoUnit.HOURS).toMillis());
    assertDurationMapping(AccessTokenDuration.ONE_DAY, Duration.of(1, ChronoUnit.DAYS).toMillis());
    assertDurationMapping(AccessTokenDuration.ONE_WEEK, Duration.of(7, ChronoUnit.DAYS).toMillis());
    assertDurationMapping(
        AccessTokenDuration.ONE_MONTH, Duration.of(30, ChronoUnit.DAYS).toMillis());
    assertDurationMapping(
        AccessTokenDuration.THREE_MONTHS, Duration.of(90, ChronoUnit.DAYS).toMillis());
    assertDurationMapping(
        AccessTokenDuration.SIX_MONTHS, Duration.of(180, ChronoUnit.DAYS).toMillis());
    assertDurationMapping(
        AccessTokenDuration.ONE_YEAR, Duration.of(365, ChronoUnit.DAYS).toMillis());

    // Verify specific millisecond values
    assertDurationMapping(AccessTokenDuration.ONE_HOUR, 3600000L);
    assertDurationMapping(AccessTokenDuration.ONE_DAY, 86400000L);
    assertDurationMapping(AccessTokenDuration.ONE_WEEK, 604800000L);
    assertDurationMapping(AccessTokenDuration.ONE_MONTH, 2592000000L);
    assertDurationMapping(AccessTokenDuration.THREE_MONTHS, 7776000000L);
    assertDurationMapping(AccessTokenDuration.SIX_MONTHS, 15552000000L);
    assertDurationMapping(AccessTokenDuration.ONE_YEAR, 31536000000L);
  }

  @Test
  public void testMapDurationToMs_NoExpiry() {
    Optional<Long> noExpiryResult = AccessTokenUtil.mapDurationToMs(AccessTokenDuration.NO_EXPIRY);
    assertFalse(noExpiryResult.isPresent());
  }
}
