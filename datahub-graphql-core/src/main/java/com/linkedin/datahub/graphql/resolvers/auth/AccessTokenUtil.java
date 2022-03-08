package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import java.time.Duration;
import java.time.temporal.ChronoUnit;


public class AccessTokenUtil {

  /**
   * Convert an {@link AccessTokenDuration} into its milliseconds equivalent.
   */
  public static long mapDurationToMs(final AccessTokenDuration duration) {
    switch (duration) {
      case ONE_HOUR:
        return Duration.of(1, ChronoUnit.HOURS).toMillis();
      case ONE_DAY:
        return Duration.of(1, ChronoUnit.DAYS).toMillis();
      case ONE_MONTH:
        return Duration.of(30, ChronoUnit.DAYS).toMillis();
      case THREE_MONTHS:
        return Duration.of(90, ChronoUnit.DAYS).toMillis();
      case SIX_MONTHS:
        return Duration.of(180, ChronoUnit.DAYS).toMillis();
      case ONE_YEAR:
        return Duration.of(365, ChronoUnit.DAYS).toMillis();
      default:
        throw new RuntimeException(String.format("Unrecognized access token duration %s provided", duration));
    }
  }

  private AccessTokenUtil() { }
}
