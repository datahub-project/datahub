package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class AccessTokenUtil {

  /** Convert an {@link AccessTokenDuration} into its milliseconds equivalent. */
  public static Optional<Long> mapDurationToMs(final AccessTokenDuration duration) {
    switch (duration) {
      case ONE_HOUR:
        return Optional.of(Duration.of(1, ChronoUnit.HOURS).toMillis());
      case ONE_DAY:
        return Optional.of(Duration.of(1, ChronoUnit.DAYS).toMillis());
      case ONE_MONTH:
        return Optional.of(Duration.of(30, ChronoUnit.DAYS).toMillis());
      case THREE_MONTHS:
        return Optional.of(Duration.of(90, ChronoUnit.DAYS).toMillis());
      case SIX_MONTHS:
        return Optional.of(Duration.of(180, ChronoUnit.DAYS).toMillis());
      case ONE_YEAR:
        return Optional.of(Duration.of(365, ChronoUnit.DAYS).toMillis());
      case NO_EXPIRY:
        return Optional.empty();
      default:
        throw new RuntimeException(
            String.format("Unrecognized access token duration %s provided", duration));
    }
  }

  private AccessTokenUtil() {}
}
