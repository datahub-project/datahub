package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;


public class AccessTokenUtil {

  /**
   * @return Get the millis of LocalDateTime
   */
  public static long getMillis(LocalDateTime localDateTime){
    long millis = localDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant().toEpochMilli();
    return millis;
  }

  /**
   * Convert an {@link AccessTokenDuration} into its milliseconds equivalent.
   */                                   
  public static Optional<Long> mapDurationToMs(final AccessTokenDuration duration) {
    switch (duration) {
      case ONE_HOUR:
        return Optional.of(getMillis(LocalDateTime.now().plusHours(1)));
      case ONE_DAY:
        return Optional.of(getMillis(LocalDateTime.now().plusDays(1)));
      case ONE_MONTH:
        return Optional.of(getMillis(LocalDateTime.now().plusMonths(1)));
      case THREE_MONTHS:
        return Optional.of(getMillis(LocalDateTime.now().plusMonths(3)));
      case SIX_MONTHS:
        return Optional.of(getMillis(LocalDateTime.now().plusMonths(6)));
      case ONE_YEAR:
        return Optional.of(getMillis(LocalDateTime.now().plusYears(1)));
      case NO_EXPIRY:
        return Optional.empty();
      default:
        throw new RuntimeException(String.format("Unrecognized access token duration %s provided", duration));
    }
  }

  private AccessTokenUtil() { }
}
