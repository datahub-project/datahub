package com.linkedin.metadata.analytics.postgres;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnull;

public final class PostgresAnalyticsUtc {
  private PostgresAnalyticsUtc() {}

  @Nonnull
  public static Instant truncateToUtcHour(@Nonnull Instant instant) {
    return instant.truncatedTo(ChronoUnit.HOURS);
  }

  @Nonnull
  public static Instant truncateToUtcDay(@Nonnull Instant instant) {
    LocalDate d = instant.atZone(ZoneOffset.UTC).toLocalDate();
    return d.atStartOfDay(ZoneOffset.UTC).toInstant();
  }

  @Nonnull
  public static Instant truncateToUtcMonth(@Nonnull Instant instant) {
    YearMonth ym = YearMonth.from(instant.atZone(ZoneOffset.UTC));
    return ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
  }

  @Nonnull
  public static Instant hourEndExclusive(@Nonnull Instant hourStart) {
    return hourStart.plus(1, ChronoUnit.HOURS);
  }

  /** True when now >= hour_end + inputLag. */
  public static boolean isHourSealable(
      @Nonnull Instant hourStart, @Nonnull Instant now, int inputLagSeconds) {
    Instant sealAt = hourEndExclusive(hourStart).plusSeconds(Math.max(0, inputLagSeconds));
    return !now.isBefore(sealAt);
  }

  @Nonnull
  public static String partitionKeyHour(@Nonnull Instant hourStart) {
    ZonedDateTime z = hourStart.atZone(ZoneOffset.UTC);
    return String.format(
        "%04d-%02d-%02dT%02d", z.getYear(), z.getMonthValue(), z.getDayOfMonth(), z.getHour());
  }

  @Nonnull
  public static String partitionKeyDay(@Nonnull Instant dayStart) {
    LocalDate d = dayStart.atZone(ZoneOffset.UTC).toLocalDate();
    return d.toString();
  }

  @Nonnull
  public static String partitionKeyMonth(@Nonnull Instant monthStart) {
    YearMonth ym = YearMonth.from(monthStart.atZone(ZoneOffset.UTC));
    return ym.toString();
  }
}
