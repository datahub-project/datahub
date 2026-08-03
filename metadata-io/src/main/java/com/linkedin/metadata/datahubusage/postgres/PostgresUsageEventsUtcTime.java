package com.linkedin.metadata.datahubusage.postgres;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** UTC date boundaries for partitioned usage events. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PostgresUsageEventsUtcTime {

  public static long startOfUtcMonthMillis(YearMonth ym) {
    return ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  /** First instant not included in {@code ym} (exclusive upper bound for PARTITION). */
  public static long endExclusiveUtcMonthMillis(YearMonth ym) {
    return ym.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  public static YearMonth yearMonthFromUtcMillis(long utcMillis) {
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneOffset.UTC);
    return YearMonth.of(ldt.getYear(), ldt.getMonth());
  }

  /**
   * Earliest UTC month boundary we keep given a retention expressed in calendar months from now.
   */
  public static long minimumRetainedUtcMonthStartMillisUtcNow(int retentionMonths) {
    YearMonth ym = YearMonth.now(ZoneOffset.UTC).minusMonths(retentionMonths);
    return startOfUtcMonthMillis(ym);
  }
}
