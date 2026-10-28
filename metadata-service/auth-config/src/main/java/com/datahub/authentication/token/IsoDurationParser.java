package com.datahub.authentication.token;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * Parses ISO-8601 durations for access-token TTLs. Supports time ({@code PT1H}), days/weeks ({@code
 * P1D}, {@code P2W}), and month/year with fixed approximations matching historical enum semantics
 * ({@code P1M} = 30 days, {@code P1Y} = 365 days).
 */
public final class IsoDurationParser {

  private static final long MS_PER_SECOND = 1000L;
  private static final long MS_PER_MINUTE = 60 * MS_PER_SECOND;
  private static final long MS_PER_HOUR = 60 * MS_PER_MINUTE;
  private static final long MS_PER_DAY = 24 * MS_PER_HOUR;
  private static final long MS_PER_WEEK = 7 * MS_PER_DAY;
  private static final long MS_PER_MONTH = 30 * MS_PER_DAY;
  private static final long MS_PER_YEAR = 365 * MS_PER_DAY;

  // PnYnMnWnDTnHnMnS — years/months/weeks/days then optional time part
  private static final Pattern ISO_DURATION =
      Pattern.compile(
          "^P(?!$)(?:(\\d+)Y)?(?:(\\d+)M)?(?:(\\d+)W)?(?:(\\d+)D)?(?:T(?=\\d)(?:(\\d+)H)?(?:(\\d+)M)?(?:(\\d+(?:\\.\\d+)?)S)?)?$",
          Pattern.CASE_INSENSITIVE);

  private IsoDurationParser() {}

  /**
   * Parse an ISO-8601 duration string to milliseconds.
   *
   * @throws IllegalArgumentException if the value is blank, unparsable, zero, or negative
   */
  public static long parseToMillis(@Nonnull final String isoDuration) {
    if (isoDuration == null || isoDuration.trim().isEmpty()) {
      throw new IllegalArgumentException("Access token duration must not be blank");
    }
    final String normalized = isoDuration.trim().toUpperCase(Locale.ROOT);
    final Matcher matcher = ISO_DURATION.matcher(normalized);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("Invalid ISO-8601 access token duration: %s", isoDuration));
    }

    final long years = parseLongGroup(matcher, 1);
    final long months = parseLongGroup(matcher, 2);
    final long weeks = parseLongGroup(matcher, 3);
    final long days = parseLongGroup(matcher, 4);
    final long hours = parseLongGroup(matcher, 5);
    final long minutes = parseLongGroup(matcher, 6);
    final double seconds = parseDoubleGroup(matcher, 7);

    final long millis =
        years * MS_PER_YEAR
            + months * MS_PER_MONTH
            + weeks * MS_PER_WEEK
            + days * MS_PER_DAY
            + hours * MS_PER_HOUR
            + minutes * MS_PER_MINUTE
            + Math.round(seconds * MS_PER_SECOND);

    if (millis <= 0) {
      throw new IllegalArgumentException(
          String.format("Access token duration must be positive: %s", isoDuration));
    }
    return millis;
  }

  private static long parseLongGroup(final Matcher matcher, final int group) {
    final String value = matcher.group(group);
    return value == null ? 0L : Long.parseLong(value);
  }

  private static double parseDoubleGroup(final Matcher matcher, final int group) {
    final String value = matcher.group(group);
    return value == null ? 0d : Double.parseDouble(value);
  }
}
