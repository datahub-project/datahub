package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.timeseries.TimeWindow;
import com.linkedin.timeseries.TimeWindowSize;
import java.util.Calendar;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Helpers for the PDL {@link TimeWindow} (a start time plus a calendar length). */
public final class TimeWindowUtils {

  private TimeWindowUtils() {}

  /**
   * Returns the end of the window: its start plus {@code length.multiple} x {@code length.unit}.
   *
   * <p>The end is computed with calendar arithmetic ({@link Calendar#add}) in the JVM default
   * timezone, matching the existing Monitor evaluation window mapping. MONTH, QUARTER and YEAR
   * therefore have their calendar length (for example, one MONTH from the start of January is 31
   * days).
   *
   * @return the end time in epoch milliseconds, or null when the start time, length or unit is
   *     missing, or the unit is unknown
   */
  @Nullable
  public static Long getEndTimeMillis(@Nonnull final TimeWindow window) {
    if (!window.hasStartTimeMillis() || !window.hasLength()) {
      return null;
    }
    final TimeWindowSize length = window.getLength();
    if (!length.hasUnit()) {
      return null;
    }
    final int multiple = length.getMultiple();
    final Calendar endTime = Calendar.getInstance();
    endTime.setTimeInMillis(window.getStartTimeMillis());
    switch (length.getUnit()) {
      case SECOND:
        endTime.add(Calendar.SECOND, multiple);
        break;
      case MINUTE:
        endTime.add(Calendar.MINUTE, multiple);
        break;
      case HOUR:
        endTime.add(Calendar.HOUR, multiple);
        break;
      case DAY:
        endTime.add(Calendar.DAY_OF_YEAR, multiple);
        break;
      case WEEK:
        endTime.add(Calendar.WEEK_OF_YEAR, multiple);
        break;
      case MONTH:
        endTime.add(Calendar.MONTH, multiple);
        break;
      case QUARTER:
        endTime.add(Calendar.MONTH, 3 * multiple);
        break;
      case YEAR:
        endTime.add(Calendar.YEAR, multiple);
        break;
      default:
        // $UNKNOWN: the unit cannot be converted.
        return null;
    }
    return endTime.getTimeInMillis();
  }
}
