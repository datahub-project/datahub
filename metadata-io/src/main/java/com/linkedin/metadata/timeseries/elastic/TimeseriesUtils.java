package com.linkedin.metadata.timeseries.elastic;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.usage.UsageTimeRange;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TimeseriesUtils {

  public static CalendarInterval windowToInterval(@Nonnull WindowDuration duration) {
    switch (duration) {
      case HOUR:
        return CalendarInterval.HOUR;
      case DAY:
        return CalendarInterval.DAY;
      case WEEK:
        return CalendarInterval.WEEK;
      case MONTH:
        return CalendarInterval.MONTH;
      case YEAR:
        return CalendarInterval.YEAR;
      default:
        throw new IllegalArgumentException("Unsupported duration value" + duration);
    }
  }

  @Nonnull
  public static Long convertRangeToStartTime(
      @Nonnull UsageTimeRange range, long currentEpochMillis) {
    // TRICKY: since start_time must be before the bucket's start, we actually
    // need to subtract extra from the current time to ensure that we get precisely
    // what we're looking for. Note that start_time and end_time are both inclusive,
    // so we must also do an off-by-one adjustment.
    final long oneHourMillis = 60 * 60 * 1000;
    final long oneDayMillis = 24 * oneHourMillis;

    if (range == UsageTimeRange.HOUR) {
      return currentEpochMillis - (2 * oneHourMillis + 1);
    } else if (range == UsageTimeRange.DAY) {
      return currentEpochMillis - (2 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.WEEK) {
      return currentEpochMillis - (8 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.MONTH) {
      // Assuming month is last 30 days.
      return currentEpochMillis - (31 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.QUARTER) {
      // Assuming a quarter is 91 days.
      return currentEpochMillis - (92 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.HALF_YEAR) {
      // Assuming half a year is 182 days.
      return currentEpochMillis - (183 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.YEAR) {
      return currentEpochMillis - (366 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.ALL) {
      return 0L;
    } else {
      throw new IllegalArgumentException("invalid UsageTimeRange enum state: " + range.name());
    }
  }

  @Nonnull
  public static ArrayList<Criterion> createCommonFilterCriteria(
      @Nonnull String resource, @Nullable Long startTime, @Nullable Long endTime) {
    ArrayList<Criterion> criteria = new ArrayList<>();
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, resource);

    criteria.add(hasUrnCriterion);
    if (startTime != null) {
      Criterion startTimeCriterion =
          buildCriterion(
              Constants.ES_FIELD_TIMESTAMP,
              Condition.GREATER_THAN_OR_EQUAL_TO,
              startTime.toString());

      criteria.add(startTimeCriterion);
    }
    if (endTime != null) {
      Criterion endTimeCriterion =
          buildCriterion(
              Constants.ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, endTime.toString());
      criteria.add(endTimeCriterion);
    }

    return criteria;
  }
}
