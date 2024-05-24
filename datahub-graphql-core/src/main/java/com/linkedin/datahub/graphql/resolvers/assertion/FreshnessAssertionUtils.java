package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.CreateFreshnessAssertionInput;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleInput;
import com.linkedin.timeseries.CalendarInterval;
import javax.annotation.Nonnull;

public class FreshnessAssertionUtils {
  @Nonnull
  public static com.linkedin.assertion.FreshnessAssertionInfo createFreshnessAssertionInfo(
      @Nonnull final CreateFreshnessAssertionInput input) {
    final com.linkedin.assertion.FreshnessAssertionInfo result =
        new com.linkedin.assertion.FreshnessAssertionInfo();
    final Urn asserteeUrn = UrnUtils.getUrn(input.getEntityUrn());

    result.setEntity(asserteeUrn);
    result.setType(FreshnessAssertionType.valueOf(input.getType().toString()));
    result.setSchedule(
        FreshnessAssertionUtils.createFreshnessAssertionSchedule(input.getSchedule()));
    if (input.getFilter() != null) {
      result.setFilter(AssertionUtils.createAssertionFilter(input.getFilter()));
    }

    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FreshnessAssertionSchedule createFreshnessAssertionSchedule(
      @Nonnull final FreshnessAssertionScheduleInput schedule) {
    final com.linkedin.assertion.FreshnessAssertionSchedule result =
        new com.linkedin.assertion.FreshnessAssertionSchedule();
    result.setType(FreshnessAssertionScheduleType.valueOf(schedule.getType().toString()));
    if (schedule.getCron() != null) {
      result.setCron(
          new FreshnessCronSchedule()
              .setCron(schedule.getCron().getCron())
              .setTimezone(schedule.getCron().getTimezone())
              .setWindowStartOffsetMs(
                  schedule.getCron().getWindowStartOffsetMs(), SetMode.IGNORE_NULL));
    }
    if (schedule.getFixedInterval() != null) {
      result.setFixedInterval(
          new FixedIntervalSchedule()
              .setMultiple(schedule.getFixedInterval().getMultiple())
              .setUnit(CalendarInterval.valueOf(schedule.getFixedInterval().getUnit().toString())));
    }
    return result;
  }

  private FreshnessAssertionUtils() {}
}
