package com.linkedin.datahub.graphql.resolvers.assertion;

import javax.annotation.Nonnull;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleInput;
import com.linkedin.timeseries.CalendarInterval;

public class FreshnessAssertionUtils {

    @Nonnull
    public static com.linkedin.assertion.FreshnessAssertionSchedule createFreshnessAssertionSchedule(@Nonnull final FreshnessAssertionScheduleInput schedule) {
        final com.linkedin.assertion.FreshnessAssertionSchedule result = new com.linkedin.assertion.FreshnessAssertionSchedule();
        result.setType(FreshnessAssertionScheduleType.valueOf(schedule.getType().toString()));
        if (schedule.getCron() != null) {
            result.setCron(new FreshnessCronSchedule()
                    .setCron(schedule.getCron().getCron())
                    .setTimezone(schedule.getCron().getTimezone())
                    .setWindowStartOffsetMs(schedule.getCron().getWindowStartOffsetMs(), SetMode.IGNORE_NULL));
        }
        if (schedule.getFixedInterval() != null) {
            result.setFixedInterval(new FixedIntervalSchedule()
                    .setMultiple(schedule.getFixedInterval().getMultiple())
                    .setUnit(CalendarInterval.valueOf(schedule.getFixedInterval().getUnit().toString())));
        }
        return result;
    }

    private FreshnessAssertionUtils() { }
}
