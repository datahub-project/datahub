package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.FixedIntervalScheduleInput;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleInput;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleType;
import com.linkedin.datahub.graphql.generated.FreshnessCronScheduleInput;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FreshnessAssertionUtilsTest {
  @Test
  public void testCreateFreshnessAssertionCronSchedule() {
    final FreshnessAssertionScheduleInput schedule = new FreshnessAssertionScheduleInput();
    final FreshnessCronScheduleInput cron = new FreshnessCronScheduleInput();
    cron.setCron("0 0 0 * * ?");
    cron.setTimezone("America/Los_Angeles");
    cron.setWindowStartOffsetMs(0L);
    schedule.setType(FreshnessAssertionScheduleType.CRON);
    schedule.setCron(cron);

    final com.linkedin.assertion.FreshnessAssertionSchedule result =
        FreshnessAssertionUtils.createFreshnessAssertionSchedule(schedule);
    Assert.assertEquals(
        result.getType(), com.linkedin.assertion.FreshnessAssertionScheduleType.CRON);
    Assert.assertEquals(result.getCron().getCron(), "0 0 0 * * ?");
    Assert.assertEquals(result.getCron().getTimezone(), "America/Los_Angeles");
    Assert.assertEquals(result.getCron().getWindowStartOffsetMs(), Long.valueOf(0L));
  }

  @Test
  public void testCreateFreshnessAssertionFixedIntervalSchedule() {
    final FreshnessAssertionScheduleInput schedule = new FreshnessAssertionScheduleInput();
    final FixedIntervalScheduleInput fixedInterval = new FixedIntervalScheduleInput();
    fixedInterval.setUnit(DateInterval.DAY);
    fixedInterval.setMultiple(1);
    schedule.setType(FreshnessAssertionScheduleType.FIXED_INTERVAL);
    schedule.setFixedInterval(fixedInterval);

    final com.linkedin.assertion.FreshnessAssertionSchedule result =
        FreshnessAssertionUtils.createFreshnessAssertionSchedule(schedule);
    Assert.assertEquals(
        result.getType(), com.linkedin.assertion.FreshnessAssertionScheduleType.FIXED_INTERVAL);
    Assert.assertEquals(
        result.getFixedInterval().getUnit(), com.linkedin.timeseries.CalendarInterval.DAY);
    Assert.assertEquals(result.getFixedInterval().getMultiple(), Integer.valueOf(1));
  }
}
