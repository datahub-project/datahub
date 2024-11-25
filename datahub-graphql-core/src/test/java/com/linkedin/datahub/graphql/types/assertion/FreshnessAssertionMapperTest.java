package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.timeseries.CalendarInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FreshnessAssertionMapperTest {
  @Test
  public void testMapCronFreshnessAssertionInfo() throws Exception {
    FreshnessAssertionInfo freshnessAssertionInfo =
        new FreshnessAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"))
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.CRON)
                    .setCron(
                        new FreshnessCronSchedule()
                            .setCron("0 0 0 * * ? *")
                            .setTimezone("America/Los_Angeles")
                            .setWindowStartOffsetMs(10L)));

    com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo result =
        FreshnessAssertionMapper.mapFreshnessAssertionInfo(null, freshnessAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.FreshnessAssertionType.DATASET_CHANGE);
    Assert.assertEquals(
        result.getFilter().getType(), com.linkedin.datahub.graphql.generated.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value > 5;");
    Assert.assertEquals(
        result.getSchedule().getType(),
        com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleType.CRON);
    Assert.assertEquals(result.getSchedule().getCron().getCron(), "0 0 0 * * ? *");
    Assert.assertEquals(result.getSchedule().getCron().getTimezone(), "America/Los_Angeles");
    Assert.assertEquals(result.getSchedule().getCron().getWindowStartOffsetMs(), Long.valueOf(10L));
  }

  @Test
  public void testMapFixedIntervalFreshnessAssertionInfo() throws Exception {
    FreshnessAssertionInfo freshnessAssertionInfo =
        new FreshnessAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"))
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
                    .setFixedInterval(
                        new FixedIntervalSchedule().setUnit(CalendarInterval.DAY).setMultiple(10)));

    com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo result =
        FreshnessAssertionMapper.mapFreshnessAssertionInfo(null, freshnessAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(),
        com.linkedin.datahub.graphql.generated.FreshnessAssertionType.DATASET_CHANGE);
    Assert.assertEquals(
        result.getFilter().getType(), com.linkedin.datahub.graphql.generated.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value > 5;");
    Assert.assertEquals(
        result.getSchedule().getType(),
        com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleType.FIXED_INTERVAL);
    Assert.assertEquals(
        result.getSchedule().getFixedInterval().getUnit(),
        com.linkedin.datahub.graphql.generated.DateInterval.DAY);
    Assert.assertEquals(result.getSchedule().getFixedInterval().getMultiple(), 10);
  }
}
