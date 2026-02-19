package com.linkedin.metadata.monitor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionTimeBucketingStrategy;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetFieldAssertionSourceType;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindowSize;
import java.util.List;
import org.testng.annotations.Test;

public class MonitorBucketingUtilsTest {

  // -- extractTimeBucketingStrategy --

  @Test
  public void testExtractStrategy_volumeParams() {
    AssertionTimeBucketingStrategy strategy = buildStrategy("ts_col", CalendarInterval.DAY, "UTC");
    MonitorInfo info = buildVolumeMonitorInfo(strategy);
    assertEquals(MonitorBucketingUtils.extractTimeBucketingStrategy(info), strategy);
  }

  @Test
  public void testExtractStrategy_fieldParams() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("created_at", CalendarInterval.WEEK, "America/New_York");
    MonitorInfo info = buildFieldMonitorInfo(strategy);
    assertEquals(MonitorBucketingUtils.extractTimeBucketingStrategy(info), strategy);
  }

  @Test
  public void testExtractStrategy_noBucketing() {
    MonitorInfo info = buildVolumeMonitorInfo(null);
    assertNull(MonitorBucketingUtils.extractTimeBucketingStrategy(info));
  }

  @Test
  public void testExtractStrategy_noAssertionMonitor() {
    MonitorInfo info =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    assertNull(MonitorBucketingUtils.extractTimeBucketingStrategy(info));
  }

  // -- extractTimeBucketingStrategyFromParams --

  @Test
  public void testExtractFromParams_volume() {
    AssertionTimeBucketingStrategy strategy = buildStrategy("ts", CalendarInterval.DAY, "UTC");
    DatasetVolumeAssertionParameters volumeParams =
        new DatasetVolumeAssertionParameters()
            .setSourceType(DatasetVolumeSourceType.QUERY)
            .setTimeBucketingStrategy(strategy);
    AssertionEvaluationParameters params =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(volumeParams);
    assertEquals(MonitorBucketingUtils.extractTimeBucketingStrategyFromParams(params), strategy);
  }

  @Test
  public void testExtractFromParams_field() {
    AssertionTimeBucketingStrategy strategy = buildStrategy("ts", CalendarInterval.WEEK, "UTC");
    DatasetFieldAssertionParameters fieldParams =
        new DatasetFieldAssertionParameters()
            .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY)
            .setTimeBucketingStrategy(strategy);
    AssertionEvaluationParameters params =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FIELD)
            .setDatasetFieldParameters(fieldParams);
    assertEquals(MonitorBucketingUtils.extractTimeBucketingStrategyFromParams(params), strategy);
  }

  @Test
  public void testExtractFromParams_noBucketing() {
    AssertionEvaluationParameters params =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(
                new DatasetVolumeAssertionParameters()
                    .setSourceType(DatasetVolumeSourceType.QUERY));
    assertNull(MonitorBucketingUtils.extractTimeBucketingStrategyFromParams(params));
  }

  // -- deriveScheduleForBucketing --

  @Test
  public void testDeriveSchedule_nullStrategy_defaultsMidnightUtc() {
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(null);
    assertEquals(schedule.getCron(), "0 0 * * *");
    assertEquals(schedule.getTimezone(), "UTC");
  }

  @Test
  public void testDeriveSchedule_dayBucket_noGrace() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("ts", CalendarInterval.DAY, "US/Eastern");
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    assertEquals(schedule.getCron(), "0 0 * * *");
    assertEquals(schedule.getTimezone(), "US/Eastern");
  }

  @Test
  public void testDeriveSchedule_dayBucket_withGrace() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("ts", CalendarInterval.DAY, "America/Chicago");
    strategy.setLateArrivalGracePeriod(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(2));
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    assertEquals(
        schedule.getCron(),
        "0 0 * * *",
        "DAY grace shifts maturity by full days; cron stays midnight");
    assertEquals(schedule.getTimezone(), "America/Chicago");
  }

  @Test
  public void testDeriveSchedule_weekBucket_noGrace() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("ts", CalendarInterval.WEEK, "Europe/London");
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    assertEquals(schedule.getCron(), "0 0 * * 1", "No grace: fires Monday at midnight");
    assertEquals(schedule.getTimezone(), "Europe/London");
  }

  @Test
  public void testDeriveSchedule_weekBucket_2dayGrace() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("ts", CalendarInterval.WEEK, "Asia/Tokyo");
    strategy.setLateArrivalGracePeriod(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(2));
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    // Monday + 2 days grace = Wednesday = cron day 3
    assertEquals(schedule.getCron(), "0 0 * * 3", "2-day grace: fires Wednesday at midnight");
    assertEquals(schedule.getTimezone(), "Asia/Tokyo");
  }

  @Test
  public void testDeriveSchedule_weekBucket_6dayGrace() {
    AssertionTimeBucketingStrategy strategy = buildStrategy("ts", CalendarInterval.WEEK, "UTC");
    strategy.setLateArrivalGracePeriod(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(6));
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    // Monday(1) + 6 = 7 % 7 = 0 (Sunday)
    assertEquals(schedule.getCron(), "0 0 * * 0", "6-day grace wraps to Sunday");
    assertEquals(schedule.getTimezone(), "UTC");
  }

  @Test
  public void testDeriveSchedule_blankTimezone_defaultsUtc() {
    AssertionTimeBucketingStrategy strategy =
        new AssertionTimeBucketingStrategy()
            .setTimestampFieldPath("ts")
            .setBucketInterval(new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1))
            .setTimezone("");
    CronSchedule schedule = MonitorBucketingUtils.deriveScheduleForBucketing(strategy);
    assertEquals(schedule.getTimezone(), "UTC");
  }

  // -- maybeOverrideSchedule --

  @Test
  public void testMaybeOverride_bucketingPresent_overridesSchedule() {
    AssertionTimeBucketingStrategy strategy =
        buildStrategy("ts", CalendarInterval.WEEK, "America/Denver");
    strategy.setLateArrivalGracePeriod(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));
    MonitorInfo info = buildVolumeMonitorInfo(strategy);

    // Pre-condition: schedule is the generic default
    assertEquals(
        info.getAssertionMonitor().getAssertions().get(0).getSchedule().getCron(), "0 0 * * *");

    boolean mutated = MonitorBucketingUtils.maybeOverrideSchedule(info);

    assertTrue(mutated);
    CronSchedule overridden = info.getAssertionMonitor().getAssertions().get(0).getSchedule();
    // Monday(1) + 1 day grace = Tuesday(2)
    assertEquals(overridden.getCron(), "0 0 * * 2");
    assertEquals(overridden.getTimezone(), "America/Denver");
  }

  @Test
  public void testMaybeOverride_noBucketing_noOp() {
    MonitorInfo info = buildVolumeMonitorInfo(null);
    CronSchedule original = info.getAssertionMonitor().getAssertions().get(0).getSchedule();
    String originalCron = original.getCron();

    boolean mutated = MonitorBucketingUtils.maybeOverrideSchedule(info);

    assertFalse(mutated);
    assertEquals(
        info.getAssertionMonitor().getAssertions().get(0).getSchedule().getCron(), originalCron);
  }

  // -- helpers --

  private static AssertionTimeBucketingStrategy buildStrategy(
      String fieldPath, CalendarInterval intervalUnit, String timezone) {
    return new AssertionTimeBucketingStrategy()
        .setTimestampFieldPath(fieldPath)
        .setBucketInterval(new TimeWindowSize().setUnit(intervalUnit).setMultiple(1))
        .setTimezone(timezone);
  }

  private static MonitorInfo buildVolumeMonitorInfo(AssertionTimeBucketingStrategy strategy) {
    DatasetVolumeAssertionParameters volumeParams =
        new DatasetVolumeAssertionParameters().setSourceType(DatasetVolumeSourceType.QUERY);
    if (strategy != null) {
      volumeParams.setTimeBucketingStrategy(strategy);
    }
    return buildMonitorInfo(
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(volumeParams));
  }

  private static MonitorInfo buildFieldMonitorInfo(AssertionTimeBucketingStrategy strategy) {
    DatasetFieldAssertionParameters fieldParams =
        new DatasetFieldAssertionParameters()
            .setSourceType(DatasetFieldAssertionSourceType.ALL_ROWS_QUERY);
    if (strategy != null) {
      fieldParams.setTimeBucketingStrategy(strategy);
    }
    return buildMonitorInfo(
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_FIELD)
            .setDatasetFieldParameters(fieldParams));
  }

  private static MonitorInfo buildMonitorInfo(AssertionEvaluationParameters params) {
    return new MonitorInfo()
        .setType(MonitorType.ASSERTION)
        .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
        .setAssertionMonitor(
            new AssertionMonitor()
                .setAssertions(
                    new com.linkedin.monitor.AssertionEvaluationSpecArray(
                        List.of(
                            new AssertionEvaluationSpec()
                                .setAssertion(UrnUtils.getUrn("urn:li:assertion:test-assertion"))
                                .setSchedule(
                                    new CronSchedule().setCron("0 0 * * *").setTimezone("UTC"))
                                .setParameters(params)))));
  }
}
