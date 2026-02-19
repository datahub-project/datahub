package com.linkedin.metadata.monitor;

import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionTimeBucketingStrategy;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

public class MonitorScheduleMutationHookTest {

  private static final OperationContext TEST_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(MonitorScheduleMutationHook.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(MONITOR_ENTITY_NAME)
                      .aspectName(MONITOR_INFO_ASPECT_NAME)
                      .build()))
          .build();
  private static final Urn TEST_URN =
      UrnUtils.getUrn(
          "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD),test-monitor)");

  @Test
  public void testBucketingPresent_overridesSchedule() {
    AssertionTimeBucketingStrategy strategy =
        new AssertionTimeBucketingStrategy()
            .setTimestampFieldPath("ts")
            .setBucketInterval(new TimeWindowSize().setUnit(CalendarInterval.WEEK).setMultiple(1))
            .setTimezone("America/Los_Angeles")
            .setLateArrivalGracePeriod(
                new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));

    MonitorInfo info = buildMonitorInfoWithSchedule(strategy, "0 0 * * *", "UTC");
    ChangeMCP changeMCP = buildChangeMCP(info);

    List<Pair<ChangeMCP, Boolean>> result =
        new MonitorScheduleMutationHook()
            .setConfig(TEST_PLUGIN_CONFIG)
            .applyWriteMutation(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getSecond(), "Schedule should have been mutated");

    MonitorInfo mutated = result.get(0).getFirst().getAspect(MonitorInfo.class);
    CronSchedule schedule = mutated.getAssertionMonitor().getAssertions().get(0).getSchedule();
    // WEEK bucket: Monday(1) + 1 day grace = Tuesday(2)
    assertEquals(schedule.getCron(), "0 0 * * 2");
    assertEquals(schedule.getTimezone(), "America/Los_Angeles");
  }

  @Test
  public void testNoBucketing_leavesScheduleUnchanged() {
    MonitorInfo info = buildMonitorInfoWithSchedule(null, "0 30 8 * * *", "US/Pacific");
    ChangeMCP changeMCP = buildChangeMCP(info);

    List<Pair<ChangeMCP, Boolean>> result =
        new MonitorScheduleMutationHook()
            .setConfig(TEST_PLUGIN_CONFIG)
            .applyWriteMutation(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "Schedule should not be mutated");

    MonitorInfo unchanged = result.get(0).getFirst().getAspect(MonitorInfo.class);
    CronSchedule schedule = unchanged.getAssertionMonitor().getAssertions().get(0).getSchedule();
    assertEquals(schedule.getCron(), "0 30 8 * * *");
    assertEquals(schedule.getTimezone(), "US/Pacific");
  }

  @Test
  public void testDayBucket_overridesToMidnightInBucketTz() {
    AssertionTimeBucketingStrategy strategy =
        new AssertionTimeBucketingStrategy()
            .setTimestampFieldPath("event_time")
            .setBucketInterval(new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1))
            .setTimezone("Europe/Berlin");

    MonitorInfo info = buildMonitorInfoWithSchedule(strategy, "0 15 10 * * *", "UTC");
    ChangeMCP changeMCP = buildChangeMCP(info);

    List<Pair<ChangeMCP, Boolean>> result =
        new MonitorScheduleMutationHook()
            .setConfig(TEST_PLUGIN_CONFIG)
            .applyWriteMutation(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(result.get(0).getSecond());
    CronSchedule schedule =
        result
            .get(0)
            .getFirst()
            .getAspect(MonitorInfo.class)
            .getAssertionMonitor()
            .getAssertions()
            .get(0)
            .getSchedule();
    assertEquals(schedule.getCron(), "0 0 * * *");
    assertEquals(schedule.getTimezone(), "Europe/Berlin");
  }

  // -- helpers --

  private static MonitorInfo buildMonitorInfoWithSchedule(
      AssertionTimeBucketingStrategy strategy, String cron, String cronTz) {
    DatasetVolumeAssertionParameters volumeParams =
        new DatasetVolumeAssertionParameters().setSourceType(DatasetVolumeSourceType.QUERY);
    if (strategy != null) {
      volumeParams.setTimeBucketingStrategy(strategy);
    }
    AssertionEvaluationParameters evalParams =
        new AssertionEvaluationParameters()
            .setType(AssertionEvaluationParametersType.DATASET_VOLUME)
            .setDatasetVolumeParameters(volumeParams);

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
                                .setSchedule(new CronSchedule().setCron(cron).setTimezone(cronTz))
                                .setParameters(evalParams)))));
  }

  private static ChangeMCP buildChangeMCP(MonitorInfo monitorInfo) {
    return ChangeItemImpl.builder()
        .changeType(ChangeType.UPSERT)
        .urn(TEST_URN)
        .aspectName(MONITOR_INFO_ASPECT_NAME)
        .recordTemplate(monitorInfo)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(TEST_CONTEXT.getAspectRetriever());
  }
}
