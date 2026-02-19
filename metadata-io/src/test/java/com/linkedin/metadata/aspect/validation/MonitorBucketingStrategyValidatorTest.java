package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
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
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

public class MonitorBucketingStrategyValidatorTest {
  private static final OperationContext TEST_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(MonitorBucketingStrategyValidator.class.getName())
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

  private static final String CRON = "0 0 * * *";

  @Test
  public void testCreateWithBucketingStrategyAllowed() {
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(null, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(result.isEmpty(), "Creating a monitor with bucketing should be allowed");
  }

  @Test
  public void testUpdateNoChangeAllowed() {
    AssertionTimeBucketingStrategy strategy = buildStrategy("col1", CalendarInterval.DAY, "UTC");
    MonitorInfo oldInfo = buildVolumeMonitorInfo(strategy);
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(result.isEmpty(), "Update with identical bucketing should be allowed");
  }

  @Test
  public void testUpdateOnlyGracePeriodChangedAllowed() {
    AssertionTimeBucketingStrategy oldStrategy = buildStrategy("col1", CalendarInterval.DAY, "UTC");
    AssertionTimeBucketingStrategy newStrategy = buildStrategy("col1", CalendarInterval.DAY, "UTC");
    newStrategy.setLateArrivalGracePeriod(
        new TimeWindowSize().setUnit(CalendarInterval.HOUR).setMultiple(2));

    MonitorInfo oldInfo = buildVolumeMonitorInfo(oldStrategy);
    MonitorInfo newInfo = buildVolumeMonitorInfo(newStrategy);

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(result.isEmpty(), "Changing only lateArrivalGracePeriod should be allowed");
  }

  @Test
  public void testUpdateTimestampFieldPathRejected() {
    MonitorInfo oldInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col2", CalendarInterval.DAY, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("timestampFieldPath"));
  }

  @Test
  public void testUpdateBucketIntervalRejected() {
    MonitorInfo oldInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.HOUR, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("bucketInterval"));
  }

  @Test
  public void testUpdateTimezoneRejected() {
    MonitorInfo oldInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "America/Los_Angeles"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("timezone"));
  }

  @Test
  public void testBucketingRemovedRejected() {
    MonitorInfo oldInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));
    MonitorInfo newInfo = buildVolumeMonitorInfo(null);

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("Cannot remove the bucketing strategy"));
  }

  @Test
  public void testBucketingAddedToPreviouslyNonBucketedAllowed() {
    MonitorInfo oldInfo = buildVolumeMonitorInfo(null);
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(
        result.isEmpty(),
        "Adding bucketing to a previously non-bucketed monitor should be allowed");
  }

  @Test
  public void testDatasetFieldParametersBucketingRejected() {
    MonitorInfo oldInfo =
        buildFieldMonitorInfo(buildStrategy("ts_col", CalendarInterval.WEEK, "UTC"));
    MonitorInfo newInfo =
        buildFieldMonitorInfo(buildStrategy("ts_col", CalendarInterval.DAY, "UTC"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getMessage().contains("bucketInterval"));
  }

  @Test
  public void testMultipleBreakingChangesProduceMultipleErrors() {
    MonitorInfo oldInfo =
        buildVolumeMonitorInfo(buildStrategy("col1", CalendarInterval.DAY, "UTC"));
    MonitorInfo newInfo =
        buildVolumeMonitorInfo(buildStrategy("col2", CalendarInterval.HOUR, "America/New_York"));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertEquals(result.size(), 3, "Expected one error per changed immutable field");
  }

  @Test
  public void testMonitorWithoutAssertionMonitorSkipped() {
    MonitorInfo oldInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    MonitorInfo newInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

    ChangeMCP changeMCP = buildChangeMCP(oldInfo, newInfo);

    List<AspectValidationException> result =
        new MonitorBucketingStrategyValidator()
            .setConfig(TEST_PLUGIN_CONFIG)
            .validatePreCommitAspects(List.of(changeMCP), mock(RetrieverContext.class))
            .toList();

    assertTrue(result.isEmpty(), "Monitors without assertionMonitor should pass validation");
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
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");
    return new MonitorInfo()
        .setType(MonitorType.ASSERTION)
        .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
        .setAssertionMonitor(
            new AssertionMonitor()
                .setAssertions(
                    new com.linkedin.monitor.AssertionEvaluationSpecArray(
                        List.of(
                            new AssertionEvaluationSpec()
                                .setAssertion(assertionUrn)
                                .setSchedule(
                                    new com.linkedin.common.CronSchedule()
                                        .setCron(CRON)
                                        .setTimezone("UTC"))
                                .setParameters(params)))));
  }

  private static ChangeMCP buildChangeMCP(MonitorInfo previous, MonitorInfo current) {
    ChangeItemImpl.ChangeItemImplBuilder builder =
        ChangeItemImpl.builder()
            .changeType(ChangeType.UPSERT)
            .urn(TEST_URN)
            .aspectName(MONITOR_INFO_ASPECT_NAME)
            .recordTemplate(current)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp());

    if (previous != null) {
      SystemAspect prevAspect = mock(SystemAspect.class);
      when(prevAspect.getRecordTemplate()).thenReturn(previous);
      builder.previousSystemAspect(prevAspect);
    }

    return builder.build(TEST_CONTEXT.getAspectRetriever());
  }
}
