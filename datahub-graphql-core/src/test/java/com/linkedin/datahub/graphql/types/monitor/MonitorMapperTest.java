package com.linkedin.datahub.graphql.types.monitor;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.monitor.AssertionEvaluationContext;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionMonitorBootstrapStatus;
import com.linkedin.monitor.AssertionMonitorMetricsCubeBootstrapState;
import com.linkedin.monitor.AssertionMonitorMetricsCubeBootstrapStatus;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.EmbeddedAssertion;
import com.linkedin.monitor.EmbeddedAssertionArray;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindow;
import com.linkedin.timeseries.TimeWindowSize;
import java.io.IOException;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorMapperTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final String TEST_MONITOR_ID = "test";

  private static final JacksonDataCodec CODEC = new JacksonDataCodec();

  @Test
  public void testMapAssertionMonitor() throws IOException {
    MonitorKey key = new MonitorKey();
    key.setEntity(TEST_ENTITY_URN);
    key.setId(TEST_MONITOR_ID);

    // Case 1: Without nullable fields
    MonitorInfo info = createAssertionMonitorInfoWithoutNullableFields();
    EntityResponse monitorEntityResponse = createMonitorEntityResponse(key, info);
    Monitor output = MonitorMapper.map(null, monitorEntityResponse);
    verifyMonitor(key, info, output);

    // Case 2: With nullable fields
    info = createAssertionMonitorInfoWithNullableFields();
    EntityResponse monitorEntityResponseWithNullables = createMonitorEntityResponse(key, info);
    output = MonitorMapper.map(null, monitorEntityResponseWithNullables);
    verifyMonitor(key, info, output);
  }

  private void verifyMonitor(MonitorKey key, MonitorInfo info, Monitor output) throws IOException {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(output.getEntity().getUrn(), key.getEntity().toString());
    Assert.assertEquals(info.getType().toString(), output.getInfo().getType().toString());
    Assert.assertEquals(
        info.getStatus().getMode().toString(), output.getInfo().getStatus().getMode().toString());
    if (info.hasAssertionMonitor()) {
      verifyAssertionMonitor(info.getAssertionMonitor(), output.getInfo().getAssertionMonitor());
    }
  }

  private void verifyAssertionMonitor(
      AssertionMonitor input, com.linkedin.datahub.graphql.generated.AssertionMonitor output)
      throws IOException {
    Assert.assertEquals(
        output.getAssertions().get(0).getAssertion().getUrn(),
        input.getAssertions().get(0).getAssertion().toString());
    Assert.assertEquals(
        output.getAssertions().get(0).getSchedule().getCron(),
        input.getAssertions().get(0).getSchedule().getCron());
    Assert.assertEquals(
        output.getAssertions().get(0).getSchedule().getTimezone(),
        input.getAssertions().get(0).getSchedule().getTimezone());
    Assert.assertEquals(
        output.getAssertions().get(0).getParameters().getType().toString(),
        input.getAssertions().get(0).getParameters().getType().toString());
    Assert.assertEquals(
        output.getAssertions().get(0).getRawParameters(),
        CODEC.mapToString(input.getAssertions().get(0).getParameters().data()));

    if (input.hasBootstrapStatus()) {
      Assert.assertEquals(
          output.getBootstrapStatus().getMetricsCubeBootstrapStatus().getState().toString(),
          input.getBootstrapStatus().getMetricsCubeBootstrapStatus().getState().toString());
      Assert.assertEquals(
          output.getBootstrapStatus().getMetricsCubeBootstrapStatus().getMessage(),
          input.getBootstrapStatus().getMetricsCubeBootstrapStatus().getMessage());
    }
    if (input.getAssertions().get(0).getParameters().hasDatasetFreshnessParameters()) {
      // Verify the dataset FRESHNESS parameters.
      DatasetFreshnessAssertionParameters inputParams =
          input.getAssertions().get(0).getParameters().getDatasetFreshnessParameters();
      com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetFreshnessParameters();
      Assert.assertEquals(
          outputParams.getSourceType().toString(), inputParams.getSourceType().toString());

      if (inputParams.hasField()) {
        FreshnessFieldSpec inputFieldSpec = inputParams.getField();
        com.linkedin.datahub.graphql.generated.FreshnessFieldSpec outputFieldSpec =
            outputParams.getField();
        Assert.assertEquals(outputFieldSpec.getNativeType(), inputFieldSpec.getNativeType());
        Assert.assertEquals(outputFieldSpec.getType(), inputFieldSpec.getType());
        Assert.assertEquals(outputFieldSpec.getPath(), inputFieldSpec.getPath());

        if (inputFieldSpec.hasKind()) {
          Assert.assertEquals(outputFieldSpec.getKind().name(), inputFieldSpec.getKind().name());
        }
      }

      if (inputParams.hasAuditLog()) {
        AuditLogSpec inputAuditLogSpec = inputParams.getAuditLog();
        com.linkedin.datahub.graphql.generated.AuditLogSpec outputAuditLogSpec =
            outputParams.getAuditLog();
        if (inputAuditLogSpec.hasOperationTypes()) {
          Assert.assertEquals(
              outputAuditLogSpec.getOperationTypes(), inputAuditLogSpec.getOperationTypes());
        }
        if (inputAuditLogSpec.hasUserName()) {
          Assert.assertEquals(outputAuditLogSpec.getUserName(), inputAuditLogSpec.getUserName());
        }
      }
    }
    if (input.getAssertions().get(0).getParameters().hasDatasetVolumeParameters()) {
      // Verify the dataset FRESHNESS parameters.
      DatasetVolumeAssertionParameters inputParams =
          input.getAssertions().get(0).getParameters().getDatasetVolumeParameters();
      com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetVolumeParameters();
      Assert.assertEquals(
          outputParams.getSourceType().toString(), inputParams.getSourceType().toString());
    }

    if (input.getAssertions().get(0).getParameters().hasDatasetSchemaParameters()) {
      // Verify the dataset SCHEMA parameters.
      DatasetSchemaAssertionParameters inputParams =
          input.getAssertions().get(0).getParameters().getDatasetSchemaParameters();
      com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetSchemaParameters();
      Assert.assertEquals(
          outputParams.getSourceType().toString(), inputParams.getSourceType().toString());
    }

    if (input.getAssertions().get(0).hasContext()
        && input.getAssertions().get(0).getContext().hasEmbeddedAssertions()) {
      EmbeddedAssertion inputAssertion =
          input.getAssertions().get(0).getContext().getEmbeddedAssertions().get(0);
      com.linkedin.datahub.graphql.generated.EmbeddedAssertion outputAssertion =
          output.getAssertions().get(0).getContext().getEmbeddedAssertions().get(0);

      Assert.assertEquals(
          inputAssertion.getEvaluationTimeWindow().getStartTimeMillis(),
          outputAssertion.getEvaluationTimeWindow().getStartTimeMillis());
      Assert.assertEquals(
          inputAssertion.getEvaluationTimeWindow().getStartTimeMillis()
              + (2 * 24 * 60 * 60 * 1000), // 2 days into milliseconds
          outputAssertion.getEvaluationTimeWindow().getEndTimeMillis());
      Assert.assertEquals(
          inputAssertion.getAssertion().getType().toString(),
          outputAssertion.getAssertion().getType().toString());
      Assert.assertEquals(
          CODEC.mapToString(inputAssertion.getAssertion().data()),
          outputAssertion.getRawAssertion());
    }
  }

  private EntityResponse createMonitorEntityResponse(final MonitorKey key, final MonitorInfo info) {
    EnvelopedAspect envelopedMonitorKey = createEnvelopedAspect(key.data());
    EnvelopedAspect envelopedMonitorInfo = createEnvelopedAspect(info.data());
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:monitor:1"));
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
    entityResponse.getAspects().put(Constants.MONITOR_KEY_ASPECT_NAME, envelopedMonitorKey);
    entityResponse.getAspects().put(Constants.MONITOR_INFO_ASPECT_NAME, envelopedMonitorInfo);
    return entityResponse;
  }

  private EnvelopedAspect createEnvelopedAspect(DataMap dataMap) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(dataMap));
    return envelopedAspect;
  }

  private MonitorInfo createAssertionMonitorInfoWithoutNullableFields() {
    MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(
        new AssertionMonitor()
            .setAssertions(
                new AssertionEvaluationSpecArray(
                    ImmutableList.of(
                        new AssertionEvaluationSpec()
                            .setAssertion(TEST_ASSERTION_URN)
                            .setSchedule(
                                new CronSchedule()
                                    .setCron("1 * * * *")
                                    .setTimezone("America/Los_Angeles"))
                            .setParameters(
                                new AssertionEvaluationParameters()
                                    .setType(
                                        com.linkedin.monitor.AssertionEvaluationParametersType
                                            .DATASET_FRESHNESS))))));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  private MonitorInfo createAssertionMonitorInfoWithNullableFields() {
    MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(
        new AssertionMonitor()
            .setBootstrapStatus(
                new AssertionMonitorBootstrapStatus()
                    .setMetricsCubeBootstrapStatus(
                        new AssertionMonitorMetricsCubeBootstrapStatus()
                            .setState(AssertionMonitorMetricsCubeBootstrapState.COMPLETED)))
            .setAssertions(
                new AssertionEvaluationSpecArray(
                    ImmutableList.of(
                        new AssertionEvaluationSpec()
                            .setAssertion(TEST_ASSERTION_URN)
                            .setSchedule(
                                new CronSchedule()
                                    .setCron("1 * * * *")
                                    .setTimezone("America/Los_Angeles"))
                            .setParameters(
                                new AssertionEvaluationParameters()
                                    .setType(
                                        com.linkedin.monitor.AssertionEvaluationParametersType
                                            .DATASET_FRESHNESS)
                                    .setDatasetFreshnessParameters(
                                        new DatasetFreshnessAssertionParameters()
                                            .setSourceType(DatasetFreshnessSourceType.FIELD_VALUE)
                                            .setField(
                                                new FreshnessFieldSpec()
                                                    .setNativeType("varchar")
                                                    .setType("STRING")
                                                    .setPath("name")
                                                    .setKind(FreshnessFieldKind.LAST_MODIFIED)))
                                    .setDatasetVolumeParameters(
                                        new DatasetVolumeAssertionParameters()
                                            .setSourceType(DatasetVolumeSourceType.QUERY))
                                    .setDatasetSchemaParameters(
                                        new DatasetSchemaAssertionParameters()
                                            .setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA)))
                            .setContext(
                                new AssertionEvaluationContext()
                                    .setEmbeddedAssertions(
                                        new EmbeddedAssertionArray(
                                            ImmutableList.of(
                                                new EmbeddedAssertion()
                                                    .setAssertion(
                                                        createFreshnessAssertionInfoWithoutNullableFields())
                                                    .setEvaluationTimeWindow(
                                                        new TimeWindow()
                                                            .setStartTimeMillis(1000000000L)
                                                            .setLength(
                                                                new TimeWindowSize()
                                                                    .setMultiple(2)
                                                                    .setUnit(
                                                                        CalendarInterval
                                                                            .DAY)))))))))));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  public AssertionInfo createFreshnessAssertionInfoWithoutNullableFields() {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertionInfo = new FreshnessAssertionInfo();
    freshnessAssertionInfo.setEntity(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)"));
    freshnessAssertionInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    info.setFreshnessAssertion(freshnessAssertionInfo);
    return info;
  }
}
