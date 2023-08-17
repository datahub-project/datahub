package com.linkedin.datahub.graphql.types.monitor;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.DatasetVolumeSourceType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorMapperTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final String TEST_MONITOR_ID = "test";

  @Test
  public void testMapAssertionMonitor() {
    MonitorKey key = new MonitorKey();
    key.setEntity(TEST_ENTITY_URN);
    key.setId(TEST_MONITOR_ID);

    // Case 1: Without nullable fields
    MonitorInfo info = createAssertionMonitorInfoWithoutNullableFields();
    EntityResponse monitorEntityResponse = createMonitorEntityResponse(key, info);
    Monitor output = MonitorMapper.map(monitorEntityResponse);
    verifyMonitor(key, info, output);

    // Case 2: With nullable fields
    info = createAssertionMonitorInfoWithNullableFields();
    EntityResponse monitorEntityResponseWithNullables = createMonitorEntityResponse(key, info);
    output = MonitorMapper.map(monitorEntityResponseWithNullables);
    verifyMonitor(key, info, output);
  }

  private void verifyMonitor(MonitorKey key, MonitorInfo info, Monitor output) {
    Assert.assertNotNull(output);
    Assert.assertNotNull(output.getInfo());
    Assert.assertEquals(output.getEntity().getUrn(), key.getEntity().toString());
    Assert.assertEquals(info.getType().toString(), output.getInfo().getType().toString());
    Assert.assertEquals(info.getStatus().getMode().toString(), output.getInfo().getStatus().getMode().toString());
    if (info.hasAssertionMonitor()) {
      verifyAssertionMonitor(info.getAssertionMonitor(), output.getInfo().getAssertionMonitor());
    }
  }

  private void verifyAssertionMonitor(AssertionMonitor input, com.linkedin.datahub.graphql.generated.AssertionMonitor output) {
    Assert.assertEquals(output.getAssertions().get(0).getAssertion().getUrn(), input.getAssertions().get(0).getAssertion().toString());
    Assert.assertEquals(output.getAssertions().get(0).getSchedule().getCron(), input.getAssertions().get(0).getSchedule().getCron());
    Assert.assertEquals(output.getAssertions().get(0).getSchedule().getTimezone(), input.getAssertions().get(0).getSchedule().getTimezone());
    Assert.assertEquals(output.getAssertions().get(0).getParameters().getType().toString(),
        input.getAssertions().get(0).getParameters().getType().toString());

    if (input.getAssertions().get(0).getParameters().hasDatasetFreshnessParameters()) {
      // Verify the dataset FRESHNESS parameters.
      DatasetFreshnessAssertionParameters inputParams = input.getAssertions().get(0).getParameters().getDatasetFreshnessParameters();
      com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetFreshnessParameters();
      Assert.assertEquals(outputParams.getSourceType().toString(), inputParams.getSourceType().toString());

      if (inputParams.hasField()) {
        FreshnessFieldSpec inputFieldSpec = inputParams.getField();
        com.linkedin.datahub.graphql.generated.FreshnessFieldSpec outputFieldSpec = outputParams.getField();
        Assert.assertEquals(outputFieldSpec.getNativeType(), inputFieldSpec.getNativeType());
        Assert.assertEquals(outputFieldSpec.getType(), inputFieldSpec.getType());
        Assert.assertEquals(outputFieldSpec.getPath(), inputFieldSpec.getPath());

        if (inputFieldSpec.hasKind()) {
          Assert.assertEquals(outputFieldSpec.getKind().name(), inputFieldSpec.getKind().name());
        }
      }

      if (inputParams.hasAuditLog()) {
        AuditLogSpec inputAuditLogSpec = inputParams.getAuditLog();
        com.linkedin.datahub.graphql.generated.AuditLogSpec outputAuditLogSpec = outputParams.getAuditLog();
        if (inputAuditLogSpec.hasOperationTypes()) {
          Assert.assertEquals(outputAuditLogSpec.getOperationTypes(), inputAuditLogSpec.getOperationTypes());
        }
        if (inputAuditLogSpec.hasUserName()) {
          Assert.assertEquals(outputAuditLogSpec.getUserName(), inputAuditLogSpec.getUserName());
        }
      }
    }
    if (input.getAssertions().get(0).getParameters().hasDatasetVolumeParameters()) {
      // Verify the dataset FRESHNESS parameters.
      DatasetVolumeAssertionParameters
          inputParams = input.getAssertions().get(0).getParameters().getDatasetVolumeParameters();
      com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParameters outputParams =
          output.getAssertions().get(0).getParameters().getDatasetVolumeParameters();
      Assert.assertEquals(outputParams.getSourceType().toString(), inputParams.getSourceType().toString());
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
    info.setAssertionMonitor(new AssertionMonitor()
      .setAssertions(new AssertionEvaluationSpecArray(
        ImmutableList.of(
          new AssertionEvaluationSpec()
            .setAssertion(TEST_ASSERTION_URN)
            .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
            .setParameters(new AssertionEvaluationParameters()
                .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_FRESHNESS)
            )
        )
      )));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }

  private MonitorInfo createAssertionMonitorInfoWithNullableFields() {
    MonitorInfo info = new MonitorInfo();
    info.setType(MonitorType.ASSERTION);
    info.setAssertionMonitor(new AssertionMonitor()
      .setAssertions(new AssertionEvaluationSpecArray(
          ImmutableList.of(
              new AssertionEvaluationSpec()
                .setAssertion(TEST_ASSERTION_URN)
                .setSchedule(new CronSchedule().setCron("1 * * * *").setTimezone("America/Los_Angeles"))
                .setParameters(new AssertionEvaluationParameters()
                    .setType(com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_FRESHNESS)
                    .setDatasetFreshnessParameters(new DatasetFreshnessAssertionParameters()
                        .setSourceType(DatasetFreshnessSourceType.FIELD_VALUE)
                        .setField(new FreshnessFieldSpec()
                            .setNativeType("varchar").setType("STRING").setPath("name").setKind(FreshnessFieldKind.LAST_MODIFIED)
                        )
                    )
                    .setDatasetVolumeParameters(new DatasetVolumeAssertionParameters()
                      .setSourceType(DatasetVolumeSourceType.QUERY)
                    )
                )
          )
      )));
    info.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));
    return info;
  }
}
