package com.linkedin.datahub.graphql.types.monitor;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParameters;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersType;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationSpec;
import com.linkedin.datahub.graphql.generated.AssertionMonitor;
import com.linkedin.datahub.graphql.generated.AuditLogSpec;
import com.linkedin.datahub.graphql.generated.CronSchedule;
import com.linkedin.datahub.graphql.generated.DataHubOperationSpec;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessSourceType;
import com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FreshnessFieldKind;
import com.linkedin.datahub.graphql.generated.FreshnessFieldSpec;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.MonitorInfo;
import com.linkedin.datahub.graphql.generated.MonitorType;
import com.linkedin.datahub.graphql.generated.MonitorStatus;
import com.linkedin.datahub.graphql.generated.MonitorMode;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class MonitorMapper {

  public static Monitor map(@Nonnull final EntityResponse entityResponse) {
    final Monitor result = new Monitor();

    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(com.linkedin.datahub.graphql.generated.EntityType.MONITOR);

    final EnvelopedAspect envelopedMonitorKey = aspects.get(Constants.MONITOR_KEY_ASPECT_NAME);
    if (envelopedMonitorKey != null) {
      MonitorKey key = new MonitorKey(envelopedMonitorKey.getValue().data());
      result.setEntity(UrnToEntityMapper.map(key.getEntity()));
    }

    final EnvelopedAspect envelopedMonitorInfo = aspects.get(Constants.MONITOR_INFO_ASPECT_NAME);
    if (envelopedMonitorInfo != null) {
      result.setInfo(mapMonitorInfo(new com.linkedin.monitor.MonitorInfo(envelopedMonitorInfo.getValue().data())));
    }

    return result;
  }

  private static MonitorInfo mapMonitorInfo(com.linkedin.monitor.MonitorInfo backendMonitorInfo) {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.valueOf(backendMonitorInfo.getType().name()));
    if (backendMonitorInfo.hasAssertionMonitor()) {
      monitorInfo.setAssertionMonitor(mapAssertionMonitor(backendMonitorInfo.getAssertionMonitor()));
    }
    if (backendMonitorInfo.hasExecutorId()) {
      monitorInfo.setExecutorId(backendMonitorInfo.getExecutorId());
    }
    monitorInfo.setStatus(mapMonitorStatus(backendMonitorInfo.getStatus()));
    return monitorInfo;
  }

  private static AssertionMonitor mapAssertionMonitor(com.linkedin.monitor.AssertionMonitor backendAssertionMonitor) {
    AssertionMonitor assertionMonitor = new AssertionMonitor();
    List<AssertionEvaluationSpec> assertionEvaluationSpecs = backendAssertionMonitor.getAssertions().stream()
        .map(MonitorMapper::mapAssertionEvaluationSpec)
        .collect(Collectors.toList());
    assertionMonitor.setAssertions(assertionEvaluationSpecs);
    return assertionMonitor;
  }

  private static AssertionEvaluationSpec mapAssertionEvaluationSpec(com.linkedin.monitor.AssertionEvaluationSpec backendAssertionEvaluationSpec) {
    final AssertionEvaluationSpec assertionEvaluationSpec = new AssertionEvaluationSpec();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(backendAssertionEvaluationSpec.getAssertion().toString());
    partialAssertion.setType(EntityType.ASSERTION);
    assertionEvaluationSpec.setAssertion(partialAssertion);
    assertionEvaluationSpec.setSchedule(mapCronSchedule(backendAssertionEvaluationSpec.getSchedule()));
    if (backendAssertionEvaluationSpec.hasParameters()) {
      assertionEvaluationSpec.setParameters(mapAssertionEvaluationParameters(backendAssertionEvaluationSpec.getParameters()));
    }
    return assertionEvaluationSpec;
  }

  private static CronSchedule mapCronSchedule(com.linkedin.common.CronSchedule backendSchedule) {
    final CronSchedule result = new CronSchedule();
    result.setCron(backendSchedule.getCron());
    result.setTimezone(backendSchedule.getTimezone());
    return result;
  }

  private static AssertionEvaluationParameters mapAssertionEvaluationParameters(
      com.linkedin.monitor.AssertionEvaluationParameters backendAssertionEvaluationParameters) {
    final AssertionEvaluationParameters assertionEvaluationParameters = new AssertionEvaluationParameters();
    assertionEvaluationParameters.setType(
        AssertionEvaluationParametersType.valueOf(backendAssertionEvaluationParameters.getType().name()));
    if (backendAssertionEvaluationParameters.getDatasetFreshnessParameters() != null) {
      assertionEvaluationParameters.setDatasetFreshnessParameters(mapDatasetFreshnessAssertionParameters(
          backendAssertionEvaluationParameters.getDatasetFreshnessParameters()));
    }
    if (backendAssertionEvaluationParameters.getDatasetVolumeParameters() != null) {
      assertionEvaluationParameters.setDatasetVolumeParameters(mapDatasetVolumeAssertionParameters(
          backendAssertionEvaluationParameters.getDatasetVolumeParameters()));
    }
    return assertionEvaluationParameters;
  }

  private static DatasetFreshnessAssertionParameters mapDatasetFreshnessAssertionParameters(
      com.linkedin.monitor.DatasetFreshnessAssertionParameters backendDatasetFreshnessAssertionParameters) {
    final DatasetFreshnessAssertionParameters datasetFreshnessAssertionParameters = new DatasetFreshnessAssertionParameters();
    datasetFreshnessAssertionParameters.setSourceType(
        DatasetFreshnessSourceType.valueOf(backendDatasetFreshnessAssertionParameters.getSourceType().name()));
    if (backendDatasetFreshnessAssertionParameters.hasField()) {
      datasetFreshnessAssertionParameters.setField(mapFreshnessFieldSpec(backendDatasetFreshnessAssertionParameters.getField()));
    }
    if (backendDatasetFreshnessAssertionParameters.hasAuditLog()) {
      datasetFreshnessAssertionParameters.setAuditLog(mapAuditLogSpec(backendDatasetFreshnessAssertionParameters.getAuditLog()));
    }
    if (backendDatasetFreshnessAssertionParameters.hasDataHubOperation()) {
      datasetFreshnessAssertionParameters.setDataHubOperation(mapDataHubOperationSpec(backendDatasetFreshnessAssertionParameters.getDataHubOperation()));
    }
    return datasetFreshnessAssertionParameters;
  }

  private static DatasetVolumeAssertionParameters mapDatasetVolumeAssertionParameters(
      com.linkedin.monitor.DatasetVolumeAssertionParameters backendDatasetVolumeAssertionParameters) {
    final DatasetVolumeAssertionParameters datasetVolumeAssertionParameters = new DatasetVolumeAssertionParameters();
    datasetVolumeAssertionParameters.setSourceType(
        DatasetVolumeSourceType.valueOf(backendDatasetVolumeAssertionParameters.getSourceType().name()));
    return datasetVolumeAssertionParameters;
  }

  private static FreshnessFieldSpec mapFreshnessFieldSpec(com.linkedin.assertion.FreshnessFieldSpec backendFreshnessFieldSpec) {
    FreshnessFieldSpec freshnessFieldSpec = new FreshnessFieldSpec();
    freshnessFieldSpec.setPath(backendFreshnessFieldSpec.getPath());
    freshnessFieldSpec.setType(backendFreshnessFieldSpec.getType());
    freshnessFieldSpec.setNativeType(backendFreshnessFieldSpec.getNativeType());
    if (backendFreshnessFieldSpec.hasKind()) {
      freshnessFieldSpec.setKind(FreshnessFieldKind.valueOf(backendFreshnessFieldSpec.getKind().name()));
    }
    return freshnessFieldSpec;
  }

  private static AuditLogSpec mapAuditLogSpec(com.linkedin.monitor.AuditLogSpec backendAuditLogSpec) {
    AuditLogSpec auditLogSpec = new AuditLogSpec();
    if (backendAuditLogSpec.hasOperationTypes()) {
      auditLogSpec.setOperationTypes(new ArrayList<>(backendAuditLogSpec.getOperationTypes()));
    }
    if (backendAuditLogSpec.hasUserName()) {
      auditLogSpec.setUserName(backendAuditLogSpec.getUserName());
    }
    return auditLogSpec;
  }

  private static DataHubOperationSpec mapDataHubOperationSpec(com.linkedin.monitor.DataHubOperationSpec backendOperationSpec) {
    DataHubOperationSpec result = new DataHubOperationSpec();
    if (backendOperationSpec.hasOperationTypes()) {
      result.setOperationTypes(backendOperationSpec.getOperationTypes());
    }
    if (backendOperationSpec.hasCustomOperationTypes()) {
      result.setCustomOperationTypes(backendOperationSpec.getCustomOperationTypes());
    }
    return result;
  }

  private static MonitorStatus mapMonitorStatus(com.linkedin.monitor.MonitorStatus backendStatus) {
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.valueOf(backendStatus.getMode().toString()));
    return monitorStatus;
  }

  private MonitorMapper() { }
}