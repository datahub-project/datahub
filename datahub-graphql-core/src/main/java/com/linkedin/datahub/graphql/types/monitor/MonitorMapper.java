package com.linkedin.datahub.graphql.types.monitor;

import static com.linkedin.datahub.graphql.types.assertion.AssertionMapper.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AnomalyReviewState;
import com.linkedin.datahub.graphql.generated.AnomalySource;
import com.linkedin.datahub.graphql.generated.AnomalySourceProperties;
import com.linkedin.datahub.graphql.generated.AnomalySourceType;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionAdjustmentSettingsInput;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationContext;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParameters;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersType;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationSpec;
import com.linkedin.datahub.graphql.generated.AssertionMetric;
import com.linkedin.datahub.graphql.generated.AssertionMonitor;
import com.linkedin.datahub.graphql.generated.AssertionMonitorBootstrapStatus;
import com.linkedin.datahub.graphql.generated.AssertionMonitorMetricsCubeBootstrapState;
import com.linkedin.datahub.graphql.generated.AssertionMonitorMetricsCubeBootstrapStatus;
import com.linkedin.datahub.graphql.generated.AssertionMonitorSettings;
import com.linkedin.datahub.graphql.generated.AuditLogSpec;
import com.linkedin.datahub.graphql.generated.CronSchedule;
import com.linkedin.datahub.graphql.generated.DataHubOperationSpec;
import com.linkedin.datahub.graphql.generated.DatasetFieldAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetFieldAssertionSourceType;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessSourceType;
import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetSchemaSourceType;
import com.linkedin.datahub.graphql.generated.DatasetVolumeAssertionParameters;
import com.linkedin.datahub.graphql.generated.DatasetVolumeSourceType;
import com.linkedin.datahub.graphql.generated.EmbeddedAssertion;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.EvaluationTimeWindow;
import com.linkedin.datahub.graphql.generated.FreshnessFieldKind;
import com.linkedin.datahub.graphql.generated.FreshnessFieldSpec;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent;
import com.linkedin.datahub.graphql.generated.MonitorError;
import com.linkedin.datahub.graphql.generated.MonitorErrorType;
import com.linkedin.datahub.graphql.generated.MonitorInfo;
import com.linkedin.datahub.graphql.generated.MonitorMode;
import com.linkedin.datahub.graphql.generated.MonitorState;
import com.linkedin.datahub.graphql.generated.MonitorStatus;
import com.linkedin.datahub.graphql.generated.MonitorType;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.monitor.EmbeddedAssertionArray;
import com.linkedin.timeseries.TimeWindow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitorMapper {

  private static final JacksonDataCodec CODEC = new JacksonDataCodec();

  public static Monitor map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Monitor result = new Monitor();

    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(com.linkedin.datahub.graphql.generated.EntityType.MONITOR);

    final EnvelopedAspect envelopedMonitorKey = aspects.get(Constants.MONITOR_KEY_ASPECT_NAME);
    if (envelopedMonitorKey != null) {
      MonitorKey key = new MonitorKey(envelopedMonitorKey.getValue().data());
      result.setEntity(UrnToEntityMapper.map(context, key.getEntity()));
    }

    final EnvelopedAspect envelopedMonitorInfo = aspects.get(Constants.MONITOR_INFO_ASPECT_NAME);
    if (envelopedMonitorInfo != null) {
      result.setInfo(
          mapMonitorInfo(
              context,
              new com.linkedin.monitor.MonitorInfo(envelopedMonitorInfo.getValue().data())));
    }

    return result;
  }

  private static MonitorInfo mapMonitorInfo(
      @Nullable final QueryContext context, com.linkedin.monitor.MonitorInfo backendMonitorInfo) {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.valueOf(backendMonitorInfo.getType().name()));
    if (backendMonitorInfo.hasAssertionMonitor()) {
      monitorInfo.setAssertionMonitor(
          mapAssertionMonitor(context, backendMonitorInfo.getAssertionMonitor()));
    }
    if (backendMonitorInfo.hasExecutorId()) {
      monitorInfo.setExecutorId(backendMonitorInfo.getExecutorId());
    }
    monitorInfo.setStatus(mapMonitorStatus(backendMonitorInfo.getStatus()));
    return monitorInfo;
  }

  private static AssertionMonitor mapAssertionMonitor(
      @Nullable final QueryContext context,
      com.linkedin.monitor.AssertionMonitor backendAssertionMonitor) {
    AssertionMonitor assertionMonitor = new AssertionMonitor();
    List<AssertionEvaluationSpec> assertionEvaluationSpecs =
        backendAssertionMonitor.getAssertions().stream()
            .map(m -> MonitorMapper.mapAssertionEvaluationSpec(context, m))
            .collect(Collectors.toList());
    assertionMonitor.setAssertions(assertionEvaluationSpecs);
    if (backendAssertionMonitor.hasSettings()) {
      assertionMonitor.setSettings(
          mapAssertionMonitorSettings(backendAssertionMonitor.getSettings()));
    }
    if (backendAssertionMonitor.hasBootstrapStatus()) {
      assertionMonitor.setBootstrapStatus(
          mapBootstrapStatus(backendAssertionMonitor.getBootstrapStatus()));
    }
    return assertionMonitor;
  }

  private static AssertionMonitorBootstrapStatus mapBootstrapStatus(
      com.linkedin.monitor.AssertionMonitorBootstrapStatus backendBootstrapStatus) {
    AssertionMonitorBootstrapStatus bootstrapStatus = new AssertionMonitorBootstrapStatus();
    if (backendBootstrapStatus.hasMetricsCubeBootstrapStatus()) {
      bootstrapStatus.setMetricsCubeBootstrapStatus(
          mapMetricsCubeBootstrapStatus(backendBootstrapStatus.getMetricsCubeBootstrapStatus()));
    }
    return bootstrapStatus;
  }

  private static AssertionMonitorMetricsCubeBootstrapStatus mapMetricsCubeBootstrapStatus(
      com.linkedin.monitor.AssertionMonitorMetricsCubeBootstrapStatus
          backendMetricsCubeBootstrapStatus) {
    AssertionMonitorMetricsCubeBootstrapStatus metricsCubeBootstrapStatus =
        new AssertionMonitorMetricsCubeBootstrapStatus();
    metricsCubeBootstrapStatus.setState(
        AssertionMonitorMetricsCubeBootstrapState.valueOf(
            backendMetricsCubeBootstrapStatus.getState().name()));
    metricsCubeBootstrapStatus.setMessage(backendMetricsCubeBootstrapStatus.getMessage());
    return metricsCubeBootstrapStatus;
  }

  private static AssertionMonitorSettings mapAssertionMonitorSettings(
      com.linkedin.monitor.AssertionMonitorSettings backendAssertionMonitorSettings) {
    AssertionMonitorSettings assertionMonitorSettings = new AssertionMonitorSettings();
    if (backendAssertionMonitorSettings.hasAdjustmentSettings()) {
      assertionMonitorSettings.setInferenceSettings(
          mapAssertionAdjustmentSettings(backendAssertionMonitorSettings.getAdjustmentSettings()));
    }
    return assertionMonitorSettings;
  }

  @Nullable
  public static com.linkedin.monitor.AssertionMonitorSettings
      mapGraphqlAdjustmentSettingsToMonitorSettings(
          @Nullable AssertionAdjustmentSettingsInput adjustmentSettings) {
    if (adjustmentSettings == null) {
      return null;
    }
    com.linkedin.monitor.AssertionMonitorSettings assertionMonitorSettings =
        new com.linkedin.monitor.AssertionMonitorSettings();
    assertionMonitorSettings.setAdjustmentSettings(
        mapGraphQLAssertionAdjustmentSettings(adjustmentSettings));
    return assertionMonitorSettings;
  }

  private static AssertionEvaluationSpec mapAssertionEvaluationSpec(
      @Nullable final QueryContext context,
      com.linkedin.monitor.AssertionEvaluationSpec backendAssertionEvaluationSpec) {
    final AssertionEvaluationSpec assertionEvaluationSpec = new AssertionEvaluationSpec();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(backendAssertionEvaluationSpec.getAssertion().toString());
    partialAssertion.setType(EntityType.ASSERTION);
    assertionEvaluationSpec.setAssertion(partialAssertion);
    assertionEvaluationSpec.setSchedule(
        mapCronSchedule(backendAssertionEvaluationSpec.getSchedule()));
    if (backendAssertionEvaluationSpec.hasParameters()) {
      assertionEvaluationSpec.setParameters(
          mapAssertionEvaluationParameters(backendAssertionEvaluationSpec.getParameters()));
      setRawParameters(backendAssertionEvaluationSpec, assertionEvaluationSpec);
    }
    if (backendAssertionEvaluationSpec.hasContext()) {
      assertionEvaluationSpec.setContext(
          mapAssertionEvaluationContext(context, backendAssertionEvaluationSpec.getContext()));
    }
    return assertionEvaluationSpec;
  }

  private static void setRawParameters(
      com.linkedin.monitor.AssertionEvaluationSpec backendAssertionEvaluationSpec,
      AssertionEvaluationSpec assertionEvaluationSpec) {
    try {
      assertionEvaluationSpec.setRawParameters(
          CODEC.mapToString(backendAssertionEvaluationSpec.getParameters().data()));
    } catch (IOException e) {
      log.warn("Failed to serialize parameters in AssertionEvaluationSpec", e);
    }
  }

  private static AssertionEvaluationContext mapAssertionEvaluationContext(
      @Nullable final QueryContext queryContext,
      com.linkedin.monitor.AssertionEvaluationContext context) {
    AssertionEvaluationContext assertionEvaluationContext = new AssertionEvaluationContext();
    if (context.hasEmbeddedAssertions()) {
      assertionEvaluationContext.setEmbeddedAssertions(
          mapEmbeddedAssertions(queryContext, context.getEmbeddedAssertions()));
    }
    if (context.hasStdDev()) {
      assertionEvaluationContext.setStdDev(context.getStdDev());
    }
    if (context.hasInferenceDetails()) {
      assertionEvaluationContext.setInferenceDetails(
          mapInferenceDetails(queryContext, context.getInferenceDetails()));
    }
    return assertionEvaluationContext;
  }

  private static List<EmbeddedAssertion> mapEmbeddedAssertions(
      @Nullable final QueryContext context, EmbeddedAssertionArray gmsEmbeddedAssertions) {
    List<EmbeddedAssertion> embeddedAssertions = new ArrayList<>();
    gmsEmbeddedAssertions.forEach(
        embeddedAssertion -> {
          embeddedAssertions.add(mapEmbeddedAssertion(context, embeddedAssertion));
        });

    return embeddedAssertions;
  }

  @SneakyThrows
  private static EmbeddedAssertion mapEmbeddedAssertion(
      @Nullable final QueryContext context,
      com.linkedin.monitor.EmbeddedAssertion gmsEmbeddedAssertion) {
    EmbeddedAssertion embeddedAssertion = new EmbeddedAssertion();
    if (gmsEmbeddedAssertion.hasAssertion()) {
      embeddedAssertion.setAssertion(
          mapAssertionInfo(context, gmsEmbeddedAssertion.getAssertion()));
      embeddedAssertion.setRawAssertion(
          CODEC.mapToString(gmsEmbeddedAssertion.getAssertion().data()));
    }
    if (gmsEmbeddedAssertion.hasEvaluationTimeWindow()) {
      embeddedAssertion.setEvaluationTimeWindow(
          mapTimeWindow(gmsEmbeddedAssertion.getEvaluationTimeWindow()));
    }
    return embeddedAssertion;
  }

  public static EvaluationTimeWindow mapTimeWindow(TimeWindow evaluationTimeWindow) {
    EvaluationTimeWindow timeWindow = new EvaluationTimeWindow();
    timeWindow.setStartTimeMillis(evaluationTimeWindow.getStartTimeMillis());
    Calendar endTime = Calendar.getInstance();
    endTime.setTimeInMillis(evaluationTimeWindow.getStartTimeMillis());
    int multiplier = evaluationTimeWindow.getLength().getMultiple();

    switch (evaluationTimeWindow.getLength().getUnit()) {
      case SECOND:
        {
          endTime.add(Calendar.SECOND, multiplier);
          break;
        }
      case MINUTE:
        {
          endTime.add(Calendar.MINUTE, multiplier);
          break;
        }
      case HOUR:
        {
          endTime.add(Calendar.HOUR, multiplier);
          break;
        }
      case DAY:
        {
          endTime.add(Calendar.DAY_OF_YEAR, multiplier);
          break;
        }
      case WEEK:
        {
          endTime.add(Calendar.WEEK_OF_YEAR, multiplier);
          break;
        }
      case MONTH:
        {
          endTime.add(Calendar.MONTH, multiplier);
          break;
        }
      case QUARTER:
        {
          endTime.add(Calendar.MONTH, 3 * multiplier);
          break;
        }
      case YEAR:
        {
          endTime.add(Calendar.YEAR, multiplier);
          break;
        }
      case $UNKNOWN:
        {
        }
      default:
        throw new IllegalStateException(
            "Unexpected value: " + evaluationTimeWindow.getLength().getUnit());
    }
    timeWindow.setEndTimeMillis(endTime.getTimeInMillis());
    return timeWindow;
  }

  private static CronSchedule mapCronSchedule(com.linkedin.common.CronSchedule backendSchedule) {
    final CronSchedule result = new CronSchedule();
    result.setCron(backendSchedule.getCron());
    result.setTimezone(backendSchedule.getTimezone());
    return result;
  }

  public static AssertionEvaluationParameters mapAssertionEvaluationParameters(
      com.linkedin.monitor.AssertionEvaluationParameters backendAssertionEvaluationParameters) {
    final AssertionEvaluationParameters assertionEvaluationParameters =
        new AssertionEvaluationParameters();
    assertionEvaluationParameters.setType(
        AssertionEvaluationParametersType.valueOf(
            backendAssertionEvaluationParameters.getType().name()));
    if (backendAssertionEvaluationParameters.getDatasetFreshnessParameters() != null) {
      assertionEvaluationParameters.setDatasetFreshnessParameters(
          mapDatasetFreshnessAssertionParameters(
              backendAssertionEvaluationParameters.getDatasetFreshnessParameters()));
    }
    if (backendAssertionEvaluationParameters.getDatasetVolumeParameters() != null) {
      assertionEvaluationParameters.setDatasetVolumeParameters(
          mapDatasetVolumeAssertionParameters(
              backendAssertionEvaluationParameters.getDatasetVolumeParameters()));
    }
    if (backendAssertionEvaluationParameters.getDatasetFieldParameters() != null) {
      assertionEvaluationParameters.setDatasetFieldParameters(
          mapDatasetFieldAssertionParameters(
              backendAssertionEvaluationParameters.getDatasetFieldParameters()));
    }
    if (backendAssertionEvaluationParameters.getDatasetSchemaParameters() != null) {
      assertionEvaluationParameters.setDatasetSchemaParameters(
          mapDatasetSchemaAssertionParameters(
              backendAssertionEvaluationParameters.getDatasetSchemaParameters()));
    }
    return assertionEvaluationParameters;
  }

  private static DatasetFreshnessAssertionParameters mapDatasetFreshnessAssertionParameters(
      com.linkedin.monitor.DatasetFreshnessAssertionParameters
          backendDatasetFreshnessAssertionParameters) {
    final DatasetFreshnessAssertionParameters datasetFreshnessAssertionParameters =
        new DatasetFreshnessAssertionParameters();
    datasetFreshnessAssertionParameters.setSourceType(
        DatasetFreshnessSourceType.valueOf(
            backendDatasetFreshnessAssertionParameters.getSourceType().name()));
    if (backendDatasetFreshnessAssertionParameters.hasField()) {
      datasetFreshnessAssertionParameters.setField(
          mapFreshnessFieldSpec(backendDatasetFreshnessAssertionParameters.getField()));
    }
    if (backendDatasetFreshnessAssertionParameters.hasAuditLog()) {
      datasetFreshnessAssertionParameters.setAuditLog(
          mapAuditLogSpec(backendDatasetFreshnessAssertionParameters.getAuditLog()));
    }
    if (backendDatasetFreshnessAssertionParameters.hasDataHubOperation()) {
      datasetFreshnessAssertionParameters.setDataHubOperation(
          mapDataHubOperationSpec(
              backendDatasetFreshnessAssertionParameters.getDataHubOperation()));
    }
    return datasetFreshnessAssertionParameters;
  }

  private static DatasetVolumeAssertionParameters mapDatasetVolumeAssertionParameters(
      com.linkedin.monitor.DatasetVolumeAssertionParameters
          backendDatasetVolumeAssertionParameters) {
    final DatasetVolumeAssertionParameters datasetVolumeAssertionParameters =
        new DatasetVolumeAssertionParameters();
    datasetVolumeAssertionParameters.setSourceType(
        DatasetVolumeSourceType.valueOf(
            backendDatasetVolumeAssertionParameters.getSourceType().name()));
    return datasetVolumeAssertionParameters;
  }

  private static DatasetFieldAssertionParameters mapDatasetFieldAssertionParameters(
      com.linkedin.monitor.DatasetFieldAssertionParameters backendDatasetFieldAssertionParameters) {
    final DatasetFieldAssertionParameters datasetFieldAssertionParameters =
        new DatasetFieldAssertionParameters();
    datasetFieldAssertionParameters.setSourceType(
        DatasetFieldAssertionSourceType.valueOf(
            backendDatasetFieldAssertionParameters.getSourceType().name()));
    if (backendDatasetFieldAssertionParameters.hasChangedRowsField()) {
      datasetFieldAssertionParameters.setChangedRowsField(
          mapFreshnessFieldSpec(backendDatasetFieldAssertionParameters.getChangedRowsField()));
    }
    return datasetFieldAssertionParameters;
  }

  private static DatasetSchemaAssertionParameters mapDatasetSchemaAssertionParameters(
      com.linkedin.monitor.DatasetSchemaAssertionParameters
          backendDatasetSchemaAssertionParameters) {
    final DatasetSchemaAssertionParameters datasetSchemaAssertionParameters =
        new DatasetSchemaAssertionParameters();
    datasetSchemaAssertionParameters.setSourceType(
        DatasetSchemaSourceType.valueOf(
            backendDatasetSchemaAssertionParameters.getSourceType().name()));
    return datasetSchemaAssertionParameters;
  }

  private static FreshnessFieldSpec mapFreshnessFieldSpec(
      com.linkedin.assertion.FreshnessFieldSpec backendFreshnessFieldSpec) {
    FreshnessFieldSpec freshnessFieldSpec = new FreshnessFieldSpec();
    freshnessFieldSpec.setPath(backendFreshnessFieldSpec.getPath());
    freshnessFieldSpec.setType(backendFreshnessFieldSpec.getType());
    freshnessFieldSpec.setNativeType(backendFreshnessFieldSpec.getNativeType());
    if (backendFreshnessFieldSpec.hasKind()) {
      freshnessFieldSpec.setKind(
          FreshnessFieldKind.valueOf(backendFreshnessFieldSpec.getKind().name()));
    }
    return freshnessFieldSpec;
  }

  private static AuditLogSpec mapAuditLogSpec(
      com.linkedin.monitor.AuditLogSpec backendAuditLogSpec) {
    AuditLogSpec auditLogSpec = new AuditLogSpec();
    if (backendAuditLogSpec.hasOperationTypes()) {
      auditLogSpec.setOperationTypes(new ArrayList<>(backendAuditLogSpec.getOperationTypes()));
    }
    if (backendAuditLogSpec.hasUserName()) {
      auditLogSpec.setUserName(backendAuditLogSpec.getUserName());
    }
    return auditLogSpec;
  }

  private static DataHubOperationSpec mapDataHubOperationSpec(
      com.linkedin.monitor.DataHubOperationSpec backendOperationSpec) {
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

    if (backendStatus.hasState()) {
      monitorStatus.setState(MonitorState.valueOf(backendStatus.getState().toString()));
    }

    if (backendStatus.hasError()) {
      monitorStatus.setError(mapMonitorError(backendStatus.getError()));
    }

    return monitorStatus;
  }

  private static MonitorError mapMonitorError(com.linkedin.monitor.MonitorError backendError) {
    MonitorError monitorError = new MonitorError();
    monitorError.setType(MonitorErrorType.valueOf(backendError.getType().toString()));
    monitorError.setMessage(backendError.getMessage(GetMode.NULL));
    return monitorError;
  }

  public static MonitorAnomalyEvent mapMonitorAnomalyEvent(
      com.linkedin.anomaly.MonitorAnomalyEvent gmsEvent) {
    MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    if (gmsEvent.hasState()) {
      event.setState(AnomalyReviewState.valueOf(gmsEvent.getState().name()));
    }
    event.setSource(mapAnomalySource(gmsEvent.getSource()));
    event.setTimestampMillis(gmsEvent.getTimestampMillis());
    event.setCreated(gmsEvent.getCreated().getTime());
    event.setLastUpdated(gmsEvent.getLastUpdated().getTime());
    return event;
  }

  private static AnomalySource mapAnomalySource(
      com.linkedin.anomaly.AnomalySource gmsAnomalySource) {
    AnomalySource anomalySource = new AnomalySource();
    anomalySource.setType(AnomalySourceType.valueOf(gmsAnomalySource.getType().name()));

    if (gmsAnomalySource.hasSourceUrn()) {
      anomalySource.setSourceUrn(gmsAnomalySource.getSourceUrn().toString());
    }

    if (gmsAnomalySource.hasProperties()) {
      mapAnomalySourceProperties(gmsAnomalySource.getProperties());
    }
    return anomalySource;
  }

  private static AnomalySourceProperties mapAnomalySourceProperties(
      com.linkedin.anomaly.AnomalySourceProperties gmsAnomalySourceProperties) {
    AnomalySourceProperties anomalySourceProperties = new AnomalySourceProperties();
    if (gmsAnomalySourceProperties.hasAssertionMetric()) {
      final AssertionMetric assertionMetric = new AssertionMetric();
      assertionMetric.setTimestampMillis(
          gmsAnomalySourceProperties.getAssertionMetric().getTimestampMs());
      assertionMetric.setValue(gmsAnomalySourceProperties.getAssertionMetric().getValue());
      anomalySourceProperties.setAssertionMetric(assertionMetric);
    }
    return anomalySourceProperties;
  }

  private MonitorMapper() {}
}
