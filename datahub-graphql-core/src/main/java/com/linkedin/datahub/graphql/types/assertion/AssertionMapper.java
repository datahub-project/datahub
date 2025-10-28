package com.linkedin.datahub.graphql.types.assertion;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionExclusionWindow;
import com.linkedin.assertion.AssertionExclusionWindowArray;
import com.linkedin.assertion.AssertionInferenceDetails;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AbsoluteTimeWindow;
import com.linkedin.datahub.graphql.generated.AbsoluteTimeWindowInput;
import com.linkedin.datahub.graphql.generated.AdjustmentAlgorithm;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionAdjustmentSettings;
import com.linkedin.datahub.graphql.generated.AssertionAdjustmentSettingsInput;
import com.linkedin.datahub.graphql.generated.AssertionExclusionWindowType;
import com.linkedin.datahub.graphql.generated.AssertionSource;
import com.linkedin.datahub.graphql.generated.AssertionSourceType;
import com.linkedin.datahub.graphql.generated.AssertionStdAggregation;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameter;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParameters;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.CustomAssertionInfo;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetAssertionInfo;
import com.linkedin.datahub.graphql.generated.DatasetAssertionScope;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FieldAssertionInfo;
import com.linkedin.datahub.graphql.generated.FixedIntervalSchedule;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo;
import com.linkedin.datahub.graphql.generated.SchemaAssertionCompatibility;
import com.linkedin.datahub.graphql.generated.SchemaAssertionField;
import com.linkedin.datahub.graphql.generated.SchemaAssertionInfo;
import com.linkedin.datahub.graphql.generated.SchemaFieldRef;
import com.linkedin.datahub.graphql.generated.SqlAssertionInfo;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.VolumeAssertionInfo;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.LineageFeaturesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.hooks.AssertionInfoMutator;
import com.linkedin.metadata.search.features.LineageFeatures;
import com.linkedin.schema.SchemaField;
import com.linkedin.timeseries.DayOfWeekArray;
import com.linkedin.timeseries.WeeklyWindow;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionMapper {

  public static Assertion map(@Nullable QueryContext context, final EntityResponse entityResponse) {
    final Assertion result = new Assertion();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ASSERTION);

    final EnvelopedAspect envelopedAssertionInfo =
        aspects.get(Constants.ASSERTION_INFO_ASPECT_NAME);
    if (envelopedAssertionInfo != null) {
      final AssertionInfo assertionInfo =
          new AssertionInfo(envelopedAssertionInfo.getValue().data());
      result.setInfo(mapAssertionInfo(context, assertionInfo));

      try {
        // Don't want to break entire graphql call if we have data corruption
        final @Nonnull Urn datasetUrn =
            Optional.ofNullable(assertionInfo.getEntityUrn())
                .orElse(AssertionInfoMutator.getEntityFromAssertionInfo(assertionInfo));
        final Dataset dataset = new Dataset();
        dataset.setUrn(datasetUrn.toString());
        dataset.setType(EntityType.DATASET);
        result.setDataset(dataset);
      } catch (RuntimeException e) {
        log.warn("Failed to map assertion {}: {}", entityUrn, e.getMessage());
      }
    }

    final EnvelopedAspect envelopedAssertionActions =
        aspects.get(Constants.ASSERTION_ACTIONS_ASPECT_NAME);
    if (envelopedAssertionActions != null) {
      result.setActions(
          mapAssertionActions(new AssertionActions(envelopedAssertionActions.getValue().data())));
    }

    final EnvelopedAspect envelopedPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      result.setPlatform(mapPlatform(new DataPlatformInstance(data)));
      result.setDataPlatformInstance(
          DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(data)));
    } else {
      final DataPlatform unknownPlatform = new DataPlatform();
      unknownPlatform.setUrn(Constants.UNKNOWN_DATA_PLATFORM);
      result.setPlatform(unknownPlatform);
    }

    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setStatus(mapStatus(new Status(envelopedStatus.getValue().data())));
    }

    final EnvelopedAspect envelopedTags = aspects.get(GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedTags.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedLineageFeatures =
        aspects.get(Constants.LINEAGE_FEATURES_ASPECT_NAME);
    if (envelopedLineageFeatures != null) {
      result.setLineageFeatures(
          LineageFeaturesMapper.map(
              context, new LineageFeatures(envelopedLineageFeatures.getValue().data())));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.Status mapStatus(Status status) {
    final com.linkedin.datahub.graphql.generated.Status result =
        new com.linkedin.datahub.graphql.generated.Status();
    result.setRemoved(status.isRemoved());
    return result;
  }

  public static com.linkedin.datahub.graphql.generated.AssertionInferenceDetails
      mapInferenceDetails(
          @Nullable QueryContext context, AssertionInferenceDetails gmsInferenceDetails) {
    com.linkedin.datahub.graphql.generated.AssertionInferenceDetails inferenceDetails =
        new com.linkedin.datahub.graphql.generated.AssertionInferenceDetails();

    inferenceDetails.setModelId(gmsInferenceDetails.getModelId());
    inferenceDetails.setModelVersion(gmsInferenceDetails.getModelVersion());
    if (gmsInferenceDetails.hasParameters()) {
      inferenceDetails.setParameters(
          StringMapMapper.map(context, gmsInferenceDetails.getParameters()));
    }
    inferenceDetails.setConfidence(gmsInferenceDetails.getConfidence());
    if (gmsInferenceDetails.hasAdjustmentSettings()) {
      inferenceDetails.setAdjustmentSettings(
          mapAssertionAdjustmentSettings(gmsInferenceDetails.getAdjustmentSettings()));
    }
    inferenceDetails.setGeneratedAt(gmsInferenceDetails.getGeneratedAt());

    // Start here
    return inferenceDetails;
  }

  public static com.linkedin.assertion.AssertionAdjustmentSettings
      mapGraphQLAssertionAdjustmentSettings(
          @Nonnull AssertionAdjustmentSettingsInput gqlAdjustmentSettings) {

    com.linkedin.assertion.AssertionAdjustmentSettings adjustmentSettings =
        new com.linkedin.assertion.AssertionAdjustmentSettings();
    if (gqlAdjustmentSettings.getAlgorithm() != null) {
      adjustmentSettings.setAlgorithm(
          com.linkedin.assertion.AdjustmentAlgorithm.valueOf(
              gqlAdjustmentSettings.getAlgorithm().name()));
    }
    adjustmentSettings.setAlgorithmName(
        gqlAdjustmentSettings.getAlgorithmName(), SetMode.IGNORE_NULL);
    if (gqlAdjustmentSettings.getSensitivity() != null) {
      adjustmentSettings.setSensitivity(
          mapGraphqlAssertionMonitorSensitivity(gqlAdjustmentSettings.getSensitivity()),
          SetMode.IGNORE_NULL);
    }
    if (gqlAdjustmentSettings.getTrainingDataLookbackWindowDays() != null) {
      adjustmentSettings.setTrainingDataLookbackWindowDays(
          gqlAdjustmentSettings.getTrainingDataLookbackWindowDays());
    }
    if (gqlAdjustmentSettings.getExclusionWindows() != null) {
      adjustmentSettings.setExclusionWindows(
          mapGraphQLAssertionExclusionWindows(gqlAdjustmentSettings.getExclusionWindows()));
    }
    if (gqlAdjustmentSettings.getContext() != null) {
      List<StringMapEntryInput> entries = gqlAdjustmentSettings.getContext();

      Map<String, String> targetMap = new HashMap<>();
      for (StringMapEntryInput entry : entries) {
        targetMap.put(entry.getKey(), entry.getValue());
      }
      adjustmentSettings.setContext(new StringMap(targetMap));
    }
    return adjustmentSettings;
  }

  public static AssertionAdjustmentSettings mapAssertionAdjustmentSettings(
      @Nonnull com.linkedin.assertion.AssertionAdjustmentSettings gmsAdjustmentSettings) {
    AssertionAdjustmentSettings adjustmentSettings = new AssertionAdjustmentSettings();
    if (gmsAdjustmentSettings.hasAlgorithm()) {
      adjustmentSettings.setAlgorithm(
          AdjustmentAlgorithm.valueOf(gmsAdjustmentSettings.getAlgorithm().toString()));
    }
    if (gmsAdjustmentSettings.hasAlgorithmName()) {
      adjustmentSettings.setAlgorithmName(gmsAdjustmentSettings.getAlgorithmName());
    }
    if (gmsAdjustmentSettings.hasSensitivity()) {
      adjustmentSettings.setSensitivity(
          mapAssertionMonitorSensitivity(gmsAdjustmentSettings.getSensitivity()));
    }
    if (gmsAdjustmentSettings.hasTrainingDataLookbackWindowDays()) {
      adjustmentSettings.setTrainingDataLookbackWindowDays(
          gmsAdjustmentSettings.getTrainingDataLookbackWindowDays().intValue());
    }
    if (gmsAdjustmentSettings.hasExclusionWindows()) {
      adjustmentSettings.setExclusionWindows(
          mapAssertionExclusionWindows(gmsAdjustmentSettings.getExclusionWindows()));
    }
    if (gmsAdjustmentSettings.hasContext()) {
      adjustmentSettings.setContext(StringMapMapper.map(null, gmsAdjustmentSettings.getContext()));
    }
    return adjustmentSettings;
  }

  public static com.linkedin.datahub.graphql.generated.AssertionInfo mapAssertionInfo(
      @Nullable QueryContext context, final AssertionInfo gmsAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.AssertionInfo assertionInfo =
        new com.linkedin.datahub.graphql.generated.AssertionInfo();
    assertionInfo.setType(AssertionType.valueOf(gmsAssertionInfo.getType().name()));

    if (gmsAssertionInfo.hasLastUpdated()) {
      assertionInfo.setLastUpdated(
          new AuditStamp(
              gmsAssertionInfo.getLastUpdated().getTime(),
              gmsAssertionInfo.getLastUpdated().getActor().toString()));
    }
    if (gmsAssertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion =
          mapDatasetAssertionInfo(context, gmsAssertionInfo.getDatasetAssertion());
      assertionInfo.setDatasetAssertion(datasetAssertion);
    }
    // Description
    if (gmsAssertionInfo.hasDescription()) {
      assertionInfo.setDescription(gmsAssertionInfo.getDescription());
    }
    // Note
    if (gmsAssertionInfo.hasNote()) {
      assertionInfo.setNote(gmsAssertionInfo.getNote().getContent());
    }
    // FRESHNESS Assertions
    if (gmsAssertionInfo.hasFreshnessAssertion()) {
      FreshnessAssertionInfo freshnessAssertionInfo =
          FreshnessAssertionMapper.mapFreshnessAssertionInfo(
              context, gmsAssertionInfo.getFreshnessAssertion());
      assertionInfo.setFreshnessAssertion(freshnessAssertionInfo);
    }
    // VOLUME Assertions
    if (gmsAssertionInfo.hasVolumeAssertion()) {
      VolumeAssertionInfo volumeAssertionInfo =
          VolumeAssertionMapper.mapVolumeAssertionInfo(
              context, gmsAssertionInfo.getVolumeAssertion());
      assertionInfo.setVolumeAssertion(volumeAssertionInfo);
    }
    // SQL Assertions
    if (gmsAssertionInfo.hasSqlAssertion()) {
      SqlAssertionInfo sqlAssertionInfo =
          SqlAssertionMapper.mapSqlAssertionInfo(gmsAssertionInfo.getSqlAssertion());
      assertionInfo.setSqlAssertion(sqlAssertionInfo);
    }
    // FIELD Assertions
    if (gmsAssertionInfo.hasFieldAssertion()) {
      FieldAssertionInfo fieldAssertionInfo =
          FieldAssertionMapper.mapFieldAssertionInfo(context, gmsAssertionInfo.getFieldAssertion());
      assertionInfo.setFieldAssertion(fieldAssertionInfo);
    }
    // SCHEMA Assertions
    if (gmsAssertionInfo.hasSchemaAssertion()) {
      SchemaAssertionInfo schemaAssertionInfo =
          mapSchemaAssertionInfo(context, gmsAssertionInfo.getSchemaAssertion());
      assertionInfo.setSchemaAssertion(schemaAssertionInfo);
    }

    if (gmsAssertionInfo.hasCustomAssertion()) {
      CustomAssertionInfo customAssertionInfo =
          mapCustomAssertionInfo(context, gmsAssertionInfo.getCustomAssertion());
      assertionInfo.setCustomAssertion(customAssertionInfo);
    }

    // Source Type
    if (gmsAssertionInfo.hasSource()) {
      assertionInfo.setSource(mapSource(gmsAssertionInfo.getSource()));
    }

    if (gmsAssertionInfo.hasExternalUrl()) {
      assertionInfo.setExternalUrl(gmsAssertionInfo.getExternalUrl().toString());
    }

    return assertionInfo;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionActions mapAssertionActions(
      final AssertionActions gmsAssertionActions) {
    final com.linkedin.datahub.graphql.generated.AssertionActions result =
        new com.linkedin.datahub.graphql.generated.AssertionActions();
    if (gmsAssertionActions.hasOnFailure()) {
      result.setOnFailure(
          gmsAssertionActions.getOnFailure().stream()
              .map(AssertionMapper::mapAssertionAction)
              .collect(Collectors.toList()));
    }
    if (gmsAssertionActions.hasOnSuccess()) {
      result.setOnSuccess(
          gmsAssertionActions.getOnSuccess().stream()
              .map(AssertionMapper::mapAssertionAction)
              .collect(Collectors.toList()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionAction mapAssertionAction(
      final AssertionAction gmsAssertionAction) {
    final com.linkedin.datahub.graphql.generated.AssertionAction result =
        new com.linkedin.datahub.graphql.generated.AssertionAction();
    result.setType(AssertionActionType.valueOf(gmsAssertionAction.getType().toString()));
    return result;
  }

  private static DatasetAssertionInfo mapDatasetAssertionInfo(
      @Nullable QueryContext context,
      final com.linkedin.assertion.DatasetAssertionInfo gmsDatasetAssertion) {
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDatasetUrn(gmsDatasetAssertion.getDataset().toString());
    datasetAssertion.setScope(DatasetAssertionScope.valueOf(gmsDatasetAssertion.getScope().name()));
    if (gmsDatasetAssertion.hasFields()) {
      datasetAssertion.setFields(
          gmsDatasetAssertion.getFields().stream()
              .map(AssertionMapper::mapDatasetSchemaField)
              .collect(Collectors.toList()));
    } else {
      datasetAssertion.setFields(Collections.emptyList());
    }
    // Agg
    if (gmsDatasetAssertion.hasAggregation()) {
      datasetAssertion.setAggregation(
          AssertionStdAggregation.valueOf(gmsDatasetAssertion.getAggregation().name()));
    }

    // Op
    datasetAssertion.setOperator(
        AssertionStdOperator.valueOf(gmsDatasetAssertion.getOperator().name()));

    // Params
    if (gmsDatasetAssertion.hasParameters()) {
      datasetAssertion.setParameters(mapParameters(gmsDatasetAssertion.getParameters()));
    }

    if (gmsDatasetAssertion.hasNativeType()) {
      datasetAssertion.setNativeType(gmsDatasetAssertion.getNativeType());
    }
    if (gmsDatasetAssertion.hasNativeParameters()) {
      datasetAssertion.setNativeParameters(
          StringMapMapper.map(context, gmsDatasetAssertion.getNativeParameters()));
    } else {
      datasetAssertion.setNativeParameters(Collections.emptyList());
    }
    if (gmsDatasetAssertion.hasLogic()) {
      datasetAssertion.setLogic(gmsDatasetAssertion.getLogic());
    }
    return datasetAssertion;
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private static SchemaFieldRef mapDatasetSchemaField(final Urn schemaFieldUrn) {
    return new SchemaFieldRef(schemaFieldUrn.toString(), schemaFieldUrn.getEntityKey().get(1));
  }

  public static List<com.linkedin.datahub.graphql.generated.AssertionExclusionWindow>
      mapAssertionExclusionWindows(@Nonnull List<AssertionExclusionWindow> gmsExclusionWindows) {
    return gmsExclusionWindows.stream()
        .map(AssertionMapper::mapAssertionExclusionWindow)
        .collect(Collectors.toList());
  }

  public static AssertionExclusionWindowArray mapGraphQLAssertionExclusionWindows(
      @Nonnull
          List<com.linkedin.datahub.graphql.generated.AssertionExclusionWindowInput>
              exclusionWindows) {
    return new AssertionExclusionWindowArray(
        exclusionWindows.stream()
            .map(AssertionMapper::mapGraphQLAssertionExclusionWindow)
            .collect(Collectors.toList()));
  }

  private static com.linkedin.datahub.graphql.generated.AssertionExclusionWindow
      mapAssertionExclusionWindow(@Nonnull AssertionExclusionWindow gmsExclusionWindow) {
    com.linkedin.datahub.graphql.generated.AssertionExclusionWindow exclusionWindow =
        new com.linkedin.datahub.graphql.generated.AssertionExclusionWindow();

    if (gmsExclusionWindow.hasType()) {
      exclusionWindow.setType(
          AssertionExclusionWindowType.valueOf(gmsExclusionWindow.getType().toString()));
    }
    exclusionWindow.setDisplayName(gmsExclusionWindow.getDisplayName(GetMode.NULL));

    if (gmsExclusionWindow.hasHoliday()) {
      com.linkedin.datahub.graphql.generated.HolidayWindow holidayWindow =
          new com.linkedin.datahub.graphql.generated.HolidayWindow();
      com.linkedin.timeseries.HolidayWindow gmsHoliday = gmsExclusionWindow.getHoliday();

      holidayWindow.setName(gmsHoliday.getName(GetMode.NULL));
      holidayWindow.setRegion(gmsHoliday.getRegion(GetMode.NULL));
      holidayWindow.setTimezone(gmsHoliday.getTimezone(GetMode.NULL));

      exclusionWindow.setHoliday(holidayWindow);
    }

    if (gmsExclusionWindow.hasFixedRange()) {
      com.linkedin.datahub.graphql.generated.AbsoluteTimeWindow fixedRange =
          new AbsoluteTimeWindow();
      fixedRange.setStartTimeMillis(gmsExclusionWindow.getFixedRange().getStartTimeMillis());
      fixedRange.setEndTimeMillis(gmsExclusionWindow.getFixedRange().getEndTimeMillis());
      exclusionWindow.setFixedRange(fixedRange);
    }

    if (gmsExclusionWindow.hasWeekly()) {
      com.linkedin.datahub.graphql.generated.WeeklyWindow weeklyWindow =
          new com.linkedin.datahub.graphql.generated.WeeklyWindow();
      WeeklyWindow gmsWeeklyWindow = gmsExclusionWindow.getWeekly();

      if (gmsWeeklyWindow.hasDaysOfWeek()) {
        weeklyWindow.setDaysOfWeek(
            gmsWeeklyWindow.getDaysOfWeek().stream()
                .map(
                    day -> com.linkedin.datahub.graphql.generated.DayOfWeek.valueOf(day.toString()))
                .collect(Collectors.toList()));
      }
      weeklyWindow.setStartTime(gmsWeeklyWindow.getStartTime(GetMode.NULL));
      weeklyWindow.setEndTime(gmsWeeklyWindow.getEndTime(GetMode.NULL));
      weeklyWindow.setTimezone(gmsWeeklyWindow.getTimezone(GetMode.NULL));

      exclusionWindow.setWeekly(weeklyWindow);
    }
    return exclusionWindow;
  }

  private static AssertionExclusionWindow mapGraphQLAssertionExclusionWindow(
      @Nonnull
          com.linkedin.datahub.graphql.generated.AssertionExclusionWindowInput exclusionWindow) {
    AssertionExclusionWindow gmsExclusionWindow = new AssertionExclusionWindow();

    if (exclusionWindow.getType() != null) {
      gmsExclusionWindow.setType(
          com.linkedin.assertion.AssertionExclusionWindowType.valueOf(
              exclusionWindow.getType().toString()));
    }
    gmsExclusionWindow.setDisplayName(exclusionWindow.getDisplayName());

    if (exclusionWindow.getHoliday() != null) {
      com.linkedin.timeseries.HolidayWindow gmsHoliday =
          new com.linkedin.timeseries.HolidayWindow();
      com.linkedin.datahub.graphql.generated.HolidayWindowInput holidayWindow =
          exclusionWindow.getHoliday();

      gmsHoliday.setName(holidayWindow.getName());
      gmsHoliday.setRegion(holidayWindow.getRegion());
      gmsHoliday.setTimezone(holidayWindow.getTimezone());

      gmsExclusionWindow.setHoliday(gmsHoliday);
    }

    if (exclusionWindow.getFixedRange() != null) {
      AbsoluteTimeWindowInput gqlFixedRange = exclusionWindow.getFixedRange();
      com.linkedin.timeseries.AbsoluteTimeWindow fixedRange =
          new com.linkedin.timeseries.AbsoluteTimeWindow();
      fixedRange.setStartTimeMillis(gqlFixedRange.getStartTimeMillis());
      fixedRange.setEndTimeMillis(gqlFixedRange.getEndTimeMillis());
      gmsExclusionWindow.setFixedRange(fixedRange);
    }

    if (exclusionWindow.getWeekly() != null) {
      com.linkedin.timeseries.WeeklyWindow gmsWeekly = new com.linkedin.timeseries.WeeklyWindow();
      com.linkedin.datahub.graphql.generated.WeeklyWindowInput weeklyWindow =
          exclusionWindow.getWeekly();

      if (weeklyWindow.getDaysOfWeek() != null) {
        gmsWeekly.setDaysOfWeek(
            new DayOfWeekArray(
                weeklyWindow.getDaysOfWeek().stream()
                    .map(day -> com.linkedin.timeseries.DayOfWeek.valueOf(day.toString()))
                    .collect(Collectors.toList())));
      }
      gmsWeekly.setStartTime(weeklyWindow.getStartTime());
      gmsWeekly.setEndTime(weeklyWindow.getEndTime());
      gmsWeekly.setTimezone(weeklyWindow.getTimezone());

      gmsExclusionWindow.setWeekly(gmsWeekly);
    }
    return gmsExclusionWindow;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionMonitorSensitivity
      mapAssertionMonitorSensitivity(
          @Nonnull com.linkedin.assertion.AssertionMonitorSensitivity gmsMonitorSensitivity) {
    return com.linkedin.datahub.graphql.generated.AssertionMonitorSensitivity.builder()
        .setLevel(gmsMonitorSensitivity.getLevel())
        .build();
  }

  private static com.linkedin.assertion.AssertionMonitorSensitivity
      mapGraphqlAssertionMonitorSensitivity(
          @Nullable
              com.linkedin.datahub.graphql.generated.AssertionMonitorSensitivityInput
                  monitorSensitivity) {
    if (monitorSensitivity == null) {
      return null;
    }
    return new com.linkedin.assertion.AssertionMonitorSensitivity()
        .setLevel(monitorSensitivity.getLevel());
  }

  protected static AssertionStdParameters mapParameters(
      final com.linkedin.assertion.AssertionStdParameters params) {
    final AssertionStdParameters result = new AssertionStdParameters();
    if (params.hasValue()) {
      result.setValue(mapParameter(params.getValue()));
    }
    if (params.hasMinValue()) {
      result.setMinValue(mapParameter(params.getMinValue()));
    }
    if (params.hasMaxValue()) {
      result.setMaxValue(mapParameter(params.getMaxValue()));
    }
    return result;
  }

  private static AssertionStdParameter mapParameter(
      final com.linkedin.assertion.AssertionStdParameter param) {
    final AssertionStdParameter result = new AssertionStdParameter();
    result.setType(AssertionStdParameterType.valueOf(param.getType().name()));
    result.setValue(param.getValue());
    return result;
  }

  protected static FixedIntervalSchedule mapFixedIntervalSchedule(
      com.linkedin.assertion.FixedIntervalSchedule gmsFixedIntervalSchedule) {
    FixedIntervalSchedule fixedIntervalSchedule = new FixedIntervalSchedule();
    fixedIntervalSchedule.setUnit(DateInterval.valueOf(gmsFixedIntervalSchedule.getUnit().name()));
    fixedIntervalSchedule.setMultiple(gmsFixedIntervalSchedule.getMultiple());
    return fixedIntervalSchedule;
  }

  private static AssertionSource mapSource(final com.linkedin.assertion.AssertionSource gmsSource) {
    AssertionSource result = new AssertionSource();
    result.setType(AssertionSourceType.valueOf(gmsSource.getType().toString()));
    if (gmsSource.hasCreated()) {
      result.setCreated(
          new AuditStamp(
              gmsSource.getCreated().getTime(), gmsSource.getCreated().getActor().toString()));
    }
    return result;
  }

  protected static com.linkedin.datahub.graphql.generated.SchemaFieldSpec mapSchemaFieldSpec(
      final com.linkedin.schema.SchemaFieldSpec gmsField) {
    final com.linkedin.datahub.graphql.generated.SchemaFieldSpec result =
        new com.linkedin.datahub.graphql.generated.SchemaFieldSpec();
    result.setPath(gmsField.getPath());
    result.setType(gmsField.getType());
    result.setNativeType(gmsField.getNativeType());
    return result;
  }

  private static SchemaAssertionInfo mapSchemaAssertionInfo(
      @Nullable final QueryContext context,
      final com.linkedin.assertion.SchemaAssertionInfo gmsSchemaAssertionInfo) {
    SchemaAssertionInfo result = new SchemaAssertionInfo();
    result.setCompatibility(
        SchemaAssertionCompatibility.valueOf(gmsSchemaAssertionInfo.getCompatibility().name()));
    result.setEntityUrn(gmsSchemaAssertionInfo.getEntity().toString());
    result.setSchema(
        SchemaMetadataMapper.INSTANCE.apply(
            context, gmsSchemaAssertionInfo.getSchema(), gmsSchemaAssertionInfo.getEntity(), 0L));
    result.setFields(
        gmsSchemaAssertionInfo.getSchema().getFields().stream()
            .map(AssertionMapper::mapSchemaField)
            .collect(Collectors.toList()));
    return result;
  }

  private static CustomAssertionInfo mapCustomAssertionInfo(
      @Nullable final QueryContext context,
      final com.linkedin.assertion.CustomAssertionInfo gmsCustomAssertionInfo) {
    CustomAssertionInfo result = new CustomAssertionInfo();
    result.setType(gmsCustomAssertionInfo.getType());
    result.setEntityUrn(gmsCustomAssertionInfo.getEntity().toString());
    if (gmsCustomAssertionInfo.hasField()) {
      result.setField(AssertionMapper.mapDatasetSchemaField(gmsCustomAssertionInfo.getField()));
    }
    if (gmsCustomAssertionInfo.hasLogic()) {
      result.setLogic(gmsCustomAssertionInfo.getLogic());
    }

    return result;
  }

  private static SchemaAssertionField mapSchemaField(final SchemaField gmsField) {
    SchemaAssertionField result = new SchemaAssertionField();
    result.setPath(gmsField.getFieldPath());
    result.setType(new SchemaFieldMapper().mapSchemaFieldDataType(gmsField.getType()));
    if (gmsField.hasNativeDataType()) {
      result.setNativeType(gmsField.getNativeDataType());
    }
    return result;
  }

  protected AssertionMapper() {}
}
