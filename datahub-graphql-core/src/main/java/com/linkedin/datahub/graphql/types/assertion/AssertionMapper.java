package com.linkedin.datahub.graphql.types.assertion;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;

import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
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
import com.linkedin.datahub.graphql.generated.VolumeAssertionInfo;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.schema.SchemaField;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
      result.setInfo(
          mapAssertionInfo(context, new AssertionInfo(envelopedAssertionInfo.getValue().data())));
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

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.Status mapStatus(Status status) {
    final com.linkedin.datahub.graphql.generated.Status result =
        new com.linkedin.datahub.graphql.generated.Status();
    result.setRemoved(status.isRemoved());
    return result;
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
