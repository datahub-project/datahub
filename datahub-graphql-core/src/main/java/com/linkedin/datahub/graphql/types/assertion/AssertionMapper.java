package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStdAggregation;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameter;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParameters;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DatasetAssertionInfo;
import com.linkedin.datahub.graphql.generated.DatasetAssertionScope;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SchemaFieldRef;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
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

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.Status mapStatus(Status status) {
    final com.linkedin.datahub.graphql.generated.Status result =
        new com.linkedin.datahub.graphql.generated.Status();
    result.setRemoved(status.isRemoved());
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionInfo mapAssertionInfo(
      @Nullable QueryContext context, final AssertionInfo gmsAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.AssertionInfo assertionInfo =
        new com.linkedin.datahub.graphql.generated.AssertionInfo();
    assertionInfo.setType(AssertionType.valueOf(gmsAssertionInfo.getType().name()));
    if (gmsAssertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion =
          mapDatasetAssertionInfo(context, gmsAssertionInfo.getDatasetAssertion());
      assertionInfo.setDatasetAssertion(datasetAssertion);
    }
    assertionInfo.setDescription(gmsAssertionInfo.getDescription());
    return assertionInfo;
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

  private static AssertionStdParameters mapParameters(
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

  private AssertionMapper() {}
}
