package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DatasetAssertionInfo;
import com.linkedin.datahub.graphql.generated.DatasetAssertionScope;
import com.linkedin.datahub.graphql.generated.DatasetColumnAssertion;
import com.linkedin.datahub.graphql.generated.DatasetColumnStdAggFunc;
import com.linkedin.datahub.graphql.generated.DatasetRowsAssertion;
import com.linkedin.datahub.graphql.generated.DatasetRowsStdAggFunc;
import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertion;
import com.linkedin.datahub.graphql.generated.DatasetSchemaStdAggFunc;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.stream.Collectors;


public class AssertionMapper {

  public static Assertion map(final EntityResponse entityResponse) {
    final Assertion result = new Assertion();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ASSERTION);

    final EnvelopedAspect envelopedAssertionInfo = aspects.get(Constants.ASSERTION_INFO_ASPECT_NAME);
    if (envelopedAssertionInfo != null) {
      result.setInfo(mapAssertionInfo(new AssertionInfo(envelopedAssertionInfo.getValue().data())));
    }
    final EnvelopedAspect envelopedPlatformInstance = aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      result.setPlatform(mapPlatform(new DataPlatformInstance(envelopedPlatformInstance.getValue().data())));
    } else {
      // Containers must have DPI to be rendered.
      return null;
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.AssertionInfo mapAssertionInfo(
      final AssertionInfo gmsAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.AssertionInfo assertionInfo =
        new com.linkedin.datahub.graphql.generated.AssertionInfo();
    assertionInfo.setType(AssertionType.valueOf(gmsAssertionInfo.getType().name()));
    if (gmsAssertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion = mapDatasetAssertionInfo(gmsAssertionInfo.getDatasetAssertion());
      if (gmsAssertionInfo.hasParameters()) {
        datasetAssertion.setParameters(StringMapMapper.map(gmsAssertionInfo.getParameters()));
      }
      if (gmsAssertionInfo.hasAssertionLogic()) {
        datasetAssertion.setLogic(gmsAssertionInfo.getAssertionLogic());
      }
      assertionInfo.setDatasetAssertion(datasetAssertion);
    }
    return assertionInfo;
  }

  private static DatasetAssertionInfo mapDatasetAssertionInfo(
      final com.linkedin.assertion.DatasetAssertionInfo gmsDatasetAssertion) {
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setScope(
        DatasetAssertionScope.valueOf(gmsDatasetAssertion.getScope().name()));
    if (gmsDatasetAssertion.hasColumnAssertion()) {
      DatasetColumnAssertion columnAssertion = new DatasetColumnAssertion();
      columnAssertion.setStdOperator(AssertionStdOperator.valueOf(
          gmsDatasetAssertion.getColumnAssertion().getStdOperator().name()));
      columnAssertion.setStdAggFunc(DatasetColumnStdAggFunc.valueOf(
          gmsDatasetAssertion.getColumnAssertion().getStdAggFunc().name()));
      columnAssertion.setNativeType(
          gmsDatasetAssertion.getColumnAssertion().getNativeType());
      datasetAssertion.setColumnAssertion(columnAssertion);
    }
    if (gmsDatasetAssertion.hasRowsAssertion()) {
      DatasetRowsAssertion rowsAssertion = new DatasetRowsAssertion();
      rowsAssertion.setStdOperator(AssertionStdOperator.valueOf(
          gmsDatasetAssertion.getRowsAssertion().getStdOperator().name()));
      rowsAssertion.setStdAggFunc(DatasetRowsStdAggFunc.valueOf(
          gmsDatasetAssertion.getRowsAssertion().getStdAggFunc().name()));
      rowsAssertion.setNativeType(gmsDatasetAssertion.getRowsAssertion().getNativeType());
      datasetAssertion.setRowsAssertion(rowsAssertion);
    }
    if (gmsDatasetAssertion.hasSchemaAssertion()) {
      DatasetSchemaAssertion schemaAssertion = new DatasetSchemaAssertion();
      schemaAssertion.setStdOperator(AssertionStdOperator.valueOf(
          gmsDatasetAssertion.getSchemaAssertion().getStdOperator().name()));
      schemaAssertion.setStdAggFunc(DatasetSchemaStdAggFunc.valueOf(
          gmsDatasetAssertion.getSchemaAssertion().getStdAggFunc().name()));
      schemaAssertion.setNativeType(
          gmsDatasetAssertion.getSchemaAssertion().getNativeType());
      datasetAssertion.setSchemaAssertion(schemaAssertion);
    }
    if (gmsDatasetAssertion.hasFields()) {
      datasetAssertion.setFieldUrns(gmsDatasetAssertion
          .getFields()
          .stream()
          .map(Urn::toString)
          .collect(Collectors.toList()));
      datasetAssertion.setFieldPaths(gmsDatasetAssertion
          .getFields()
          .stream()
          .map(urn -> urn.getEntityKey().get(1))
          .collect(Collectors.toList()));
    }
    return datasetAssertion;
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private AssertionMapper() {
  }
}