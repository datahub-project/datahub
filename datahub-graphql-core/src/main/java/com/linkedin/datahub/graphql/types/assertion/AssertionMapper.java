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
    if (gmsAssertionInfo.hasParameters()) {
      assertionInfo.setParameters(StringMapMapper.map(gmsAssertionInfo.getParameters()));
    }
    if (gmsAssertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
      datasetAssertion.setScope(
          DatasetAssertionScope.valueOf(gmsAssertionInfo.getDatasetAssertion().getScope().name()));
      if (gmsAssertionInfo.getDatasetAssertion().hasColumnAssertion()) {
        DatasetColumnAssertion columnAssertion = new DatasetColumnAssertion();
        columnAssertion.setStdOperator(AssertionStdOperator.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getColumnAssertion().getStdOperator().name()));
        columnAssertion.setStdAggFunc(DatasetColumnStdAggFunc.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getColumnAssertion().getStdAggFunc().name()));
        columnAssertion.setNativeOperator(
            gmsAssertionInfo.getDatasetAssertion().getColumnAssertion().getNativeOperator());
        columnAssertion.setNativeAggFunc(
            gmsAssertionInfo.getDatasetAssertion().getColumnAssertion().getNativeAggFunc());
        datasetAssertion.setColumnAssertion(columnAssertion);
      }
      if (gmsAssertionInfo.getDatasetAssertion().hasRowsAssertion()) {
        DatasetRowsAssertion rowsAssertion = new DatasetRowsAssertion();
        rowsAssertion.setStdOperator(AssertionStdOperator.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getRowsAssertion().getStdOperator().name()));
        rowsAssertion.setStdAggFunc(DatasetRowsStdAggFunc.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getRowsAssertion().getStdAggFunc().name()));
        rowsAssertion.setNativeOperator(gmsAssertionInfo.getDatasetAssertion().getRowsAssertion().getNativeOperator());
        rowsAssertion.setNativeAggFunc(gmsAssertionInfo.getDatasetAssertion().getRowsAssertion().getNativeAggFunc());
        datasetAssertion.setRowsAssertion(rowsAssertion);
      }
      if (gmsAssertionInfo.getDatasetAssertion().hasSchemaAssertion()) {
        DatasetSchemaAssertion schemaAssertion = new DatasetSchemaAssertion();
        schemaAssertion.setStdOperator(AssertionStdOperator.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getSchemaAssertion().getStdOperator().name()));
        schemaAssertion.setStdAggFunc(DatasetSchemaStdAggFunc.valueOf(
            gmsAssertionInfo.getDatasetAssertion().getSchemaAssertion().getStdAggFunc().name()));
        schemaAssertion.setNativeOperator(
            gmsAssertionInfo.getDatasetAssertion().getSchemaAssertion().getNativeOperator());
        schemaAssertion.setNativeAggFunc(
            gmsAssertionInfo.getDatasetAssertion().getSchemaAssertion().getNativeAggFunc());
        datasetAssertion.setSchemaAssertion(schemaAssertion);
      }
      if (gmsAssertionInfo.getDatasetAssertion().hasDatasets()) {
        datasetAssertion.setDatasets(gmsAssertionInfo.getDatasetAssertion()
            .getDatasets()
            .stream()
            .map(urn -> urn.toString())
            .collect(Collectors.toList()));
      }
      if (gmsAssertionInfo.getDatasetAssertion().hasFields()) {
        datasetAssertion.setFields(gmsAssertionInfo.getDatasetAssertion()
            .getFields()
            .stream()
            .map(urn -> urn.toString())
            .collect(Collectors.toList()));
      }
      ;
      assertionInfo.setDatasetAssertion(datasetAssertion);
    }
    return assertionInfo;
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