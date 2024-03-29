package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.FieldAssertionType;
import com.linkedin.datahub.graphql.generated.FieldMetricType;
import com.linkedin.datahub.graphql.generated.FieldTransformType;
import com.linkedin.datahub.graphql.generated.FieldValuesFailThresholdType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetFilterMapper;
import javax.annotation.Nullable;

public class FieldAssertionMapper extends AssertionMapper {

  public static com.linkedin.datahub.graphql.generated.FieldAssertionInfo mapFieldAssertionInfo(
      @Nullable final QueryContext context, final FieldAssertionInfo gmsFieldAssertionInfo) {
    final com.linkedin.datahub.graphql.generated.FieldAssertionInfo result =
        new com.linkedin.datahub.graphql.generated.FieldAssertionInfo();
    result.setEntityUrn(gmsFieldAssertionInfo.getEntity().toString());
    result.setType(FieldAssertionType.valueOf(gmsFieldAssertionInfo.getType().name()));
    if (gmsFieldAssertionInfo.hasFilter()) {
      result.setFilter(DatasetFilterMapper.map(context, gmsFieldAssertionInfo.getFilter()));
    }
    if (gmsFieldAssertionInfo.hasFieldValuesAssertion()) {
      result.setFieldValuesAssertion(
          mapFieldValuesAssertion(gmsFieldAssertionInfo.getFieldValuesAssertion()));
    }
    if (gmsFieldAssertionInfo.hasFieldMetricAssertion()) {
      result.setFieldMetricAssertion(
          mapFieldMetricAssertion(gmsFieldAssertionInfo.getFieldMetricAssertion()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.FieldValuesAssertion
      mapFieldValuesAssertion(
          final com.linkedin.assertion.FieldValuesAssertion gmsFieldValuesAssertion) {
    final com.linkedin.datahub.graphql.generated.FieldValuesAssertion result =
        new com.linkedin.datahub.graphql.generated.FieldValuesAssertion();
    result.setField(mapSchemaFieldSpec(gmsFieldValuesAssertion.getField()));
    result.setOperator(AssertionStdOperator.valueOf(gmsFieldValuesAssertion.getOperator().name()));
    result.setFailThreshold(
        mapFieldValuesFailThreshold(gmsFieldValuesAssertion.getFailThreshold()));
    result.setExcludeNulls(gmsFieldValuesAssertion.isExcludeNulls());

    if (gmsFieldValuesAssertion.hasTransform()) {
      result.setTransform(mapFieldTransform(gmsFieldValuesAssertion.getTransform()));
    }

    if (gmsFieldValuesAssertion.hasParameters()) {
      result.setParameters(mapParameters(gmsFieldValuesAssertion.getParameters()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.FieldMetricAssertion
      mapFieldMetricAssertion(
          final com.linkedin.assertion.FieldMetricAssertion gmsFieldMetricAssertion) {
    final com.linkedin.datahub.graphql.generated.FieldMetricAssertion result =
        new com.linkedin.datahub.graphql.generated.FieldMetricAssertion();
    result.setField(mapSchemaFieldSpec(gmsFieldMetricAssertion.getField()));
    result.setMetric(FieldMetricType.valueOf(gmsFieldMetricAssertion.getMetric().name()));
    result.setOperator(AssertionStdOperator.valueOf(gmsFieldMetricAssertion.getOperator().name()));

    if (gmsFieldMetricAssertion.hasParameters()) {
      result.setParameters(mapParameters(gmsFieldMetricAssertion.getParameters()));
    }

    return result;
  }

  private static com.linkedin.datahub.graphql.generated.FieldTransform mapFieldTransform(
      final com.linkedin.assertion.FieldTransform gmsFieldTransform) {
    final com.linkedin.datahub.graphql.generated.FieldTransform result =
        new com.linkedin.datahub.graphql.generated.FieldTransform();
    result.setType(FieldTransformType.valueOf(gmsFieldTransform.getType().name()));
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.FieldValuesFailThreshold
      mapFieldValuesFailThreshold(
          final com.linkedin.assertion.FieldValuesFailThreshold gmsFieldValuesFailThreshold) {
    final com.linkedin.datahub.graphql.generated.FieldValuesFailThreshold result =
        new com.linkedin.datahub.graphql.generated.FieldValuesFailThreshold();
    result.setType(
        FieldValuesFailThresholdType.valueOf(gmsFieldValuesFailThreshold.getType().name()));
    result.setValue(gmsFieldValuesFailThreshold.getValue());
    return result;
  }

  private FieldAssertionMapper() {}
}
