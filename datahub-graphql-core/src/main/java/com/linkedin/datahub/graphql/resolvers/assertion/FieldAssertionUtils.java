package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FieldTransformType;
import com.linkedin.assertion.FieldValuesFailThresholdType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateFieldAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldMetricAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldTransformInput;
import com.linkedin.datahub.graphql.generated.FieldValuesAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldValuesFailThresholdInput;
import javax.annotation.Nonnull;

public class FieldAssertionUtils {

  @Nonnull
  public static com.linkedin.assertion.FieldValuesFailThreshold createFieldValuesFailThreshold(
      @Nonnull final FieldValuesFailThresholdInput input) {
    final com.linkedin.assertion.FieldValuesFailThreshold result =
        new com.linkedin.assertion.FieldValuesFailThreshold();
    result.setType(FieldValuesFailThresholdType.valueOf(input.getType().toString()));
    result.setValue(input.getValue());
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FieldTransform createFieldTransform(
      @Nonnull final FieldTransformInput input) {
    final com.linkedin.assertion.FieldTransform result =
        new com.linkedin.assertion.FieldTransform();
    result.setType(FieldTransformType.valueOf(input.getType().toString()));
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FieldValuesAssertion createFieldValuesAssertion(
      @Nonnull final FieldValuesAssertionInput input) {
    final com.linkedin.assertion.FieldValuesAssertion result =
        new com.linkedin.assertion.FieldValuesAssertion();
    result.setField(AssertionUtils.createSchemaFieldSpec(input.getField()));
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    result.setFailThreshold(createFieldValuesFailThreshold(input.getFailThreshold()));
    result.setExcludeNulls(input.getExcludeNulls());
    if (input.getTransform() != null) {
      result.setTransform(createFieldTransform(input.getTransform()));
    }
    if (input.getParameters() != null) {
      result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FieldMetricAssertion createFieldMetricAssertion(
      @Nonnull final FieldMetricAssertionInput input) {
    final com.linkedin.assertion.FieldMetricAssertion result =
        new com.linkedin.assertion.FieldMetricAssertion();
    result.setField(AssertionUtils.createSchemaFieldSpec(input.getField()));
    result.setMetric(FieldMetricType.valueOf(input.getMetric().toString()));
    result.setOperator(AssertionUtils.createAssertionStdOperator(input.getOperator()));
    if (input.getParameters() != null) {
      result.setParameters(AssertionUtils.createDatasetAssertionParameters(input.getParameters()));
    }
    return result;
  }

  @Nonnull
  public static com.linkedin.assertion.FieldAssertionInfo createFieldAssertionInfo(
      @Nonnull final CreateFieldAssertionInput input) {
    final com.linkedin.assertion.FieldAssertionInfo result =
        new com.linkedin.assertion.FieldAssertionInfo();
    final Urn asserteeUrn = UrnUtils.getUrn(input.getEntityUrn());

    result.setEntity(asserteeUrn);
    result.setType(FieldAssertionType.valueOf(input.getType().toString()));
    if (input.getFilter() != null) {
      result.setFilter(AssertionUtils.createAssertionFilter(input.getFilter()));
    }

    switch (input.getType()) {
      case FIELD_VALUES:
        if (input.getFieldValuesAssertion() != null) {
          result.setFieldValuesAssertion(
              createFieldValuesAssertion(input.getFieldValuesAssertion()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. FieldValuesAssertion must be specified if FieldAssertionType is FIELD_VALUES.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      case FIELD_METRIC:
        if (input.getFieldMetricAssertion() != null) {
          result.setFieldMetricAssertion(
              createFieldMetricAssertion(input.getFieldMetricAssertion()));
        } else {
          throw new DataHubGraphQLException(
              "Invalid input. FieldMetricAssertion must be specified if FieldAssertionType is FIELD_METRIC.",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        break;
      default:
        throw new RuntimeException(
            String.format("Unsupported FieldAssertionType %s provided", input.getType()));
    }

    return result;
  }

  private FieldAssertionUtils() {}
}
