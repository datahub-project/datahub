package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.CreateFieldAssertionInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.generated.FieldAssertionType;
import com.linkedin.datahub.graphql.generated.FieldMetricAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldMetricType;
import com.linkedin.datahub.graphql.generated.FieldTransformInput;
import com.linkedin.datahub.graphql.generated.FieldTransformType;
import com.linkedin.datahub.graphql.generated.FieldValuesAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldValuesFailThresholdInput;
import com.linkedin.datahub.graphql.generated.FieldValuesFailThresholdType;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldAssertionUtilsTest {
  @Test
  public void testCreateFieldValuesFailThreshold() {
    FieldValuesFailThresholdInput input = new FieldValuesFailThresholdInput();
    input.setType(FieldValuesFailThresholdType.COUNT);
    input.setValue(1L);

    final com.linkedin.assertion.FieldValuesFailThreshold result =
        FieldAssertionUtils.createFieldValuesFailThreshold(input);
    Assert.assertEquals(
        result.getType(), com.linkedin.assertion.FieldValuesFailThresholdType.COUNT);
    Assert.assertEquals(result.getValue(), Long.valueOf(1L));
  }

  @Test
  public void testCreateFieldTransform() {
    FieldTransformInput input = new FieldTransformInput();
    input.setType(FieldTransformType.LENGTH);

    final com.linkedin.assertion.FieldTransform result =
        FieldAssertionUtils.createFieldTransform(input);
    Assert.assertEquals(result.getType(), com.linkedin.assertion.FieldTransformType.LENGTH);
  }

  @Test
  public void testCreateFieldValuesAssertion() {
    FieldValuesAssertionInput input = new FieldValuesAssertionInput();
    AssertionStdParametersInput parameters = new AssertionStdParametersInput();
    AssertionStdParameterInput parameter = new AssertionStdParameterInput();
    SchemaFieldSpecInput field = new SchemaFieldSpecInput();
    FieldValuesFailThresholdInput failThreshold = new FieldValuesFailThresholdInput();

    field.setPath("path");
    field.setNativeType("VARCHAR");
    field.setType("VARCHAR");
    failThreshold.setType(FieldValuesFailThresholdType.PERCENTAGE);
    failThreshold.setValue(1L);
    parameter.setType(AssertionStdParameterType.NUMBER);
    parameter.setValue("1");
    parameters.setValue(parameter);
    input.setField(field);
    input.setOperator(AssertionStdOperator.GREATER_THAN);
    input.setFailThreshold(failThreshold);
    input.setExcludeNulls(true);
    input.setParameters(parameters);

    final com.linkedin.assertion.FieldValuesAssertion result =
        FieldAssertionUtils.createFieldValuesAssertion(input);
    Assert.assertEquals(result.getField().getPath(), "path");
    Assert.assertEquals(result.getField().getNativeType(), "VARCHAR");
    Assert.assertEquals(result.getField().getType(), "VARCHAR");
    Assert.assertEquals(
        result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
    Assert.assertEquals(
        result.getFailThreshold().getType(),
        com.linkedin.assertion.FieldValuesFailThresholdType.PERCENTAGE);
    Assert.assertEquals(result.getFailThreshold().getValue(), Long.valueOf(1L));
    Assert.assertTrue(result.isExcludeNulls());
    Assert.assertEquals(
        result.getParameters().getValue().getType(),
        com.linkedin.assertion.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
  }

  @Test
  public void testCreateFieldMetricAssertion() {
    FieldMetricAssertionInput input = new FieldMetricAssertionInput();
    AssertionStdParametersInput parameters = new AssertionStdParametersInput();
    AssertionStdParameterInput parameter = new AssertionStdParameterInput();
    SchemaFieldSpecInput field = new SchemaFieldSpecInput();

    field.setPath("path");
    field.setNativeType("VARCHAR");
    field.setType("VARCHAR");
    parameter.setType(AssertionStdParameterType.NUMBER);
    parameter.setValue("1");
    parameters.setValue(parameter);
    input.setField(field);
    input.setOperator(AssertionStdOperator.GREATER_THAN);
    input.setParameters(parameters);
    input.setMetric(FieldMetricType.MEAN);

    final com.linkedin.assertion.FieldMetricAssertion result =
        FieldAssertionUtils.createFieldMetricAssertion(input);
    Assert.assertEquals(result.getField().getPath(), "path");
    Assert.assertEquals(result.getField().getNativeType(), "VARCHAR");
    Assert.assertEquals(result.getField().getType(), "VARCHAR");
    Assert.assertEquals(
        result.getOperator(), com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
    Assert.assertEquals(result.getMetric(), com.linkedin.assertion.FieldMetricType.MEAN);
    Assert.assertEquals(
        result.getParameters().getValue().getType(),
        com.linkedin.assertion.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(result.getParameters().getValue().getValue(), "1");
  }

  @Test
  public void testCreateFieldAssertionInfo() {
    CreateFieldAssertionInput input = new CreateFieldAssertionInput();
    FieldMetricAssertionInput fieldMetric = new FieldMetricAssertionInput();
    AssertionStdParametersInput parameters = new AssertionStdParametersInput();
    AssertionStdParameterInput parameter = new AssertionStdParameterInput();
    SchemaFieldSpecInput field = new SchemaFieldSpecInput();
    DatasetFilterInput filter = new DatasetFilterInput();

    field.setPath("path");
    field.setNativeType("VARCHAR");
    field.setType("VARCHAR");
    parameter.setType(AssertionStdParameterType.NUMBER);
    parameter.setValue("1");
    parameters.setValue(parameter);
    fieldMetric.setField(field);
    fieldMetric.setOperator(AssertionStdOperator.GREATER_THAN);
    fieldMetric.setParameters(parameters);
    fieldMetric.setMetric(FieldMetricType.MEAN);
    filter.setSql("WHERE value = 1;");
    filter.setType(DatasetFilterType.SQL);
    input.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset1,PROD)");
    input.setType(FieldAssertionType.FIELD_METRIC);
    input.setFieldMetricAssertion(fieldMetric);
    input.setFilter(filter);

    final com.linkedin.assertion.FieldAssertionInfo result =
        FieldAssertionUtils.createFieldAssertionInfo(input);
    Assert.assertEquals(
        result.getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,dataset1,PROD)");
    Assert.assertEquals(result.getType(), com.linkedin.assertion.FieldAssertionType.FIELD_METRIC);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value = 1;");
    Assert.assertEquals(result.getFilter().getType(), com.linkedin.dataset.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getPath(), "path");
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getNativeType(), "VARCHAR");
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getType(), "VARCHAR");
    Assert.assertEquals(
        result.getFieldMetricAssertion().getOperator(),
        com.linkedin.assertion.AssertionStdOperator.GREATER_THAN);
    Assert.assertEquals(
        result.getFieldMetricAssertion().getMetric(), com.linkedin.assertion.FieldMetricType.MEAN);
    Assert.assertEquals(
        result.getFieldMetricAssertion().getParameters().getValue().getType(),
        com.linkedin.assertion.AssertionStdParameterType.NUMBER);
    Assert.assertEquals(
        result.getFieldMetricAssertion().getParameters().getValue().getValue(), "1");
  }
}
