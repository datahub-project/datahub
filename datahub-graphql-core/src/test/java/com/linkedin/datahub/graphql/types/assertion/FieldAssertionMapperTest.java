package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FieldTransform;
import com.linkedin.assertion.FieldTransformType;
import com.linkedin.assertion.FieldValuesAssertion;
import com.linkedin.assertion.FieldValuesFailThreshold;
import com.linkedin.assertion.FieldValuesFailThresholdType;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.schema.SchemaFieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldAssertionMapperTest {
  @Test
  public void testMapFieldValuesAssertionInfo() throws Exception {
    FieldAssertionInfo fieldAssertionInfo =
        new FieldAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setFilter(
                new DatasetFilter().setType(DatasetFilterType.SQL).setSql("WHERE value > 5;"))
            .setType(FieldAssertionType.FIELD_VALUES)
            .setFieldValuesAssertion(
                new FieldValuesAssertion()
                    .setExcludeNulls(true)
                    .setFailThreshold(
                        new FieldValuesFailThreshold()
                            .setType(FieldValuesFailThresholdType.PERCENTAGE)
                            .setValue(5L))
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setType("STRING")
                            .setNativeType("VARCHAR"))
                    .setOperator(AssertionStdOperator.IS_TRUE)
                    .setTransform(new FieldTransform().setType(FieldTransformType.LENGTH)));

    com.linkedin.datahub.graphql.generated.FieldAssertionInfo result =
        FieldAssertionMapper.mapFieldAssertionInfo(null, fieldAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.FieldAssertionType.FIELD_VALUES);
    Assert.assertEquals(
        result.getFilter().getType(), com.linkedin.datahub.graphql.generated.DatasetFilterType.SQL);
    Assert.assertEquals(result.getFilter().getSql(), "WHERE value > 5;");
    Assert.assertEquals(result.getFieldValuesAssertion().getField().getPath(), "path");
    Assert.assertEquals(result.getFieldValuesAssertion().getField().getType(), "STRING");
    Assert.assertEquals(result.getFieldValuesAssertion().getField().getNativeType(), "VARCHAR");
    Assert.assertEquals(
        result.getFieldValuesAssertion().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.IS_TRUE);
    Assert.assertEquals(
        result.getFieldValuesAssertion().getTransform().getType(),
        com.linkedin.datahub.graphql.generated.FieldTransformType.LENGTH);
    Assert.assertEquals(result.getFieldValuesAssertion().getExcludeNulls(), true);
    Assert.assertEquals(
        result.getFieldValuesAssertion().getFailThreshold().getType(),
        com.linkedin.datahub.graphql.generated.FieldValuesFailThresholdType.PERCENTAGE);
    Assert.assertEquals(
        result.getFieldValuesAssertion().getFailThreshold().getValue(), Long.valueOf(5L));
  }

  @Test
  public void testMapFieldMetricAssertionInfo() throws Exception {
    FieldAssertionInfo fieldAssertionInfo =
        new FieldAssertionInfo()
            .setEntity(new Urn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)"))
            .setType(FieldAssertionType.FIELD_METRIC)
            .setFieldMetricAssertion(
                new FieldMetricAssertion()
                    .setField(
                        new SchemaFieldSpec()
                            .setPath("path")
                            .setType("STRING")
                            .setNativeType("VARCHAR"))
                    .setOperator(AssertionStdOperator.IS_TRUE)
                    .setMetric(FieldMetricType.MEDIAN));

    com.linkedin.datahub.graphql.generated.FieldAssertionInfo result =
        FieldAssertionMapper.mapFieldAssertionInfo(null, fieldAssertionInfo);
    Assert.assertEquals(result.getEntityUrn(), "urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz)");
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.FieldAssertionType.FIELD_METRIC);
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getPath(), "path");
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getType(), "STRING");
    Assert.assertEquals(result.getFieldMetricAssertion().getField().getNativeType(), "VARCHAR");
    Assert.assertEquals(
        result.getFieldMetricAssertion().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.IS_TRUE);
    Assert.assertEquals(
        result.getFieldMetricAssertion().getMetric(),
        com.linkedin.datahub.graphql.generated.FieldMetricType.MEDIAN);
  }
}
