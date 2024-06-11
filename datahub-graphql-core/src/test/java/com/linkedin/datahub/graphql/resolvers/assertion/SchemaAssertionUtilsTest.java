package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.datahub.graphql.generated.CreateSchemaAssertionInput;
import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.SchemaAssertionCompatibility;
import com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaAssertionUtilsTest {

  @Test
  public void testMapEvaluationParametersNullInput() {
    AssertionEvaluationParameters parameters = SchemaAssertionUtils.mapEvaluationParameters(null);

    Assert.assertNotNull(parameters);
    Assert.assertEquals(parameters.getType(), AssertionEvaluationParametersType.DATASET_SCHEMA);
    Assert.assertNotNull(parameters.getDatasetSchemaParameters());
    Assert.assertEquals(
        parameters.getDatasetSchemaParameters().getSourceType(),
        DatasetSchemaSourceType.DATAHUB_SCHEMA);
  }

  @Test
  public void testMapEvaluationParametersNonNullInput() {
    DatasetSchemaAssertionParametersInput input = new DatasetSchemaAssertionParametersInput();
    input.setSourceType(
        com.linkedin.datahub.graphql.generated.DatasetSchemaSourceType.DATAHUB_SCHEMA);

    AssertionEvaluationParameters parameters = SchemaAssertionUtils.mapEvaluationParameters(input);

    Assert.assertNotNull(parameters);
    Assert.assertEquals(parameters.getType(), AssertionEvaluationParametersType.DATASET_SCHEMA);
    Assert.assertNotNull(parameters.getDatasetSchemaParameters());
    Assert.assertEquals(
        parameters.getDatasetSchemaParameters().getSourceType(),
        DatasetSchemaSourceType.DATAHUB_SCHEMA);
  }

  @Test
  public void testCreateSchemaMetadata() {
    SchemaAssertionFieldInput fieldInput = new SchemaAssertionFieldInput();
    fieldInput.setPath("testPath");
    fieldInput.setType(com.linkedin.datahub.graphql.generated.SchemaFieldDataType.STRING);
    SchemaAssertionUtils.createSchemaMetadata(Collections.singletonList(fieldInput));
    // Add more assertions as needed
  }

  @Test
  public void testCreateSchemaField() {
    SchemaAssertionFieldInput fieldInput = new SchemaAssertionFieldInput();
    fieldInput.setPath("testPath");
    fieldInput.setType(com.linkedin.datahub.graphql.generated.SchemaFieldDataType.STRING);

    com.linkedin.schema.SchemaField schemaField =
        SchemaAssertionUtils.createSchemaField(fieldInput);

    Assert.assertNotNull(schemaField);
    Assert.assertEquals(schemaField.getFieldPath(), "testPath");
    // Add more assertions as needed
  }

  @Test
  public void testCreateDataSchemaAssertionInfo() {
    SchemaAssertionFieldInput fieldInput = new SchemaAssertionFieldInput();
    fieldInput.setPath("testPath");
    fieldInput.setType(com.linkedin.datahub.graphql.generated.SchemaFieldDataType.STRING);

    CreateSchemaAssertionInput input = new CreateSchemaAssertionInput();
    input.setFields(Collections.singletonList(fieldInput));
    input.setCompatibility(SchemaAssertionCompatibility.EXACT_MATCH);
    input.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    SchemaAssertionInfo info = SchemaAssertionUtils.createDataSchemaAssertionInfo(input);

    Assert.assertNotNull(info);
    Assert.assertEquals(
        info.getCompatibility(), com.linkedin.assertion.SchemaAssertionCompatibility.EXACT_MATCH);
    Assert.assertEquals(
        info.getEntity().toString(), "urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    Assert.assertEquals(info.getSchema().getFields().size(), 1);
    Assert.assertEquals(info.getSchema().getFields().get(0).getFieldPath(), "testPath");
    Assert.assertEquals(
        info.getSchema().getFields().get(0).getType().getType(),
        SchemaFieldDataType.Type.create(new StringType()));
  }
}
