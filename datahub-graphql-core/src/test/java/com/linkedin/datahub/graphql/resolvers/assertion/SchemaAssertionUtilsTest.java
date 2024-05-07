package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.DatasetSchemaSourceType;
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
}
