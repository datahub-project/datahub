package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersInput;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersType;
import com.linkedin.datahub.graphql.generated.DatasetSchemaAssertionParametersInput;
import com.linkedin.monitor.AssertionEvaluationParameters;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorUtilsTest {
  @Test
  public void createAssertionEvaluationParametersSchema() {
    AssertionEvaluationParametersInput testInput = new AssertionEvaluationParametersInput();
    testInput.setType(AssertionEvaluationParametersType.DATASET_SCHEMA);
    testInput.setDatasetSchemaParameters(
        new DatasetSchemaAssertionParametersInput(
            com.linkedin.datahub.graphql.generated.DatasetSchemaSourceType.DATAHUB_SCHEMA));
    AssertionEvaluationParameters result =
        MonitorUtils.createAssertionEvaluationParameters(testInput);
    Assert.assertEquals(
        result.getType(), com.linkedin.monitor.AssertionEvaluationParametersType.DATASET_SCHEMA);
    Assert.assertEquals(
        result.getDatasetSchemaParameters().getSourceType(),
        com.linkedin.monitor.DatasetSchemaSourceType.DATAHUB_SCHEMA);
  }
}
