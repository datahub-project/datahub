package com.linkedin.datahub.graphql.types.ingestion;

import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.addAspect;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getEntityResponse;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getExecutionRequestInput;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getExecutionRequestInputArgs;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getExecutionRequestResult;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getExecutionRequestSource;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.getExecutionRequestStructuredReport;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.verifyExecutionRequestInput;
import static com.linkedin.datahub.graphql.types.ingestion.TestUtils.verifyExecutionRequestResult;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.entity.EntityResponse;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.Constants;
import org.testng.annotations.Test;

public class ExecutionRequestMapperTest {
  private static final String TEST_EXECUTION_REQUEST_URN = "urn:li:dataHubExecutionRequest:id-1";
  private static final String TEST_INGESTION_SOURCE_URN = "urn:li:dataHubIngestionSource:id-1";

  @Test
  public void testGetSuccess() throws Exception {
    EntityResponse entityResponse =
        getEntityResponse()
            .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
            .setUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));
    ExecutionRequestInput requestInput =
        getExecutionRequestInput(
            getExecutionRequestInputArgs("recipe", "version"),
            "task",
            "executorId",
            0L,
            getExecutionRequestSource(Urn.createFromString(TEST_INGESTION_SOURCE_URN), "type"),
            Urn.createFromString("urn:li:corpuser:datahub"));
    addAspect(entityResponse, Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, requestInput);
    StructuredExecutionReport structuredReport = getExecutionRequestStructuredReport();
    ExecutionRequestResult requestResult =
        getExecutionRequestResult(10L, "report", 0L, "success", structuredReport);
    addAspect(entityResponse, Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME, requestResult);

    ExecutionRequest executionRequest = ExecutionRequestMapper.map(null, entityResponse);

    assertEquals(executionRequest.getUrn(), TEST_EXECUTION_REQUEST_URN);
    assertEquals(executionRequest.getType(), EntityType.EXECUTION_REQUEST);
    verifyExecutionRequestInput(executionRequest, requestInput);
    verifyExecutionRequestResult(executionRequest, requestResult);
  }

  @Test
  public void testGetWithNoResult() throws Exception {
    EntityResponse entityResponse =
        getEntityResponse()
            .setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
            .setUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));
    ExecutionRequestInput requestInput =
        getExecutionRequestInput(
            getExecutionRequestInputArgs("recipe", "version"),
            "task",
            "executorId",
            0L,
            getExecutionRequestSource(Urn.createFromString(TEST_INGESTION_SOURCE_URN), "type"),
            Urn.createFromString("urn:li:corpuser:datahub"));
    addAspect(entityResponse, Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, requestInput);

    ExecutionRequest executionRequest = ExecutionRequestMapper.map(null, entityResponse);

    assertEquals(executionRequest.getUrn(), TEST_EXECUTION_REQUEST_URN);
    assertEquals(executionRequest.getType(), EntityType.EXECUTION_REQUEST);
    verifyExecutionRequestInput(executionRequest, requestInput);
    assertNull(executionRequest.getResult());
  }
}
