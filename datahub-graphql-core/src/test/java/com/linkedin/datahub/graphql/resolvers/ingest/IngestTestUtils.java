package com.linkedin.datahub.graphql.resolvers.ingest;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.Secret;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.secret.DataHubSecretValue;
import org.mockito.Mockito;

import static org.testng.Assert.*;


public class IngestTestUtils {

  public static final Urn TEST_INGESTION_SOURCE_URN = Urn.createFromTuple(Constants.INGESTION_SOURCE_ENTITY_NAME, "test");
  public static final Urn TEST_SECRET_URN = Urn.createFromTuple(Constants.SECRETS_ENTITY_NAME, "TEST_SECRET");
  public static final Urn TEST_EXECUTION_REQUEST_URN = Urn.createFromTuple(Constants.EXECUTION_REQUEST_ENTITY_NAME, "1234");


  public static QueryContext getMockAllowContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    return mockContext;
  }

  public static QueryContext getMockDenyContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    return mockContext;
  }

  public static DataHubIngestionSourceInfo getTestIngestionSourceInfo() {
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setName("My Test Source");
    info.setType("mysql");
    info.setSchedule(new DataHubIngestionSourceSchedule().setTimezone("UTC").setInterval("* * * * *"));
    info.setConfig(new DataHubIngestionSourceConfig().setVersion("0.8.18").setRecipe("{}").setExecutorId("executor id"));
    return info;
  }

  public static DataHubSecretValue getTestSecretValue() {
    DataHubSecretValue value = new DataHubSecretValue();
    value.setValue("encryptedvalue");
    value.setName(TEST_SECRET_URN.getId());
    value.setDescription("none");
    return value;
  }

  public static ExecutionRequestInput getTestExecutionRequestInput() {
    ExecutionRequestInput input = new ExecutionRequestInput();
    input.setArgs(new StringMap(
        ImmutableMap.of(
            "recipe", "my-custom-recipe",
            "version", "0.8.18")
    ));
    input.setTask("RUN_INGEST");
    input.setExecutorId("default");
    input.setRequestedAt(0L);
    input.setSource(new ExecutionRequestSource().setIngestionSource(TEST_INGESTION_SOURCE_URN).setType("SCHEDULED_INGESTION"));
    return input;
  }

  public static ExecutionRequestResult getTestExecutionRequestResult() {
    final ExecutionRequestResult result = new ExecutionRequestResult();
    result.setDurationMs(10L);
    result.setReport("Test report");
    result.setStartTimeMs(0L);
    result.setStatus("SUCCEEDED");
    return result;
  }

  public static void verifyTestIngestionSourceGraphQL(IngestionSource ingestionSource, DataHubIngestionSourceInfo info) {
    assertEquals(ingestionSource.getUrn(), TEST_INGESTION_SOURCE_URN.toString());
    assertEquals(ingestionSource.getName(), info.getName());
    assertEquals(ingestionSource.getType(), info.getType());
    assertEquals(ingestionSource.getConfig().getRecipe(), info.getConfig().getRecipe());
    assertEquals(ingestionSource.getConfig().getExecutorId(), info.getConfig().getExecutorId());
    assertEquals(ingestionSource.getConfig().getVersion(), info.getConfig().getVersion());
    assertEquals(ingestionSource.getSchedule().getInterval(), info.getSchedule().getInterval());
    assertEquals(ingestionSource.getSchedule().getTimezone(), info.getSchedule().getTimezone());
  }

  public static void verifyTestSecretGraphQL(Secret secret, DataHubSecretValue value) {
    assertEquals(secret.getUrn(), TEST_SECRET_URN.toString());
    assertEquals(secret.getName(), value.getName());
    assertEquals(secret.getDescription(), value.getDescription());
  }

  public static void verifyTestExecutionRequest(
      ExecutionRequest executionRequest,
      ExecutionRequestInput input,
      ExecutionRequestResult result) {
    assertEquals(executionRequest.getUrn(), TEST_EXECUTION_REQUEST_URN.toString());
    assertEquals(executionRequest.getInput().getTask(), input.getTask());
    assertEquals(executionRequest.getInput().getSource().getType(), input.getSource().getType());
    assertEquals((long) executionRequest.getInput().getRequestedAt(), 0L);
    for (StringMapEntry entry : executionRequest.getInput().getArguments()) {
      assertTrue(input.getArgs().containsKey(entry.getKey()));
      assertEquals(entry.getValue(), input.getArgs().get(entry.getKey()));
    }
    assertEquals(executionRequest.getResult().getDurationMs(), result.getDurationMs());
    assertEquals(executionRequest.getResult().getReport(), result.getReport());
    assertEquals(executionRequest.getResult().getStatus(), result.getStatus());
    assertEquals(executionRequest.getResult().getStartTimeMs(), result.getStartTimeMs());
  }

  private IngestTestUtils() { }
}
