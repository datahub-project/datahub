package com.linkedin.datahub.graphql.resolvers.ingest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Secret;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.secret.DataHubSecretValue;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;

public class IngestTestUtils {

  public static final Urn TEST_INGESTION_SOURCE_URN =
      Urn.createFromTuple(Constants.INGESTION_SOURCE_ENTITY_NAME, "test");
  public static final Urn TEST_SECRET_URN =
      Urn.createFromTuple(Constants.SECRETS_ENTITY_NAME, "TEST_SECRET");
  public static final Urn TEST_EXECUTION_REQUEST_URN =
      Urn.createFromTuple(Constants.EXECUTION_REQUEST_ENTITY_NAME, "1234");

  public static QueryContext getMockAllowContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getOperationContext().authorize(any(), nullable(EntitySpec.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
    return mockContext;
  }

  public static QueryContext getMockDenyContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getOperationContext().authorize(any(), nullable(EntitySpec.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
    return mockContext;
  }

  public static DataHubIngestionSourceInfo getTestIngestionSourceInfo() {
    DataHubIngestionSourceInfo info = new DataHubIngestionSourceInfo();
    info.setName("My Test Source");
    info.setType("mysql");
    info.setSchedule(
        new DataHubIngestionSourceSchedule().setTimezone("UTC").setInterval("* * * * *"));
    info.setConfig(
        new DataHubIngestionSourceConfig()
            .setVersion("0.8.18")
            .setRecipe("{}")
            .setExecutorId("executor id"));
    return info;
  }

  public static DataHubSecretValue getTestSecretValue() {
    DataHubSecretValue value = new DataHubSecretValue();
    value.setValue("encryptedvalue");
    value.setName(TEST_SECRET_URN.getId());
    value.setDescription("none");
    return value;
  }

  public static void verifyTestSecretGraphQL(Secret secret, DataHubSecretValue value) {
    assertEquals(secret.getUrn(), TEST_SECRET_URN.toString());
    assertEquals(secret.getName(), value.getName());
    assertEquals(secret.getDescription(), value.getDescription());
  }

  private IngestTestUtils() {}
}
