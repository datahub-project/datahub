package com.linkedin.datahub.graphql.resolvers.ingest;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionAuthUtilsTest {

  @Test
  public void testCanManageIngestionAuthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    eq("MANAGE_INGESTION"),
                    eq(new EntitySpec(Constants.INGESTION_SOURCE_ENTITY_NAME, ""))))
        .thenReturn(result);

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageIngestionUnauthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    eq("MANAGE_INGESTION"),
                    eq(new EntitySpec(Constants.INGESTION_SOURCE_ENTITY_NAME, ""))))
        .thenReturn(result);

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageSecretsAuthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    eq("MANAGE_SECRETS"), eq(new EntitySpec(Constants.SECRETS_ENTITY_NAME, ""))))
        .thenReturn(result);

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageSecrets(mockContext));
  }

  @Test
  public void testCanManageSecretsUnauthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:unauthorized",
            "MANAGE_SECRETS",
            Optional.of(new EntitySpec(Constants.SECRETS_ENTITY_NAME, "")));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    eq("MANAGE_SECRETS"), eq(new EntitySpec(Constants.SECRETS_ENTITY_NAME, ""))))
        .thenReturn(result);

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageSecrets(mockContext));
  }
}
