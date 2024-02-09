package com.linkedin.datahub.graphql.resolvers.ingest;

import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionAuthUtilsTest {

  @Test
  public void testCanManageIngestionAuthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:authorized", "MANAGE_INGESTION", Optional.empty());

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageIngestionUnauthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:unauthorized", "MANAGE_INGESTION", Optional.empty());

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageSecretsAuthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);

    AuthorizationRequest request =
        new AuthorizationRequest("urn:li:corpuser:authorized", "MANAGE_SECRETS", Optional.empty());

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageSecrets(mockContext));
  }

  @Test
  public void testCanManageSecretsUnauthorized() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);

    AuthorizationRequest request =
        new AuthorizationRequest(
            "urn:li:corpuser:unauthorized", "MANAGE_SECRETS", Optional.empty());

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageSecrets(mockContext));
  }
}
