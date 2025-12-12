package com.linkedin.datahub.graphql.resolvers.ingest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.ConstantAuthorizationResultMap;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.PredefinedAuthorizationResultMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionAuthUtilsTest {

  @Test
  public void testCanManageIngestionAuthorized() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "EDIT_ENTITY", "DELETE_ENTITY");
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    any(),
                    eq(new EntitySpec(Constants.INGESTION_SOURCE_ENTITY_NAME, "")),
                    anyCollection()))
        .thenReturn(
            new BatchAuthorizationResult(
                null, new PredefinedAuthorizationResultMap(allowedPrivileges)));

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageIngestionUnauthorized() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    anySet(),
                    eq(new EntitySpec(Constants.INGESTION_SOURCE_ENTITY_NAME, "")),
                    anyCollection()))
        .thenReturn(
            new BatchAuthorizationResult(
                null, new ConstantAuthorizationResultMap(AuthorizationResult.Type.DENY)));

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageIngestion(mockContext));
  }

  @Test
  public void testCanManageSecretsAuthorized() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    anySet(),
                    eq(new EntitySpec(Constants.SECRETS_ENTITY_NAME, "")),
                    anyCollection()))
        .thenReturn(
            new BatchAuthorizationResult(
                null, new PredefinedAuthorizationResultMap(Set.of("MANAGE_SECRETS"))));

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:authorized");

    assertTrue(IngestionAuthUtils.canManageSecrets(mockContext));
  }

  @Test
  public void testCanManageSecretsUnauthorized() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    Mockito.when(
            mockContext
                .getOperationContext()
                .authorize(
                    anySet(),
                    eq(new EntitySpec(Constants.SECRETS_ENTITY_NAME, "")),
                    anyCollection()))
        .thenReturn(
            new BatchAuthorizationResult(
                null, new ConstantAuthorizationResultMap(AuthorizationResult.Type.DENY)));

    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:unauthorized");

    assertFalse(IngestionAuthUtils.canManageSecrets(mockContext));
  }
}
