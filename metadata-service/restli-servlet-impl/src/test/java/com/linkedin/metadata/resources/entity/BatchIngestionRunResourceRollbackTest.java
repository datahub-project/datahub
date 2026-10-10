package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.service.RollbackNotHandedOffException;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Field;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** How the rollback action treats a rollback that was not handed off versus one that failed. */
public class BatchIngestionRunResourceRollbackTest {
  private static final String RUN_ID = "my-run-id";

  private BatchIngestionRunResource resource;
  private RollbackService rollbackService;

  @BeforeMethod
  public void setup() throws Exception {
    rollbackService = mock(RollbackService.class);
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, ""));

    resource = new BatchIngestionRunResource();
    inject("rollbackService", rollbackService);
    inject("authorizer", authorizer);
    inject("systemOperationContext", TestOperationContexts.systemContextNoSearchAuthorization());

    final Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "user"));
    AuthenticationContext.setAuthentication(authentication);
  }

  @AfterMethod
  public void tearDown() {
    AuthenticationContext.remove();
  }

  @Test
  public void rollbackNotHandedOffIsA409AndTheRunKeepsItsStatus() throws Exception {
    when(rollbackService.rollbackIngestion(
            any(OperationContext.class), eq(RUN_ID), anyBoolean(), anyBoolean(), any()))
        .thenThrow(new RollbackNotHandedOffException("not handed off"));

    final RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class, () -> resource.rollback(RUN_ID, false, null, null));

    assertEquals(thrown.getStatus(), HttpStatus.S_409_CONFLICT);
    verify(rollbackService, never())
        .updateExecutionRequestStatus(any(OperationContext.class), any(), any());
  }

  @Test
  public void rollbackFailureMarksTheRunRollbackFailed() throws Exception {
    when(rollbackService.rollbackIngestion(
            any(OperationContext.class), eq(RUN_ID), anyBoolean(), anyBoolean(), any()))
        .thenThrow(new IllegalStateException("boom"));

    assertThrows(Exception.class, () -> resource.rollback(RUN_ID, false, null, null));

    verify(rollbackService)
        .updateExecutionRequestStatus(
            any(OperationContext.class), eq(RUN_ID), eq(RollbackService.ROLLBACK_FAILED_STATUS));
  }

  private void inject(final String fieldName, final Object value) throws Exception {
    final Field field = BatchIngestionRunResource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(resource, value);
  }
}
