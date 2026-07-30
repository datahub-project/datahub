package com.linkedin.metadata.resources.restli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class RestliUtilsTest {

  @Test
  public void testAsSessionMapsActorAccessExceptionToForbidden() {
    OperationContext systemOpContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Authentication auth = new Authentication(new Actor(ActorType.USER, "test"), "credentials");
    RequestContext.RequestContextBuilder requestContext =
        RequestContext.builder().buildRestli("urn:li:corpuser:test", null, "test", java.util.List.of());

    try (MockedStatic<OperationContext> mocked = mockStatic(OperationContext.class)) {
      mocked
          .when(
              () ->
                  OperationContext.asSession(
                      eq(systemOpContext),
                      eq(requestContext),
                      any(Authorizer.class),
                      eq(auth),
                      anyBoolean()))
          .thenThrow(new ActorAccessException("actor denied"));

      RestLiServiceException thrown =
          expectThrows(
              RestLiServiceException.class,
              () ->
                  RestliUtils.asSession(
                      systemOpContext, requestContext, Authorizer.EMPTY, auth, true));

      assertEquals(thrown.getStatus(), HttpStatus.S_403_FORBIDDEN);
      assertEquals(thrown.getMessage(), "actor denied");
    }
  }

  @Test
  public void testToTaskMapsDirectActorAccessExceptionToForbidden() {
    RestLiServiceException thrown =
        expectThrows(
            RestLiServiceException.class,
            () ->
                RestliUtils.toTask(
                    () -> {
                      throw new ActorAccessException("direct deny");
                    }));

    assertEquals(thrown.getStatus(), HttpStatus.S_403_FORBIDDEN);
    assertEquals(thrown.getMessage(), "direct deny");
  }
}
