package com.linkedin.metadata.resources.operations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.restli.server.ResourceContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UtilsTest {

  @Test
  public void testRestoreIndicesBuildsUsageInstrumentedOperationContext() {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, "allowed"));

    EntityService<?> entityService = mock(EntityService.class);
    RestoreIndicesResult restoreResult = new RestoreIndicesResult();
    restoreResult.rowsMigrated = 1;
    when(entityService.restoreIndices(any(), any(), any()))
        .thenReturn(Collections.singletonList(restoreResult));

    ResourceContext resourceContext = mock(ResourceContext.class);
    when(resourceContext.getRawRequestContext())
        .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
    when(resourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR"))
        .thenReturn("127.0.0.1");

    try (MockedStatic<AuthenticationContext> authContext =
        Mockito.mockStatic(AuthenticationContext.class)) {
      authContext
          .when(AuthenticationContext::getAuthentication)
          .thenReturn(new Authentication(new Actor(ActorType.USER, "test"), "token"));

      String result =
          Utils.restoreIndices(
              TestOperationContexts.systemContextNoSearchAuthorization(),
              resourceContext,
              "status",
              "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)",
              null,
              0,
              100,
              1000,
              null,
              null,
              authorizer,
              entityService,
              false);

      assertNotNull(result);
    }
  }
}
