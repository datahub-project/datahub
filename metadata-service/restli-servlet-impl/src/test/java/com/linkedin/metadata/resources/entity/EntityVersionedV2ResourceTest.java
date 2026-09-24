package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.common.urn.VersionedUrn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityVersionedV2ResourceTest {

  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:victim");

  @Test
  public void testBatchGetVersionedOmitsCredentialsWithoutManageUserCredentials()
      throws Exception {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.getEntitiesVersionedV2(any(), anySet(), anySet()))
        .thenReturn(
            Map.of(USER_URN, EntityV2ResourceTest.corpUserResponseWithCredentials(USER_URN)));

    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              AuthorizationResult.Type type =
                  PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE
                          .getType()
                          .equals(request.getPrivilege())
                      ? AuthorizationResult.Type.DENY
                      : AuthorizationResult.Type.ALLOW;
              return new AuthorizationResult(request, type, "");
            });

    EntityVersionedV2Resource resource = new EntityVersionedV2Resource();
    resource.setEntityService(entityService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());
    AuthenticationContext.setAuthentication(
        new Authentication(new Actor(ActorType.USER, "regular-user"), ""));

    Map<Urn, EntityResponse> responses =
        EntityV2ResourceTest.awaitTask(
            resource.batchGetVersioned(
                Set.of(new VersionedUrn(USER_URN.toString(), null)), "corpuser", null));

    assertEquals(
        responses.get(USER_URN).getAspects().keySet(),
        Set.of(Constants.CORP_USER_INFO_ASPECT_NAME));
  }
}
