package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Set;
import java.util.UUID;
import org.testng.annotations.Test;

/** A7: who may deprecate a custom environment, without widening it for other entities. */
public class DeprecationUtilsTest {

  private static final Urn ENVIRONMENT = UrnUtils.getUrn("urn:li:customFabricType:MY_ENV");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,PROD)");

  @Test
  public void manageEnvironmentsDeprecatesEnvironmentsOnly() {
    QueryContext context =
        contextGranting(PoliciesConfig.MANAGE_CUSTOM_FABRIC_TYPES_PRIVILEGE.getType());

    assertTrue(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, ENVIRONMENT));
    assertFalse(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, DATASET));
  }

  @Test
  public void editDeprecationStillWorksForAnyEntity() {
    QueryContext context =
        contextGranting(PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE.getType());

    assertTrue(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, ENVIRONMENT));
    assertTrue(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, DATASET));
  }

  @Test
  public void noPrivilegeIsDenied() {
    QueryContext context = contextGranting();

    assertFalse(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, ENVIRONMENT));
    assertFalse(DeprecationUtils.isAuthorizedToUpdateDeprecationForEntity(context, DATASET));
  }

  /**
   * A context whose authorizer allows exactly the given privileges, on any resource. Each context
   * gets its own actor, because authorization results are cached per actor and request.
   */
  private static QueryContext contextGranting(String... privileges) {
    String actorId = "user-" + UUID.randomUUID();
    Set<String> granted = Set.of(privileges);
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any()))
        .thenAnswer(
            args -> {
              AuthorizationRequest request = args.getArgument(0);
              return new AuthorizationResult(
                  request,
                  granted.contains(request.getPrivilege())
                      ? AuthorizationResult.Type.ALLOW
                      : AuthorizationResult.Type.DENY,
                  "");
            });
    Authentication authentication = new Authentication(new Actor(ActorType.USER, actorId), "creds");
    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(authorizer, authentication);
    QueryContext context = mock(QueryContext.class);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:" + actorId);
    when(context.getAuthorizer()).thenReturn(authorizer);
    when(context.getAuthentication()).thenReturn(authentication);
    when(context.getOperationContext()).thenReturn(operationContext);
    return context;
  }
}
