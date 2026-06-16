package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class AuthorizationContextTest {

  private static final DataHubPolicyInfo POLICY =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setUsers(new UrnArray(UrnUtils.getUrn("urn:li:corpuser:testuser"))))
          .setPrivileges(new StringArray(List.of("VIEW", "EDIT")));

  @Test
  public void authorizePassesActorPoliciesByPrivilegeForResourceOnly() {
    Authorizer authorizer = mock(Authorizer.class);
    ArgumentCaptor<AuthorizationRequest> captor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    when(authorizer.authorize(captor.capture()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.DENY, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");

    authorizationContext.authorize(actorContext, "VIEW", resource);

    AuthorizationRequest request = captor.getValue();
    assertEquals(request.getActorPoliciesByPrivilege(), actorPoliciesByPrivilege);
    assertEquals(request.getPrivilege(), "VIEW");
    assertEquals(request.getResourceSpec().orElse(null), resource);
  }

  @Test
  public void authorizePassesActorPoliciesByPrivilegeWithSubResources() {
    Authorizer authorizer = mock(Authorizer.class);
    ArgumentCaptor<AuthorizationRequest> captor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    when(authorizer.authorize(captor.capture()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.DENY, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");
    EntitySpec subResource = new EntitySpec("tag", "urn:li:tag:test");

    authorizationContext.authorize(actorContext, "EDIT", resource, List.of(subResource));

    AuthorizationRequest request = captor.getValue();
    assertEquals(request.getActorPoliciesByPrivilege(), actorPoliciesByPrivilege);
    assertEquals(request.getSubResources(), List.of(subResource));
    verify(authorizer, times(1)).authorize(any());
  }

  private static ActorContext sessionActor(
      Map<String, List<RecordTemplate>> actorPoliciesByPrivilege) {
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "testuser"), "");
    return ActorContext.builder()
        .authentication(authentication)
        .policyInfoSet(Set.of(POLICY))
        .actorPoliciesByPrivilege(actorPoliciesByPrivilege)
        .groupMembership(Collections.emptyList())
        .build();
  }
}
