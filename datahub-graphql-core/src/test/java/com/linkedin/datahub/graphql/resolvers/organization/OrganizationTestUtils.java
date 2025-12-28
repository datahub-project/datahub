package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.metadata.Constants.USER_ORGANIZATIONS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.UserOrganizations;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.utils.GenericRecordUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OrganizationTestUtils {

  public static QueryContext getMockContextWithUserOrganizations(
      String actorUrn, Set<Urn> userOrganizations) {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, "");
    when(mockAuthorizer.authorize(any())).thenReturn(result);

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    when(mockContext.getAuthentication()).thenReturn(authentication);

    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    if (!userOrganizations.isEmpty()) {
      UserOrganizations userOrgs = new UserOrganizations();
      userOrgs.setOrganizations(new UrnArray(userOrganizations));
      Aspect userOrgsAspect = new Aspect(GenericRecordUtils.serializeAspect(userOrgs).data());
      Map<Urn, Map<String, Aspect>> aspectMap = new HashMap<>();
      Map<String, Aspect> userAspects = new HashMap<>();
      userAspects.put(USER_ORGANIZATIONS_ASPECT_NAME, userOrgsAspect);
      aspectMap.put(UrnUtils.getUrn(actorUrn), userAspects);
      when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet())).thenReturn(aspectMap);
      when(mockAspectRetriever.getLatestAspectObject(any(Urn.class), any()))
          .thenAnswer(
              invocation -> {
                Urn urn = invocation.getArgument(0);
                String aspectName = invocation.getArgument(1);
                if (urn.toString().equals(actorUrn)
                    && aspectName.equals(USER_ORGANIZATIONS_ASPECT_NAME)) {
                  return userOrgsAspect;
                }
                return null;
              });
    } else {
      when(mockAspectRetriever.getLatestAspectObjects(anySet(), anySet()))
          .thenReturn(new HashMap<>());
      when(mockAspectRetriever.getLatestAspectObject(any(Urn.class), any())).thenReturn(null);
    }

    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(mockAspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(() -> null))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();

    OperationContext operationContext =
        TestOperationContexts.systemContext(
            null,
            () -> authentication,
            null,
            null,
            () -> retrieverContext,
            null,
            null,
            null,
            null,
            null);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }
}
