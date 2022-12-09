package com.linkedin.datahub.graphql.resolvers.glossary;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.*;

public class GlossaryUtilsTest {

  private final String userUrn = "urn:li:corpuser:authorized";
  private final QueryContext mockContext = Mockito.mock(QueryContext.class);
  private final Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
  private final Urn parentNodeUrn = UrnUtils.getUrn("urn:li:glossaryNode:parent_node");

  private void setUpTests() {
    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getActorUrn()).thenReturn(userUrn);
  }

  private void mockAuthRequest(String privilege, AuthorizationResult.Type allowOrDeny, ResourceSpec resourceSpec) {
    final AuthorizationRequest authorizationRequest = new AuthorizationRequest(
        userUrn,
        privilege,
        resourceSpec != null ? Optional.of(resourceSpec) : Optional.empty()
    );
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(allowOrDeny);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(authorizationRequest))).thenReturn(result);
  }

  @Test
  public void testCanManageGlossariesAuthorized() {
    setUpTests();
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.ALLOW, null);

    assertTrue(GlossaryUtils.canManageGlossaries(mockContext));
  }

  @Test
  public void testCanManageGlossariesUnauthorized() {
    setUpTests();
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    assertFalse(GlossaryUtils.canManageGlossaries(mockContext));
  }

  @Test
  public void testCanManageChildrenEntitiesWithManageGlossaries() {
    setUpTests();
    // they have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.ALLOW, null);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn));
  }

  @Test
  public void testCanManageChildrenEntitiesNoParentNode() {
    setUpTests();
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, null));
  }

  @Test
  public void testCanManageChildrenEntitiesAuthorized() {
    setUpTests();
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final ResourceSpec resourceSpec = new ResourceSpec(parentNodeUrn.getEntityType(), parentNodeUrn.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec);

    assertTrue(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn));
  }

  @Test
  public void testCanManageChildrenEntitiesUnauthorized() {
    setUpTests();
    // they do NOT have the MANAGE_GLOSSARIES platform privilege
    mockAuthRequest("MANAGE_GLOSSARIES", AuthorizationResult.Type.DENY, null);

    final ResourceSpec resourceSpec = new ResourceSpec(parentNodeUrn.getEntityType(), parentNodeUrn.toString());
    mockAuthRequest("MANAGE_GLOSSARY_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec);

    assertFalse(GlossaryUtils.canManageChildrenEntities(mockContext, parentNodeUrn));
  }
}
