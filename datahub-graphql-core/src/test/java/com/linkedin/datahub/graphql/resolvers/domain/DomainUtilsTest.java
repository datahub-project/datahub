package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainUtilsTest {

  private final String userUrn = "urn:li:corpuser:authorized";
  private final QueryContext mockContext = Mockito.mock(QueryContext.class);
  private final EntityClient mockClient = Mockito.mock(EntityClient.class);
  private final Urn parentDomainUrn = UrnUtils.getUrn("urn:li:domain:parent_domain");
  private final Urn parentDomainUrn1 = UrnUtils.getUrn("urn:li:domain:parent_domain1");
  private final Urn parentDomainUrn2 = UrnUtils.getUrn("urn:li:domain:parent_domain2");
  private final Urn parentDomainUrn3 = UrnUtils.getUrn("urn:li:domain:parent_domain3");

  @BeforeMethod
  private void setUpTests() throws Exception {
    Mockito.when(mockContext.getActorUrn()).thenReturn(userUrn);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    // Setup domain hierarchy: domain1 -> domain2 -> domain3 -> (root)
    DomainProperties parentDomain1 =
        new DomainProperties()
            .setName("Domain 1")
            .setParentDomain(UrnUtils.getUrn("urn:li:domain:parent_domain2"));
    DomainProperties parentDomain2 =
        new DomainProperties()
            .setName("Domain 2")
            .setParentDomain(UrnUtils.getUrn("urn:li:domain:parent_domain3"));
    DomainProperties parentDomain3 = new DomainProperties().setName("Domain 3");

    Map<String, EnvelopedAspect> parentDomain1Aspects = new HashMap<>();
    parentDomain1Aspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomain1.data())));

    Map<String, EnvelopedAspect> parentDomain2Aspects = new HashMap<>();
    parentDomain2Aspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomain2.data())));

    Map<String, EnvelopedAspect> parentDomain3Aspects = new HashMap<>();
    parentDomain3Aspects.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentDomain3.data())));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.DOMAIN_ENTITY_NAME),
                eq(parentDomainUrn1),
                eq(ImmutableSet.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentDomain1Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.DOMAIN_ENTITY_NAME),
                eq(parentDomainUrn2),
                eq(ImmutableSet.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentDomain2Aspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.DOMAIN_ENTITY_NAME),
                eq(parentDomainUrn3),
                eq(ImmutableSet.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(parentDomain3Aspects)));

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);
  }

  private void mockAuthRequest(
      String privilege, AuthorizationResult.Type allowOrDeny, EntitySpec resourceSpec) {
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(allowOrDeny);
    when(mockContext.getOperationContext().authorize(eq(privilege), eq(resourceSpec), any()))
        .thenReturn(result);
  }

  @Test
  public void testCanManageDomainsAuthorized() throws Exception {
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.ALLOW, null);

    assertTrue(DomainUtils.canManageDomains(mockContext));
  }

  @Test
  public void testCanManageDomainsUnauthorized() throws Exception {
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    assertFalse(DomainUtils.canManageDomains(mockContext));
  }

  @Test
  public void testCanManageChildDomainsWithManageDomains() throws Exception {
    // they have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.ALLOW, null);

    assertTrue(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn, mockClient));
  }

  @Test
  public void testCanManageChildDomainsNoParentDomain() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    assertFalse(DomainUtils.canManageChildDomains(mockContext, null, mockClient));
  }

  @Test
  public void testCanManageChildDomainsAuthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec);

    assertTrue(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn, mockClient));
  }

  @Test
  public void testCanManageChildDomainsUnauthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec);
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec);

    assertFalse(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn, mockClient));
  }

  @Test
  public void testCanManageChildDomainsRecursivelyAuthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertTrue(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanManageChildDomainsRecursivelyUnauthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertFalse(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanManageChildDomainsRecursivelyAuthorizedLevel2() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec2);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    assertTrue(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanManageChildDomainsRecursivelyUnauthorizedLevel2() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    assertFalse(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn2, mockClient));
  }

  @Test
  public void testCanManageChildDomainsRecursivelyNoLevel2() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    assertFalse(DomainUtils.canManageChildDomains(mockContext, parentDomainUrn3, mockClient));
  }

  @Test
  public void testCanUpdateDomainEntityAuthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec2);

    assertTrue(DomainUtils.canUpdateDomainEntity(parentDomainUrn1, mockContext, mockClient));
  }

  @Test
  public void testCanUpdateDomainEntityUnauthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec1);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("MANAGE_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("MANAGE_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    assertFalse(DomainUtils.canUpdateDomainEntity(parentDomainUrn1, mockContext, mockClient));
  }

  @Test
  public void testCanViewChildDomainsWithViewEntityPage() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("VIEW_ENTITY_PAGE", AuthorizationResult.Type.ALLOW, resourceSpec);

    assertTrue(DomainUtils.canViewChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanViewChildDomainsWithDirectViewPrivilege() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("VIEW_ENTITY_PAGE", AuthorizationResult.Type.DENY, resourceSpec1);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("VIEW_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec2);

    assertTrue(DomainUtils.canViewChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanViewChildDomainsRecursively() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("VIEW_ENTITY_PAGE", AuthorizationResult.Type.DENY, resourceSpec1);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("VIEW_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.ALLOW, resourceSpec3);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("VIEW_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);
    mockAuthRequest("VIEW_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    assertTrue(DomainUtils.canViewChildDomains(mockContext, parentDomainUrn1, mockClient));
  }

  @Test
  public void testCanViewChildDomainsUnauthorized() throws Exception {
    // they do NOT have the MANAGE_DOMAINS platform privilege
    mockAuthRequest("MANAGE_DOMAINS", AuthorizationResult.Type.DENY, null);

    final EntitySpec resourceSpec1 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn1.toString());
    mockAuthRequest("VIEW_ENTITY_PAGE", AuthorizationResult.Type.DENY, resourceSpec1);

    final EntitySpec resourceSpec2 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn2.toString());
    mockAuthRequest("VIEW_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);
    mockAuthRequest("VIEW_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec2);

    final EntitySpec resourceSpec3 =
        new EntitySpec(parentDomainUrn.getEntityType(), parentDomainUrn3.toString());
    mockAuthRequest("VIEW_ALL_DOMAIN_CHILDREN", AuthorizationResult.Type.DENY, resourceSpec3);

    assertFalse(DomainUtils.canViewChildDomains(mockContext, parentDomainUrn1, mockClient));
  }
}
