package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests to verify that "direct" privileges (VIEW_DOMAIN_CHILDREN, MANAGE_DOMAIN_CHILDREN) only
 * grant access to direct children, not grandchildren or deeper descendants.
 */
public class PolicyEngineDirectPrivilegeTest {

  private static final Urn GRANDPARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:root");
  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private EntityClient _entityClient;
  private OperationContext _opContext;
  private PolicyEngine _policyEngine;

  @BeforeMethod
  public void setup() {
    _entityClient = mock(EntityClient.class);
    _opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    _policyEngine = new PolicyEngine(_entityClient);
  }

  @Test
  public void testDirectPrivilegeAllowsDirectChild() throws Exception {
    setupDomainHierarchy();

    DataHubPolicyInfo policy =
        createPolicy(
            PoliciesConfig.VIEW_DOMAIN_CHILDREN_PRIVILEGE.getType(), GRANDPARENT_DOMAIN_URN);

    ResolvedEntitySpec actor = buildEntitySpec(CORP_USER_ENTITY_NAME, TEST_USER_URN);
    ResolvedEntitySpec parentDomain = buildDomainSpec(PARENT_DOMAIN_URN, GRANDPARENT_DOMAIN_URN);

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            _opContext,
            policy,
            actor,
            PoliciesConfig.VIEW_DOMAIN_CHILDREN_PRIVILEGE.getType(),
            java.util.Optional.of(parentDomain),
            Collections.emptyList());

    assertTrue(result.isGranted(), "Direct child should be accessible with VIEW_DOMAIN_CHILDREN");
  }

  @Test
  public void testDirectPrivilegeDeniesGrandchild() throws Exception {
    setupDomainHierarchy();

    DataHubPolicyInfo policy =
        createPolicy(
            PoliciesConfig.VIEW_DOMAIN_CHILDREN_PRIVILEGE.getType(), GRANDPARENT_DOMAIN_URN);

    ResolvedEntitySpec actor = buildEntitySpec(CORP_USER_ENTITY_NAME, TEST_USER_URN);
    ResolvedEntitySpec childDomain = buildDomainSpec(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            _opContext,
            policy,
            actor,
            PoliciesConfig.VIEW_DOMAIN_CHILDREN_PRIVILEGE.getType(),
            java.util.Optional.of(childDomain),
            Collections.emptyList());

    assertFalse(
        result.isGranted(),
        "Grandchild should NOT be accessible with VIEW_DOMAIN_CHILDREN (direct only)");
  }

  @Test
  public void testAllPrivilegeAllowsGrandchild() throws Exception {
    setupDomainHierarchy();

    DataHubPolicyInfo policy =
        createPolicy(
            PoliciesConfig.VIEW_ALL_DOMAIN_CHILDREN_PRIVILEGE.getType(), GRANDPARENT_DOMAIN_URN);

    ResolvedEntitySpec actor = buildEntitySpec(CORP_USER_ENTITY_NAME, TEST_USER_URN);
    ResolvedEntitySpec childDomain = buildDomainSpec(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            _opContext,
            policy,
            actor,
            PoliciesConfig.VIEW_ALL_DOMAIN_CHILDREN_PRIVILEGE.getType(),
            java.util.Optional.of(childDomain),
            Collections.emptyList());

    assertTrue(
        result.isGranted(),
        "Grandchild should be accessible with VIEW_ALL_DOMAIN_CHILDREN (recursive)");
  }

  @Test
  public void testManageDirectPrivilegeDeniesGrandchild() throws Exception {
    setupDomainHierarchy();

    DataHubPolicyInfo policy =
        createPolicy(
            PoliciesConfig.MANAGE_DOMAIN_CHILDREN_PRIVILEGE.getType(), GRANDPARENT_DOMAIN_URN);

    ResolvedEntitySpec actor = buildEntitySpec(CORP_USER_ENTITY_NAME, TEST_USER_URN);
    ResolvedEntitySpec childDomain = buildDomainSpec(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);

    PolicyEngine.PolicyEvaluationResult result =
        _policyEngine.evaluatePolicy(
            _opContext,
            policy,
            actor,
            PoliciesConfig.MANAGE_DOMAIN_CHILDREN_PRIVILEGE.getType(),
            java.util.Optional.of(childDomain),
            Collections.emptyList());

    assertFalse(
        result.isGranted(),
        "Grandchild should NOT be manageable with MANAGE_DOMAIN_CHILDREN (direct only)");
  }

  private void setupDomainHierarchy() throws Exception {
    Map<Urn, Urn> parentMap = new HashMap<>();
    parentMap.put(PARENT_DOMAIN_URN, GRANDPARENT_DOMAIN_URN);
    parentMap.put(CHILD_DOMAIN_URN, PARENT_DOMAIN_URN);

    when(_entityClient.getV2(eq(_opContext), eq(DOMAIN_ENTITY_NAME), any(Urn.class), anySet()))
        .thenAnswer(
            invocation -> {
              Urn domainUrn = invocation.getArgument(2);
              Urn parentUrn = parentMap.get(domainUrn);
              if (parentUrn == null) {
                return null;
              }

              DomainProperties properties = new DomainProperties();
              properties.setParentDomain(parentUrn);

              EnvelopedAspect envelopedAspect = new EnvelopedAspect();
              envelopedAspect.setValue(new Aspect(properties.data()));

              EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
              aspectMap.put(DOMAIN_PROPERTIES_ASPECT_NAME, envelopedAspect);

              EntityResponse response = new EntityResponse();
              response.setUrn(domainUrn);
              response.setEntityName(DOMAIN_ENTITY_NAME);
              response.setAspects(aspectMap);

              return response;
            });
  }

  private DataHubPolicyInfo createPolicy(String privilege, Urn resourceUrn) {
    DataHubPolicyInfo policy = new DataHubPolicyInfo();
    policy.setType(PoliciesConfig.METADATA_POLICY_TYPE);
    policy.setState(PoliciesConfig.ACTIVE_POLICY_STATE);
    policy.setPrivileges(new com.linkedin.common.StringArray(privilege));
    policy.setDisplayName("Test Policy");

    DataHubActorFilter actors = new DataHubActorFilter();
    actors.setAllUsers(true);
    actors.setResourceOwners(false);
    policy.setActors(actors);

    DataHubResourceFilter resources = new DataHubResourceFilter();
    resources.setType(DOMAIN_ENTITY_NAME);
    resources.setResources(new UrnArray(ImmutableList.of(resourceUrn)));
    policy.setResources(resources);

    return policy;
  }

  private ResolvedEntitySpec buildEntitySpec(String entityType, Urn urn) {
    EntitySpec spec = new EntitySpec(entityType, urn.toString());
    return new ResolvedEntitySpec(spec, Collections.emptySet());
  }

  private ResolvedEntitySpec buildDomainSpec(Urn domainUrn, Urn parentUrn) {
    EntitySpec spec = new EntitySpec(DOMAIN_ENTITY_NAME, domainUrn.toString());
    ResolvedEntitySpec resolvedSpec = new ResolvedEntitySpec(spec, Collections.emptySet());

    resolvedSpec.addFieldValue(
        EntityFieldType.PARENT_DOMAIN, parentUrn.toString(), Collections.emptySet());

    return resolvedSpec;
  }
}
