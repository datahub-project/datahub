package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A3, A4: policies filtered on ENV, evaluated through the real entity spec resolver and policy
 * engine. ENV of key entities comes from the URN with no I/O; ENV of entities whose env is in an
 * aspect is fetched only when a policy filters on it.
 */
public class EnvPolicyTest {

  private static final String PRIVILEGE = "EDIT_ENTITY_DOCS";

  private SystemEntityClient entityClient;
  private OperationContext opContext;
  private DefaultEntitySpecResolver resolver;
  private PolicyEngine policyEngine;
  private ResolvedEntitySpec actor;

  @BeforeMethod
  public void setUp() {
    entityClient = mock(SystemEntityClient.class);
    GroupService groupService = mock(GroupService.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    resolver = new DefaultEntitySpecResolver(opContext, entityClient, groupService);
    policyEngine = new PolicyEngine(entityClient, groupService);
    actor = resolver.resolve(new EntitySpec(Constants.CORP_USER_ENTITY_NAME, "urn:li:corpuser:u"));
  }

  @Test
  public void testEnvFilterOnKeyEntitiesNeedsNoFetch() throws Exception {
    DataHubPolicyInfo prodOnly = policy("dataset", EntityFieldType.ENV, "PROD");
    DataHubPolicyInfo customOnly = policy("dataset", EntityFieldType.ENV, "MY_ENV");

    assertTrue(granted(prodOnly, "dataset", "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"));
    assertFalse(granted(prodOnly, "dataset", "urn:li:dataset:(urn:li:dataPlatform:hive,t,DEV)"));
    assertTrue(
        granted(customOnly, "dataset", "urn:li:dataset:(urn:li:dataPlatform:hive,t,MY_ENV)"));
    assertFalse(granted(customOnly, "dataset", "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)"));
    verify(entityClient, never()).getV2(any(), any(), any(), anySet());
  }

  @Test
  public void testEnvFilterOnAspectEntityFetchesTheAspect() throws Exception {
    Urn prodContainer = UrnUtils.getUrn("urn:li:container:prod");
    Urn devContainer = UrnUtils.getUrn("urn:li:container:dev");
    containerWithEnv(prodContainer, "PROD");
    containerWithEnv(devContainer, "DEV");
    DataHubPolicyInfo prodOnly = policy("container", EntityFieldType.ENV, "PROD");

    assertTrue(granted(prodOnly, "container", prodContainer.toString()));
    assertFalse(granted(prodOnly, "container", devContainer.toString()));
  }

  @Test
  public void testEnvFilterDoesNotMatchEntitiesWithoutAnEnv() throws Exception {
    DataHubPolicyInfo prodOnly = policy("chart", EntityFieldType.ENV, "PROD");

    assertFalse(granted(prodOnly, "chart", "urn:li:chart:(looker,c)"));
    verify(entityClient, never()).getV2(any(), any(), any(), anySet());
  }

  private boolean granted(DataHubPolicyInfo policy, String entityType, String urn) {
    ResolvedEntitySpec resource = resolver.resolve(new EntitySpec(entityType, urn));
    return policyEngine
        .evaluatePolicy(
            opContext, policy, actor, PRIVILEGE, Optional.of(resource), Collections.emptyList())
        .isGranted();
  }

  private void containerWithEnv(Urn urn, String env) throws Exception {
    ContainerProperties properties = new ContainerProperties().setName(urn.getId());
    com.linkedin.common.FabricTypeNames.set(properties, "env", env);
    EntityResponse response =
        new EntityResponse()
            .setUrn(urn)
            .setEntityName(Constants.CONTAINER_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    Map.of(
                        Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(properties.data())))));
    when(entityClient.getV2(any(), eq(Constants.CONTAINER_ENTITY_NAME), eq(urn), anySet()))
        .thenReturn(response);
  }

  private static DataHubPolicyInfo policy(
      String resourceType, EntityFieldType field, String value) {
    DataHubPolicyInfo info = new DataHubPolicyInfo();
    info.setType("METADATA");
    info.setState("ACTIVE");
    info.setPrivileges(new StringArray(PRIVILEGE));
    info.setDisplayName("test");
    info.setEditable(true);
    info.setActors(
        new DataHubActorFilter().setAllUsers(true).setAllGroups(false).setResourceOwners(false));
    DataHubResourceFilter resources = new DataHubResourceFilter();
    resources.setFilter(
        FilterUtils.newFilter(
            Map.of(EntityFieldType.TYPE, List.of(resourceType), field, List.of(value))));
    info.setResources(resources);
    return info;
  }
}
