package com.datahub.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.EXISTS;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.ConfigEntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.SystemDataVisibility;
import com.linkedin.metadata.models.annotation.SystemEntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.policy.SystemDataPolicy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SystemDataAuthUtilTest {

  @BeforeClass
  public void enableRestAuth() {
    AuthUtil authUtil = new AuthUtil();
    authUtil.restApiAuthorizationEnabled = true;
    authUtil.systemDataAccessControlEnabledOverride = true;
    authUtil.init();
  }

  @AfterMethod
  public void resetSystemDataAccessControl() {
    AuthUtil authUtil = new AuthUtil();
    authUtil.restApiAuthorizationEnabled = true;
    authUtil.systemDataAccessControlEnabledOverride = true;
    authUtil.init();
  }

  @Test
  public void testHiddenSystemEntityAllowedWhenAccessControlDisabled() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(false, false);
    when(registry.getEntitySpecs()).thenReturn(Map.of("testsystemdata", systemEntity));
    when(registry.getEntitySpec("testSystemData")).thenReturn(systemEntity);

    AuthUtil authUtil = new AuthUtil();
    authUtil.restApiAuthorizationEnabled = true;
    authUtil.systemDataAccessControlEnabledOverride = false;
    authUtil.init();

    AuthorizationSession session = new RegistrySession(registry, (priv, resource) -> allowResult());

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            session, READ, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
  }

  @Test
  public void testHiddenSystemEntityDeniedBeforePolicyEngine() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(false, false);
    when(registry.getEntitySpecs()).thenReturn(Map.of("testsystemdata", systemEntity));
    when(registry.getEntitySpec("testSystemData")).thenReturn(systemEntity);

    AuthorizationSession session = new RegistrySession(registry, (priv, resource) -> allowResult());

    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrns(
            session, READ, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrns(
            session, EXISTS, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
  }

  @Test
  public void testHiddenSystemEntityDeniedForGraphQLStyleAuth() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(false, false);
    when(registry.getEntitySpecs()).thenReturn(Map.of("testsystemdata", systemEntity));
    when(registry.getEntitySpec("testSystemData")).thenReturn(systemEntity);

    AuthorizationSession session = new RegistrySession(registry, (priv, resource) -> allowResult());

    assertFalse(AuthUtil.canViewEntity(session, UrnUtils.getUrn("urn:li:testSystemData:abc")));
    assertFalse(
        AuthUtil.isAuthorizedEntityUrns(
            session, READ, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
    assertFalse(
        AuthUtil.isAuthorizedUrns(
            session, ENTITY, EXISTS, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
  }

  @Test
  public void testPolicyEligibleSystemEntityReachesPolicyEngine() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(true, true);
    when(registry.getEntitySpecs())
        .thenReturn(Map.of("testsystemdatapolicyeligible", systemEntity));
    when(registry.getEntitySpec("testSystemDataPolicyEligible")).thenReturn(systemEntity);

    AuthorizationSession allowSession =
        new RegistrySession(registry, (priv, resource) -> allowResult());
    AuthorizationSession denySession =
        new RegistrySession(registry, (priv, resource) -> denyResult());

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            allowSession,
            READ,
            List.of(UrnUtils.getUrn("urn:li:testSystemDataPolicyEligible:abc"))));
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrns(
            denySession,
            READ,
            List.of(UrnUtils.getUrn("urn:li:testSystemDataPolicyEligible:abc"))));
  }

  @Test
  public void testAllowReadImpliesExistsAtApiGate() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(true, false);
    when(registry.getEntitySpecs()).thenReturn(Map.of("testsystemdata", systemEntity));
    when(registry.getEntitySpec("testSystemData")).thenReturn(systemEntity);

    AuthorizationSession session = new RegistrySession(registry, (priv, resource) -> allowResult());

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            session, EXISTS, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
  }

  @Test
  public void testBulkReadEntityTypeAuthPassesWhenHiddenSystemEntitiesExcluded() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec hiddenSystem = buildSystemEntity(false, false);
    ConfigEntitySpec dataset = new ConfigEntitySpec("dataset", "datasetKey", List.of(), "default");
    when(registry.getEntitySpecs())
        .thenReturn(Map.of("testsystemdata", hiddenSystem, "dataset", dataset));
    when(registry.getEntitySpec("dataset")).thenReturn(dataset);

    AuthorizationSession session = new RegistrySession(registry, (priv, resource) -> allowResult());

    assertFalse(
        AuthUtil.isAPIAuthorizedEntityType(session, READ, List.of("testSystemData", "dataset")));

    Collection<String> scrollEntityTypes =
        SystemDataPolicy.resolveEntityNamesForRead(registry, null, true);
    assertTrue(scrollEntityTypes.contains("dataset"));
    assertFalse(scrollEntityTypes.contains("testSystemData"));
    assertTrue(AuthUtil.isAPIAuthorizedEntityType(session, READ, scrollEntityTypes));
  }

  @Test
  public void testAllowExistsWithoutReadReachesPolicyEngineForExistsOnly() {
    EntityRegistry registry = mock(EntityRegistry.class);
    ConfigEntitySpec systemEntity = buildSystemEntity(false, true);
    when(registry.getEntitySpecs()).thenReturn(Map.of("testsystemdata", systemEntity));
    when(registry.getEntitySpec("testSystemData")).thenReturn(systemEntity);

    AuthorizationSession allowSession =
        new RegistrySession(registry, (priv, resource) -> allowResult());

    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrns(
            allowSession, READ, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            allowSession, EXISTS, List.of(UrnUtils.getUrn("urn:li:testSystemData:abc"))));
  }

  private static ConfigEntitySpec buildSystemEntity(boolean allowRead, boolean allowExists) {
    AspectSpec key =
        new AspectSpec(
            new AspectAnnotation("key", false, false, null, 0),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null);
    key.setSystemEntityAnnotation(
        new SystemEntityAnnotation(new SystemDataVisibility(true, allowRead, allowExists)));
    ConfigEntitySpec entity =
        new ConfigEntitySpec("testSystemData", "key", List.of(key), "default");
    entity.setSystemEntityFlags(true, allowRead, allowExists);
    return entity;
  }

  private static AuthorizationResult allowResult() {
    return new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, "");
  }

  private static AuthorizationResult denyResult() {
    return new AuthorizationResult(null, AuthorizationResult.Type.DENY, "");
  }

  private static final class RegistrySession
      implements AuthorizationSession, EntityRegistryAuthorizationSession {
    private final EntityRegistry entityRegistry;
    private final java.util.function.BiFunction<String, EntitySpec, AuthorizationResult> fn;

    private RegistrySession(
        EntityRegistry entityRegistry,
        java.util.function.BiFunction<String, EntitySpec, AuthorizationResult> fn) {
      this.entityRegistry = entityRegistry;
      this.fn = fn;
    }

    @Override
    public AuthorizationResult authorize(String privilege, EntitySpec resourceSpec) {
      return fn.apply(privilege, resourceSpec);
    }

    @Override
    public AuthorizationResult authorize(
        String privilege, EntitySpec resourceSpec, java.util.Collection<EntitySpec> subResources) {
      return authorize(privilege, resourceSpec);
    }

    @Override
    public EntityRegistry getEntityRegistry() {
      return entityRegistry;
    }
  }
}
