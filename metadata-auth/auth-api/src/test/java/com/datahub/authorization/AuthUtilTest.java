package com.datahub.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static com.linkedin.metadata.authorization.PoliciesConfig.API_ENTITY_PRIVILEGE_MAP;
import static com.linkedin.metadata.authorization.PoliciesConfig.API_PRIVILEGE_MAP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.Conjunctive;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.util.Pair;
import io.datahubproject.test.metadata.context.TestAuthSession;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@SpringBootTest
@TestPropertySource(properties = {"authorization.restApiAuthorization=true"})
public class AuthUtilTest {

  // The AuthUtil @PostConstruct is not getting called from the unit tests, so calling
  // it explicitly.
  @BeforeClass
  public void beforeAll() {
    authUtil = new AuthUtil();
    authUtil.restApiAuthorizationEnabled = true;
    authUtil.init();
  }

  @Autowired private AuthUtil authUtil;

  private static final Authentication TEST_AUTH_A =
      new Authentication(new Actor(ActorType.USER, "testA"), "");
  private static final Authentication TEST_AUTH_B =
      new Authentication(new Actor(ActorType.USER, "testB"), "");
  private static final Urn TEST_ENTITY_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:s3,1,PROD)");
  private static final Urn TEST_ENTITY_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,2,PROD)");
  private static final Urn TEST_ENTITY_3 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,3,PROD)");
  private static final Urn TEST_GLOBAL_SETTINGS_URN = UrnUtils.getUrn("urn:li:globalSettings:0");

  @Test
  public void testSimplePrivilegeGroupBuilder() {
    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupAPIPrivilege(ENTITY, READ, "dataset")),
        new DisjunctivePrivilegeGroup(
            List.of(
                new ConjunctivePrivilegeGroup(List.of("VIEW_ENTITY_PAGE")),
                new ConjunctivePrivilegeGroup(List.of("GET_ENTITY_PRIVILEGE")),
                new ConjunctivePrivilegeGroup(List.of("EDIT_ENTITY")),
                new ConjunctivePrivilegeGroup(List.of("DELETE_ENTITY")))));
  }

  @Test
  public void testManageEntityPrivilegeGroupBuilder() {
    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupEntityAPIPrivilege(MANAGE, Constants.POLICY_ENTITY_NAME)),
        new DisjunctivePrivilegeGroup(
            List.of(new ConjunctivePrivilegeGroup(List.of("MANAGE_POLICIES")))));
  }

  @Test
  public void testIsAPIAuthorizedUrns() {
    Authorizer mockAuthorizer =
        mockAuthorizer(
            Map.of(
                TEST_AUTH_A.getActor().toUrnStr(),
                    Map.of(
                        "EDIT_ENTITY", Set.of(TEST_ENTITY_1, TEST_ENTITY_2),
                        "VIEW_ENTITY_PAGE", Set.of(TEST_ENTITY_3)),
                TEST_AUTH_B.getActor().toUrnStr(),
                    Map.of("VIEW_ENTITY_PAGE", Set.of(TEST_ENTITY_1, TEST_ENTITY_3))));

    // User A (Entity 1 & 2 Edit, View only Entity 3)
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3)),
        "Expected read allowed for all entities");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            ENTITY,
            List.of(
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_1),
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_2),
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_3))),
        Map.of(
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_1), 200,
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_2), 200,
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_3), 403),
        "Expected edit on entities 1 and 2 and denied on 3");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            ENTITY,
            List.of(
                Pair.of(ChangeType.DELETE, TEST_ENTITY_1),
                Pair.of(ChangeType.DELETE, TEST_ENTITY_2),
                Pair.of(ChangeType.DELETE, TEST_ENTITY_3))),
        Map.of(
            Pair.of(ChangeType.DELETE, TEST_ENTITY_1), 403,
            Pair.of(ChangeType.DELETE, TEST_ENTITY_2), 403,
            Pair.of(ChangeType.DELETE, TEST_ENTITY_3), 403),
        "Expected deny on delete for all entities");

    // User B Entity 2 Denied, Read access 1 & 3
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrns(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3)),
        "Expected read denied for based on entity 2");
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_3)),
        "Expected read allowed due to exclusion of entity 2");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            ENTITY,
            List.of(
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_1),
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_2),
                Pair.of(ChangeType.UPSERT, TEST_ENTITY_3))),
        Map.of(
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_1), 403,
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_2), 403,
            Pair.of(ChangeType.UPSERT, TEST_ENTITY_3), 403),
        "Expected edit on entities 1-3 to be denied");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            ENTITY,
            List.of(
                Pair.of(ChangeType.DELETE, TEST_ENTITY_1),
                Pair.of(ChangeType.DELETE, TEST_ENTITY_2),
                Pair.of(ChangeType.DELETE, TEST_ENTITY_3))),
        Map.of(
            Pair.of(ChangeType.DELETE, TEST_ENTITY_1), 403,
            Pair.of(ChangeType.DELETE, TEST_ENTITY_2), 403,
            Pair.of(ChangeType.DELETE, TEST_ENTITY_3), 403),
        "Expected deny on delete for all entities");
  }

  @Test
  public void testReadInheritance() {
    assertTrue(
        AuthUtil.lookupAPIPrivilege(ApiGroup.ENTITY, ApiOperation.READ, "dataset")
            .containsAll(API_PRIVILEGE_MAP.get(ENTITY).get(READ)),
        "Expected most privileges to imply VIEW");
  }

  @Test
  public void testManageConjoin() {
    assertTrue(
        AuthUtil.lookupAPIPrivilege(ApiGroup.ENTITY, ApiOperation.MANAGE, "dataset")
            .contains(
                Conjunctive.of(
                    API_PRIVILEGE_MAP.get(ENTITY).get(UPDATE).get(0).get(0),
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.DELETE).get(0).get(0))),
        "Expected MANAGE to require both EDIT and DELETE");
  }

  @Test
  public void testEntityType() {
    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.MANAGE, "dataset")
            .contains(
                Conjunctive.of(
                    API_PRIVILEGE_MAP.get(ENTITY).get(UPDATE).get(0).get(0),
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.DELETE).get(0).get(0))),
        "Expected MANAGE on dataset to require both EDIT and DELETE");

    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.MANAGE, "dataHubPolicy")
            .contains(
                Conjunctive.of(
                    API_ENTITY_PRIVILEGE_MAP.get("dataHubPolicy").get(UPDATE).get(0).get(0))),
        "Expected MANAGE permission directly on dataHubPolicy entity");

    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupEntityAPIPrivilege(UPDATE, "globalSettings")),
        new DisjunctivePrivilegeGroup(
            List.of(
                new ConjunctivePrivilegeGroup(
                    List.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS.getType())))),
        "Expected UPDATE on globalSettings to require MANAGE_GLOBAL_SETTINGS");

    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(READ, "globalSettings")
            .contains(Conjunctive.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS)),
        "Expected READ on globalSettings to allow MANAGE_GLOBAL_SETTINGS as an alternative");
    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.EXISTS, "globalSettings")
            .contains(Conjunctive.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS)),
        "Expected EXISTS on globalSettings to allow MANAGE_GLOBAL_SETTINGS as an alternative");

    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupEntityAPIPrivilege(CREATE, "globalSettings")),
        new DisjunctivePrivilegeGroup(
            List.of(
                new ConjunctivePrivilegeGroup(
                    List.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS.getType())))),
        "Expected CREATE on globalSettings to require MANAGE_GLOBAL_SETTINGS");

    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupEntityAPIPrivilege(DELETE, "globalSettings")),
        new DisjunctivePrivilegeGroup(
            List.of(
                new ConjunctivePrivilegeGroup(
                    List.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS.getType())))),
        "Expected DELETE on globalSettings to require MANAGE_GLOBAL_SETTINGS");

    assertEquals(
        AuthUtil.buildDisjunctivePrivilegeGroup(
            AuthUtil.lookupEntityAPIPrivilege(MANAGE, "globalSettings")),
        new DisjunctivePrivilegeGroup(
            List.of(
                new ConjunctivePrivilegeGroup(
                    List.of(PoliciesConfig.MANAGE_GLOBAL_SETTINGS.getType())))),
        "Expected MANAGE on globalSettings to require MANAGE_GLOBAL_SETTINGS for update and delete");

    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(READ, "globalSettings")
            .contains(Conjunctive.of(PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE)),
        "Expected READ on globalSettings to retain standard entity read privileges");
    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.EXISTS, "globalSettings")
            .contains(
                Conjunctive.of(
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.EXISTS).get(0).get(0))),
        "Expected EXISTS on globalSettings to retain standard entity exists privileges");
  }

  @Test
  public void testGlobalSettingsRestApiAuthorization() {
    final String actorEditOnly = "urn:li:corpuser:globalSettingsEdit";
    final String actorManage = "urn:li:corpuser:globalSettingsManage";
    final String actorView = "urn:li:corpuser:globalSettingsView";

    Authorizer mockAuthorizer =
        mockAuthorizer(
            Map.of(
                actorEditOnly, Map.of("EDIT_ENTITY", Set.of(TEST_GLOBAL_SETTINGS_URN)),
                actorManage,
                    Map.of(
                        PoliciesConfig.MANAGE_GLOBAL_SETTINGS.getType(),
                        Set.of(TEST_GLOBAL_SETTINGS_URN)),
                actorView, Map.of("VIEW_ENTITY_PAGE", Set.of(TEST_GLOBAL_SETTINGS_URN))));

    Authentication authEditOnly =
        new Authentication(new Actor(ActorType.USER, "globalSettingsEdit"), "");
    Authentication authManage =
        new Authentication(new Actor(ActorType.USER, "globalSettingsManage"), "");
    Authentication authView =
        new Authentication(new Actor(ActorType.USER, "globalSettingsView"), "");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(authEditOnly, mockAuthorizer),
            ENTITY,
            List.of(Pair.of(ChangeType.UPSERT, TEST_GLOBAL_SETTINGS_URN))),
        Map.of(Pair.of(ChangeType.UPSERT, TEST_GLOBAL_SETTINGS_URN), 403),
        "EDIT_ENTITY alone must not authorize globalSettings mutation");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(authEditOnly, mockAuthorizer),
            ENTITY,
            List.of(Pair.of(ChangeType.CREATE_ENTITY, TEST_GLOBAL_SETTINGS_URN))),
        Map.of(Pair.of(ChangeType.CREATE_ENTITY, TEST_GLOBAL_SETTINGS_URN), 403),
        "EDIT_ENTITY alone must not authorize globalSettings create");

    assertEquals(
        AuthUtil.isAPIAuthorizedUrns(
            TestAuthSession.from(authManage, mockAuthorizer),
            ENTITY,
            List.of(
                Pair.of(ChangeType.UPSERT, TEST_GLOBAL_SETTINGS_URN),
                Pair.of(ChangeType.DELETE, TEST_GLOBAL_SETTINGS_URN),
                Pair.of(ChangeType.CREATE_ENTITY, TEST_GLOBAL_SETTINGS_URN))),
        Map.of(
            Pair.of(ChangeType.UPSERT, TEST_GLOBAL_SETTINGS_URN), 200,
            Pair.of(ChangeType.DELETE, TEST_GLOBAL_SETTINGS_URN), 200,
            Pair.of(ChangeType.CREATE_ENTITY, TEST_GLOBAL_SETTINGS_URN), 200),
        "MANAGE_GLOBAL_SETTINGS should authorize globalSettings mutations");

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            TestAuthSession.from(authView, mockAuthorizer),
            READ,
            List.of(TEST_GLOBAL_SETTINGS_URN)),
        "VIEW_ENTITY_PAGE should authorize globalSettings read");

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrns(
            TestAuthSession.from(authManage, mockAuthorizer),
            READ,
            List.of(TEST_GLOBAL_SETTINGS_URN)),
        "MANAGE_GLOBAL_SETTINGS should authorize globalSettings read as an alternative");
  }

  @Test
  public void testIsAPIAuthorizedEntityUrnsWithSubResources() {
    // Create some tag entities for subresources
    final Urn TEST_SUB_ENTITY_1 = UrnUtils.getUrn("urn:li:tag:tag1");
    final Urn TEST_SUB_ENTITY_2 = UrnUtils.getUrn("urn:li:tag:tag2");
    final Urn TEST_SUB_ENTITY_3 = UrnUtils.getUrn("urn:li:tag:tag3");

    Authorizer mockAuthorizer =
        mockAuthorizer(
            Map.of(
                TEST_AUTH_A.getActor().toUrnStr(),
                Map.of(
                    "EDIT_ENTITY",
                        Set.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2),
                    "VIEW_ENTITY_PAGE", Set.of(TEST_ENTITY_3, TEST_SUB_ENTITY_3),
                    "VIEW_ENTITY",
                        Set.of(
                            TEST_ENTITY_1,
                            TEST_ENTITY_2,
                            TEST_ENTITY_3,
                            TEST_SUB_ENTITY_1,
                            TEST_SUB_ENTITY_2,
                            TEST_SUB_ENTITY_3)),
                TEST_AUTH_B.getActor().toUrnStr(),
                Map.of(
                    "VIEW_ENTITY_PAGE", Set.of(TEST_ENTITY_1, TEST_ENTITY_3, TEST_SUB_ENTITY_1),
                    "VIEW_ENTITY", Set.of(TEST_ENTITY_1, TEST_ENTITY_3, TEST_SUB_ENTITY_1))));

    // Test User A - should have read access to all main entities and subresources
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2, TEST_SUB_ENTITY_3)),
        "Expected User A to have read access to all entities and subresources");

    // Test User A - should have update access to entities 1 & 2 and subresources 1 & 2, but not
    // entity 3 or subresource 3
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            UPDATE,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2, TEST_SUB_ENTITY_3)),
        "Expected User A to be denied update access due to entity 3 and subresource 3 restrictions");

    // Test User A - should have update access when excluding restricted entities/subresources
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            UPDATE,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2)),
        "Expected User A to have update access to entities 1 & 2 and subresources 1 & 2");

    // Test User B - should have limited read access
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2, TEST_SUB_ENTITY_3)),
        "Expected User B to be denied read access due to entity 2 and subresource 2 & 3 restrictions");

    // Test User B - should have read access to allowed entities and subresources only
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_3),
            List.of(TEST_SUB_ENTITY_1)),
        "Expected User B to have read access to allowed entities and subresources");

    // Test User B - should be denied update access to all entities and subresources
    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            UPDATE,
            List.of(TEST_ENTITY_1),
            List.of(TEST_SUB_ENTITY_1)),
        "Expected User B to be denied update access to all entities and subresources");

    // Test with empty subresources - should work like the regular method
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3),
            List.of()),
        "Expected method to work with empty subresources list for User A");

    assertFalse(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(TEST_ENTITY_1, TEST_ENTITY_2, TEST_ENTITY_3),
            List.of()),
        "Expected method to work with empty subresources list for User B (denied due to entity 2)");

    // Test with empty main resources but with subresources
    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_A, mockAuthorizer),
            READ,
            List.of(),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2, TEST_SUB_ENTITY_3)),
        "Expected User A to have read access to subresources only");

    assertTrue(
        AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
            TestAuthSession.from(TEST_AUTH_B, mockAuthorizer),
            READ,
            List.of(),
            List.of(TEST_SUB_ENTITY_1, TEST_SUB_ENTITY_2, TEST_SUB_ENTITY_3)),
        "Expected User B to be allowed access to subresources 2 & 3");
  }

  private Authorizer mockAuthorizer(Map<String, Map<String, Set<Urn>>> allowActorPrivUrn) {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any()))
        .thenAnswer(
            args -> {
              AuthorizationRequest req = args.getArgument(0);
              String actorUrn = req.getActorUrn();
              String priv = req.getPrivilege();

              if (!allowActorPrivUrn.containsKey(actorUrn)) {
                return new AuthorizationResult(
                    req, AuthorizationResult.Type.DENY, String.format("Actor %s denied", actorUrn));
              }

              Map<String, Set<Urn>> privMap = allowActorPrivUrn.get(actorUrn);
              if (!privMap.containsKey(priv)) {
                return new AuthorizationResult(
                    req, AuthorizationResult.Type.DENY, String.format("Privilege %s denied", priv));
              }

              if (req.getResourceSpec().isPresent()) {
                Urn entityUrn = UrnUtils.getUrn(req.getResourceSpec().get().getEntity());
                Set<Urn> resources = privMap.get(priv);
                if (!resources.contains(entityUrn)) {
                  return new AuthorizationResult(
                      req,
                      AuthorizationResult.Type.DENY,
                      String.format("Entity %s denied", entityUrn));
                }
              }

              return new AuthorizationResult(req, AuthorizationResult.Type.ALLOW, "Allowed");
            });
    return authorizer;
  }
}
