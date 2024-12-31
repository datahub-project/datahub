package com.datahub.authorization;

import static com.linkedin.metadata.Constants.REST_API_AUTHORIZATION_ENABLED_ENV;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
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
import com.linkedin.util.Pair;
import io.datahubproject.test.metadata.context.TestAuthSession;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.testng.SystemStub;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class AuthUtilTest {
  @SystemStub private EnvironmentVariables setEnvironment;

  @BeforeClass
  public void beforeAll() {
    setEnvironment.set(REST_API_AUTHORIZATION_ENABLED_ENV, "true");
  }

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

  @Test
  public void testSystemEnvInit() {
    assertEquals(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV), "true");
  }

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
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.UPDATE).get(0).get(0),
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.DELETE).get(0).get(0))),
        "Expected MANAGE to require both EDIT and DELETE");
  }

  @Test
  public void testEntityType() {
    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.MANAGE, "dataset")
            .contains(
                Conjunctive.of(
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.UPDATE).get(0).get(0),
                    API_PRIVILEGE_MAP.get(ENTITY).get(ApiOperation.DELETE).get(0).get(0))),
        "Expected MANAGE on dataset to require both EDIT and DELETE");

    assertTrue(
        AuthUtil.lookupEntityAPIPrivilege(ApiOperation.MANAGE, "dataHubPolicy")
            .contains(
                Conjunctive.of(
                    API_ENTITY_PRIVILEGE_MAP
                        .get("dataHubPolicy")
                        .get(ApiOperation.UPDATE)
                        .get(0)
                        .get(0))),
        "Expected MANAGE permission directly on dataHubPolicy entity");
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
