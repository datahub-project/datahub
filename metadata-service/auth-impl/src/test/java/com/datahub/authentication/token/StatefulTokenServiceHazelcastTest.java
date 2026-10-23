package com.datahub.authentication.token;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.UUID;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StatefulTokenServiceHazelcastTest {

  private static final String TEST_SIGNING_KEY = "datahub-test-signing-key-not-a-secret";
  private static final String TEST_SALTING_KEY = "datahub-test-salting-key-not-a-secret";

  private HazelcastInstance memberA;
  private HazelcastInstance memberB;
  private EntityService<?> entityService;
  private OperationContext readContext;

  @BeforeClass
  public void setUp() throws Exception {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    ConfigEntityRegistry registry =
        new ConfigEntityRegistry(
            StatefulTokenServiceHazelcastTest.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yaml"));
    readContext =
        TestOperationContexts.systemContextNoSearchAuthorization(registry)
            .withReadPreference(ReadPreference.READ);

    String cluster = "token-revocation-" + UUID.randomUUID();
    int port = 20000 + java.util.concurrent.ThreadLocalRandom.current().nextInt(20000);
    memberA = member(cluster, port);
    memberB = member(cluster, port);
    for (int i = 0; i < 50 && memberB.getCluster().getMembers().size() < 2; i++) {
      Thread.sleep(100);
    }
    assertEquals(memberB.getCluster().getMembers().size(), 2);

    entityService = mock(EntityService.class);
  }

  @BeforeMethod
  public void resetEntityService() {
    Mockito.reset(entityService);
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(new RollbackRunResult(List.of(), 0, List.of()));
  }

  @AfterClass
  public void tearDown() {
    if (memberA != null) {
      memberA.shutdown();
    }
    if (memberB != null) {
      memberB.shutdown();
    }
  }

  @Test
  public void generateOnOneMemberIsVisibleOnTheOtherWithoutADatabaseRead() throws Exception {
    StatefulTokenService serviceA = service(memberA);
    StatefulTokenService serviceB = service(memberB);
    Actor actor = new Actor(ActorType.USER, "datahub");

    String token =
        serviceA.generateAccessToken(
            readContext, TokenType.PERSONAL, actor, "cluster token", "desc", actor.toUrnStr());
    assertNotNull(token);

    TokenClaims claims = serviceB.validateAccessToken(token);
    assertEquals(claims.getActorId(), "datahub");
    verify(entityService, never()).exists(any(), any(Urn.class), anyBoolean());
  }

  @Test
  public void revokeOnOneMemberInvalidatesTheOther() throws Exception {
    StatefulTokenService serviceA = service(memberA);
    StatefulTokenService serviceB = service(memberB);
    Actor actor = new Actor(ActorType.USER, "datahub");
    String token =
        serviceA.generateAccessToken(
            readContext, TokenType.PERSONAL, actor, "revoked token", "desc", actor.toUrnStr());

    serviceB.validateAccessToken(token);
    String hash = serviceA.hash(token);
    serviceA.revokeAccessToken(readContext, hash);
    assertEquals(TokenRevocationMap.get(memberB).get(hash), Boolean.TRUE);

    assertThrows(TokenException.class, () -> serviceB.validateAccessToken(token));
  }

  @Test
  public void cacheMissLoadsExistenceFromPrimary() throws Exception {
    StatefulTokenService serviceA = service(memberA);
    StatefulTokenService serviceB = service(memberB);
    Actor actor = new Actor(ActorType.USER, "datahub");
    String token =
        serviceA.generateAccessToken(
            readContext, TokenType.PERSONAL, actor, "primary reload", "desc", actor.toUrnStr());
    TokenRevocationMap.get(memberA).remove(serviceA.hash(token));
    when(entityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);

    serviceB.validateAccessToken(token);

    ArgumentCaptor<OperationContext> context = ArgumentCaptor.forClass(OperationContext.class);
    verify(entityService).exists(context.capture(), any(Urn.class), eq(true));
    assertEquals(
        context.getValue().getPrimaryStorageContext().getReadPreference(), ReadPreference.PRIMARY);
  }

  private StatefulTokenService service(HazelcastInstance hazelcast) {
    return new StatefulTokenService(
        readContext, TEST_SIGNING_KEY, "HS256", null, entityService, TEST_SALTING_KEY, hazelcast);
  }

  private static HazelcastInstance member(String cluster, int port) {
    Config config = new Config();
    config.setClusterName(cluster);
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().setPort(port);
    config.getNetworkConfig().setPortAutoIncrement(true);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config
        .getNetworkConfig()
        .getJoin()
        .getTcpIpConfig()
        .setEnabled(true)
        .addMember("127.0.0.1:" + port);
    config.addMapConfig(TokenRevocationMap.mapConfig());
    return Hazelcast.newHazelcastInstance(config);
  }
}
