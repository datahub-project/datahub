package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyChangeMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.common.HazelcastSerializationFilter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Two GMS members: a policy change signalled on one is read by key on the other. */
public class PolicyChangeReceiverClusterTest {

  private static final long WAIT_MILLIS = 10_000;

  private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
  private HazelcastInstance member1;
  private HazelcastInstance member2;
  private DataHubAuthorizer authorizer2;
  private PolicyChangeReceiver receiver2;

  @BeforeClass
  public void startCluster() {
    member1 = factory.newHazelcastInstance(config());
    member2 = factory.newHazelcastInstance(config());
    assertEquals(member2.getCluster().getMembers().size(), 2);
  }

  @AfterClass
  public void stopCluster() {
    factory.terminateAll();
  }

  @BeforeMethod
  public void setUp() {
    PolicyChangeMap.get(member1).clear();
    authorizer2 = mock(DataHubAuthorizer.class);
    receiver2 = new PolicyChangeReceiver(authorizer2, PolicyChangeMap.get(member2), null);
  }

  @AfterMethod
  public void tearDown() {
    receiver2.close();
  }

  @Test
  public void testChangeOnOneMemberIsReadOnTheOther() {
    receiver2.start();
    Urn policy = UrnUtils.getUrn("urn:li:dataHubPolicy:changed-on-member-1");

    new ClusterPolicyChangeObserver(
            mock(DataHubAuthorizer.class), PolicyChangeMap.get(member1), null)
        .onPolicyChange(Set.of(policy), true);

    verify(authorizer2, timeout(WAIT_MILLIS)).requestUpdate(containing(policy));
  }

  @Test
  public void testMemberThatStartsLaterReadsEarlierChanges() {
    Urn policy = UrnUtils.getUrn("urn:li:dataHubPolicy:changed-before-start");
    PolicyChangeMap.get(member1).set(policy.toString(), System.currentTimeMillis());

    receiver2.start();

    verify(authorizer2, timeout(WAIT_MILLIS)).requestUpdate(containing(policy));
  }

  @Test
  public void testUnreadableKeyRequestsAFullLoad() {
    receiver2.start();

    PolicyChangeMap.get(member1).set("not-a-policy-urn", System.currentTimeMillis());

    verify(authorizer2, timeout(WAIT_MILLIS)).requestLoad();
  }

  private static Collection<Urn> containing(Urn urn) {
    return argThat(urns -> urns != null && urns.contains(urn));
  }

  private static Config config() {
    Config config = new Config();
    HazelcastSerializationFilter.apply(config, List.of(), null, null);
    config.addMapConfig(PolicyChangeMap.mapConfig(600));
    return config;
  }
}
