package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;

import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyChangeMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.wiring.WiringCoverage;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class PolicyChangeReceiverFactoryTest {

  private static final String FLAG = "authorization.defaultAuthorizer.changeBroadcast.enabled";

  private final TestHazelcastInstanceFactory hazelcastFactory = new TestHazelcastInstanceFactory(1);

  @AfterClass
  public void stopCluster() {
    hazelcastFactory.terminateAll();
  }

  private static DataHubAuthorizer authorizer() {
    DataHubAuthorizer authorizer = mock(DataHubAuthorizer.class);
    when(authorizer.isImmediateUpdateEnabled()).thenReturn(true);
    when(authorizer.getSystemOpContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    return authorizer;
  }

  @Test
  public void testGmsReceiverReadsSignalledPolicies() {
    DataHubAuthorizer authorizer = authorizer();
    HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance(new Config());
    Urn policy = UrnUtils.getUrn("urn:li:dataHubPolicy:signalled");

    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(PolicyChangeReceiverFactory.class)
        .withBean("dataHubAuthorizer", DataHubAuthorizer.class, () -> authorizer)
        .withBean("hazelcastInstance", HazelcastInstance.class, () -> hazelcast)
        .withPropertyValues(FLAG + "=true")
        .run(
            context -> {
              PolicyChangeMap.get(hazelcast).set(policy.toString(), System.currentTimeMillis());
              verify(authorizer, timeout(10_000))
                  .requestUpdate(argThat(urns -> urns != null && urns.contains(policy)));
            });
  }

  @Test
  public void testNoReceiverWithoutHazelcast() {
    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(PolicyChangeReceiverFactory.class)
        .withBean(
            "dataHubAuthorizer",
            DataHubAuthorizer.class,
            PolicyChangeReceiverFactoryTest::authorizer)
        .withPropertyValues(FLAG + "=true")
        .run(
            context ->
                assertNull(context.getBeanProvider(PolicyChangeReceiver.class).getIfAvailable()));
  }
}
