package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyChangeMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.PolicyChangeObserver;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.wiring.WiringCoverage;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

public class PolicyChangeObserverFactoryTest {

  @SuppressWarnings("unchecked")
  private static <T> ObjectProvider<T> provider(T value) {
    ObjectProvider<T> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(value);
    return provider;
  }

  private static DataHubAuthorizer authorizer(boolean immediateUpdate) {
    DataHubAuthorizer authorizer = mock(DataHubAuthorizer.class);
    when(authorizer.isImmediateUpdateEnabled()).thenReturn(immediateUpdate);
    when(authorizer.getSystemOpContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    return authorizer;
  }

  @Test
  public void testRegistersTheAuthorizerOnTheEntityService() {
    DataHubAuthorizer authorizer = authorizer(true);
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    // A real Spring context, so the bean method runs as it does in GMS.
    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(PolicyChangeObserverFactory.class)
        .withBean("dataHubAuthorizer", DataHubAuthorizer.class, () -> authorizer)
        .withBean("entityService", EntityService.class, () -> service)
        .withPropertyValues(
            "authorization.defaultAuthorizer.immediateUpdate.policyChangeWaitMillis=5000")
        .run(context -> assertNotNull(context.getBean(PolicyChangeObserver.class)));

    verify(service).setPolicyChangeObserver(any());
    verify(authorizer).setPolicyWritesObserved(true);
    verify(authorizer).setPolicyChangeWaitMillis(5000L);
  }

  @Test
  public void testNothingWithoutImmediateUpdates() {
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(
                provider(authorizer(false)), provider(service), provider(null), 5000L, false));

    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testNothingWithoutAnAuthorizer() {
    // system-update scans this package but builds no authorizer.
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(provider(null), provider(service), provider(null), 5000L, false));

    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testNothingWithoutAnEntityServiceImpl() {
    DataHubAuthorizer authorizer = authorizer(true);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(
                provider(authorizer),
                provider(mock(EntityService.class)),
                provider(null),
                5000L,
                false));

    verify(authorizer, never()).setPolicyWritesObserved(true);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBroadcastSignalsThroughTheHazelcastMap() {
    DataHubAuthorizer authorizer = authorizer(true);
    EntityServiceImpl service = mock(EntityServiceImpl.class);
    HazelcastInstance hazelcast = mock(HazelcastInstance.class);
    IMap<Object, Object> changes = mock(IMap.class);
    when(hazelcast.getMap(PolicyChangeMap.NAME)).thenReturn(changes);
    when(changes.setAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    Urn policy = UrnUtils.getUrn("urn:li:dataHubPolicy:signalled");

    PolicyChangeObserver observer =
        new PolicyChangeObserverFactory()
            .policyChangeObserver(
                provider(authorizer), provider(service), provider(hazelcast), 5000L, true);
    observer.onPolicyChange(Set.of(policy), false);

    verify(changes).setAsync(eq(policy.toString()), any());
  }

  @Test
  public void testBroadcastWithoutHazelcastStillUpdatesTheLocalCache() {
    DataHubAuthorizer authorizer = authorizer(true);
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNotNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(
                provider(authorizer), provider(service), provider(null), 5000L, true));

    verify(service).setPolicyChangeObserver(any());
  }
}
