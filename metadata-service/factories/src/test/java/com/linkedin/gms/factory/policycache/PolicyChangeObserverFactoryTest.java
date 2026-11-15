package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import org.springframework.beans.factory.ObjectProvider;
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
    return authorizer;
  }

  @Test
  public void testRegistersTheAuthorizerOnTheEntityService() {
    DataHubAuthorizer authorizer = authorizer(true);
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNotNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(provider(authorizer), provider(service), 5000L));

    verify(service).setPolicyChangeObserver(any());
    verify(authorizer).setPolicyWritesObserved(true);
    verify(authorizer).setPolicyChangeWaitMillis(5000L);
  }

  @Test
  public void testNothingWithoutImmediateUpdates() {
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(provider(authorizer(false)), provider(service), 5000L));

    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testNothingWithoutAnAuthorizer() {
    // system-update scans this package but builds no authorizer.
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(provider(null), provider(service), 5000L));

    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testNothingWithoutAnEntityServiceImpl() {
    DataHubAuthorizer authorizer = authorizer(true);

    assertNull(
        new PolicyChangeObserverFactory()
            .policyChangeObserver(
                provider(authorizer), provider(mock(EntityService.class)), 5000L));

    verify(authorizer, never()).setPolicyWritesObserved(true);
  }
}
