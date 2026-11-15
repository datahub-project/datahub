package com.linkedin.gms.factory.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class DataHubAuthorizerFactoryTest {

  @SuppressWarnings("unchecked")
  private static <T> ObjectProvider<T> provider(T value) {
    ObjectProvider<T> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(value);
    return provider;
  }

  private static DataHubAuthorizerFactory factory(boolean immediateUpdate) {
    DataHubAuthorizerFactory factory = new DataHubAuthorizerFactory();
    ReflectionTestUtils.setField(factory, "policyCacheRefreshIntervalSeconds", 0);
    ReflectionTestUtils.setField(factory, "policyCacheFetchSize", 1000);
    ReflectionTestUtils.setField(factory, "policiesEnabled", true);
    ReflectionTestUtils.setField(factory, "immediateUpdateEnabled", immediateUpdate);
    ReflectionTestUtils.setField(factory, "recentChangeWindowSeconds", 600L);
    ReflectionTestUtils.setField(factory, "policyChangeWaitMillis", 5000L);
    return factory;
  }

  private static DataHubAuthorizer build(
      DataHubAuthorizerFactory factory, EntityService<?> service) {
    return factory.dataHubAuthorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        mock(SystemEntityClient.class),
        mock(GroupService.class),
        provider(service));
  }

  @Test
  public void testImmediateUpdateRegistersTheObserver() {
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    DataHubAuthorizer authorizer = build(factory(true), service);

    assertTrue(authorizer.isImmediateUpdateEnabled());
    verify(service).setPolicyChangeObserver(any());
  }

  @Test
  public void testFlagOffKeepsTheSearchSource() {
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    DataHubAuthorizer authorizer = build(factory(false), service);

    assertFalse(authorizer.isImmediateUpdateEnabled());
    verify(service, never()).setPolicyChangeObserver(any());
  }
}
