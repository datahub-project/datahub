package com.linkedin.gms.factory.auth;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
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
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.stream.Stream;
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

  private static DataHubAuthorizerFactory factory(boolean sqlSource) {
    DataHubAuthorizerFactory factory = new DataHubAuthorizerFactory();
    ReflectionTestUtils.setField(factory, "policyCacheRefreshIntervalSeconds", 0);
    ReflectionTestUtils.setField(factory, "policyCacheFetchSize", 1000);
    ReflectionTestUtils.setField(factory, "policiesEnabled", true);
    ReflectionTestUtils.setField(factory, "sqlSourceEnabled", sqlSource);
    ReflectionTestUtils.setField(factory, "policyChangeWaitMillis", 5000L);
    return factory;
  }

  private static DataHubAuthorizer build(
      DataHubAuthorizerFactory factory, AspectDao dao, EntityService<?> service) {
    return factory.dataHubAuthorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        mock(SystemEntityClient.class),
        mock(GroupService.class),
        provider(dao),
        provider(service));
  }

  @Test
  public void testSqlSourceRegistersTheObserverAndLoadsAtStartup() {
    AspectDao dao = mock(AspectDao.class);
    when(dao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(Stream.empty());
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    DataHubAuthorizer authorizer = build(factory(true), dao, service);

    assertTrue(authorizer.isSqlSourceEnabled());
    verify(service).setPolicyChangeObserver(any());
    verify(dao).streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME);
  }

  @Test
  public void testWithoutAnEntityStoreTheSearchSourceStays() {
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    DataHubAuthorizer authorizer = build(factory(true), null, service);

    assertFalse(authorizer.isSqlSourceEnabled());
    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testFlagOffKeepsTheSearchSource() {
    AspectDao dao = mock(AspectDao.class);
    EntityServiceImpl service = mock(EntityServiceImpl.class);

    DataHubAuthorizer authorizer = build(factory(false), dao, service);

    assertFalse(authorizer.isSqlSourceEnabled());
    verify(service, never()).setPolicyChangeObserver(any());
  }

  @Test
  public void testBroadcastTurnsOnTheSqlSourceAndSyncPolicyWrites() {
    AspectDao dao = mock(AspectDao.class);
    when(dao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(Stream.empty());
    EntityServiceImpl service = mock(EntityServiceImpl.class);
    DataHubAuthorizerFactory factory = factory(false);
    ReflectionTestUtils.setField(factory, "changeBroadcastEnabled", true);
    ReflectionTestUtils.setField(factory, "messagingTransport", "kafka");

    DataHubAuthorizer authorizer = build(factory, dao, service);

    assertTrue(authorizer.isSqlSourceEnabled());
    verify(service).setSyncPolicyWrites(true);
  }
}
