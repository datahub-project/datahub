package com.linkedin.gms.factory.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DataHubAuthorizerFactoryTest {

  private SystemEntityClient client;

  private DataHubAuthorizer build(boolean immediateUpdate, boolean broadcast) {
    DataHubAuthorizerFactory factory = new DataHubAuthorizerFactory();
    ReflectionTestUtils.setField(factory, "policyCacheRefreshIntervalSeconds", 0);
    ReflectionTestUtils.setField(factory, "policyCacheFetchSize", 1000);
    ReflectionTestUtils.setField(factory, "policiesEnabled", true);
    ReflectionTestUtils.setField(factory, "immediateUpdateEnabled", immediateUpdate);
    ReflectionTestUtils.setField(factory, "recentChangeWindowSeconds", 600L);
    ReflectionTestUtils.setField(factory, "changeBroadcastEnabled", broadcast);
    ReflectionTestUtils.setField(factory, "messagingTransport", "kafka");
    // An empty search result, so the startup load succeeds at once.
    client = mock(SystemEntityClient.class);
    try {
      when(client.scrollAcrossEntities(
              any(OperationContext.class), anyList(), any(), any(), any(), any(), anyList(), any()))
          .thenReturn(new ScrollResult().setNumEntities(0).setEntities(new SearchEntityArray()));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return factory.dataHubAuthorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        client,
        mock(GroupService.class));
  }

  @DataProvider
  public Object[][] flags() {
    return new Object[][] {
      {false, false, false}, {true, false, true}, {false, true, true}, {true, true, true}
    };
  }

  @Test(dataProvider = "flags")
  public void testImmediateUpdatesFollowEitherFlag(
      boolean immediateUpdate, boolean broadcast, boolean expected) throws Exception {
    DataHubAuthorizer authorizer = build(immediateUpdate, broadcast);
    if (expected) {
      assertTrue(authorizer.isImmediateUpdateEnabled());
      // The factory ran the synchronous startup load, which listed policies from search.
      verify(client, atLeastOnce())
          .scrollAcrossEntities(
              any(OperationContext.class), anyList(), any(), any(), any(), any(), anyList(), any());
    } else {
      assertFalse(authorizer.isImmediateUpdateEnabled());
      // Without immediate updates, no startup load runs (the search refresh timer is off here).
      verify(client, never())
          .scrollAcrossEntities(
              any(OperationContext.class), anyList(), any(), any(), any(), any(), anyList(), any());
    }
  }
}
