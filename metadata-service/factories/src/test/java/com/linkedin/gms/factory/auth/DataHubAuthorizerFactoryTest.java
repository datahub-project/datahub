package com.linkedin.gms.factory.auth;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DataHubAuthorizerFactoryTest {

  private static DataHubAuthorizer build(boolean immediateUpdate, boolean broadcast) {
    DataHubAuthorizerFactory factory = new DataHubAuthorizerFactory();
    ReflectionTestUtils.setField(factory, "policyCacheRefreshIntervalSeconds", 0);
    ReflectionTestUtils.setField(factory, "policyCacheFetchSize", 1000);
    ReflectionTestUtils.setField(factory, "policiesEnabled", true);
    ReflectionTestUtils.setField(factory, "immediateUpdateEnabled", immediateUpdate);
    ReflectionTestUtils.setField(factory, "recentChangeWindowSeconds", 600L);
    ReflectionTestUtils.setField(factory, "changeBroadcastEnabled", broadcast);
    ReflectionTestUtils.setField(factory, "messagingTransport", "kafka");
    // The mocked client returns no search results, so the startup load fails fast; the flags are
    // what this test checks.
    return factory.dataHubAuthorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        mock(SystemEntityClient.class),
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
      boolean immediateUpdate, boolean broadcast, boolean expected) {
    DataHubAuthorizer authorizer = build(immediateUpdate, broadcast);
    if (expected) {
      assertTrue(authorizer.isImmediateUpdateEnabled());
    } else {
      assertFalse(authorizer.isImmediateUpdateEnabled());
    }
  }
}
