package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.authorization.DataHubAuthorizer;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import java.util.UUID;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** An unhealthy cluster at startup degrades to the refresh timer instead of failing GMS. */
public class PolicyChangeReceiverTest {

  private DataHubAuthorizer authorizer;
  private IMap<String, Long> changes;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    authorizer = mock(DataHubAuthorizer.class);
    changes = mock(IMap.class);
  }

  @Test
  public void testListenerFailureDoesNotFailStartup() {
    when(changes.addEntryListener(any(MapListener.class), anyBoolean()))
        .thenThrow(new HazelcastException("cluster unavailable"));

    new PolicyChangeReceiver(authorizer, changes, null).start();

    verify(changes, never()).keySet();
    verify(authorizer, never()).requestLoad();
  }

  @Test
  public void testKeySetFailureKeepsTheListenerAndRequestsAFullLoad() {
    UUID id = UUID.randomUUID();
    when(changes.addEntryListener(any(MapListener.class), anyBoolean())).thenReturn(id);
    when(changes.keySet()).thenThrow(new HazelcastException("operation timed out"));

    PolicyChangeReceiver receiver = new PolicyChangeReceiver(authorizer, changes, null);
    receiver.start();

    verify(authorizer).requestLoad();
    receiver.close();
    verify(changes).removeEntryListener(id);
  }
}
