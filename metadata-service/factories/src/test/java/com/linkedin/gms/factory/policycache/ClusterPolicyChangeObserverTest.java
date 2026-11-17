package com.linkedin.gms.factory.policycache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.authorization.DataHubAuthorizer;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.IMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClusterPolicyChangeObserverTest {

  private static final Urn FIRST = UrnUtils.getUrn("urn:li:dataHubPolicy:first");
  private static final Urn SECOND = UrnUtils.getUrn("urn:li:dataHubPolicy:second");

  private DataHubAuthorizer authorizer;
  private IMap<String, Long> changes;
  private MetricUtils metricUtils;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    authorizer = mock(DataHubAuthorizer.class);
    changes = mock(IMap.class);
    when(changes.setAsync(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));
    metricUtils = mock(MetricUtils.class);
  }

  @Test
  public void testClientWriteSignalsEachPolicyBeforeWaitingLocally() {
    new ClusterPolicyChangeObserver(authorizer, changes, metricUtils)
        .onPolicyChange(Set.of(FIRST, SECOND), true);

    // Other members must not wait behind this member's local read.
    final InOrder order = inOrder(changes, authorizer);
    order.verify(changes, times(2)).setAsync(anyString(), anyLong());
    order.verify(authorizer).onPolicyChange(Set.of(FIRST, SECOND));
    verify(authorizer, never()).requestUpdate(any());
    verify(changes).setAsync(eq(FIRST.toString()), anyLong());
    verify(changes).setAsync(eq(SECOND.toString()), anyLong());
  }

  @Test
  public void testConsumerWriteDoesNotWaitAndIsSignalled() {
    new ClusterPolicyChangeObserver(authorizer, changes, metricUtils)
        .onPolicyChange(Set.of(FIRST), false);

    verify(authorizer).requestUpdate(Set.of(FIRST));
    verify(authorizer, never()).onPolicyChange(any());
    verify(changes).setAsync(eq(FIRST.toString()), anyLong());
  }

  @Test
  public void testWithoutBroadcastOnlyTheLocalCacheIsUpdated() {
    new ClusterPolicyChangeObserver(authorizer, null, metricUtils)
        .onPolicyChange(Set.of(FIRST), true);

    verify(authorizer).onPolicyChange(Set.of(FIRST));
  }

  @Test
  public void testHazelcastFailuresDoNotFailTheWrite() {
    when(changes.setAsync(eq(FIRST.toString()), anyLong()))
        .thenThrow(new HazelcastException("member is shutting down"));
    when(changes.setAsync(eq(SECOND.toString()), anyLong()))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("partition lost")));

    new ClusterPolicyChangeObserver(authorizer, changes, metricUtils)
        .onPolicyChange(Set.of(FIRST, SECOND), true);

    verify(authorizer).onPolicyChange(Set.of(FIRST, SECOND));
    verify(metricUtils, times(2))
        .increment(
            eq(ClusterPolicyChangeObserver.class), eq("policyChange.publish_failed"), anyDouble());
  }
}
