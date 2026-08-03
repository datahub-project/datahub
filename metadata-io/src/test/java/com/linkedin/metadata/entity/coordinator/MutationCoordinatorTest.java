package com.linkedin.metadata.entity.coordinator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class MutationCoordinatorTest {

  private static final ConflictKey KEY_A = ConflictKey.of("a", "id-a");
  private static final ConflictKey KEY_B = ConflictKey.of("b", "id-b");
  private static final String LOCK_A = "a/id-a";
  private static final String LOCK_B = "b/id-b";

  private static CoordinatedIngestConfiguration config() {
    // (maxPlanExpansions, maxMutationCount, lockLeaseSeconds, lockAcquireTimeoutSeconds,
    // lockProvider)
    return CoordinatedIngestConfiguration.builder()
        .maxPlanExpansions(2)
        .maxMutationCount(1000)
        .lockLeaseSeconds(30L)
        .lockAcquireTimeoutSeconds(2L)
        .lockProvider("hazelcast")
        .build();
  }

  private static MutationPlan planWith(ConflictKey... keys) {
    SortedSet<ConflictKey> conflictKeys = new TreeSet<>();
    for (ConflictKey key : keys) {
      conflictKeys.add(key);
    }
    return new MutationPlan(conflictKeys);
  }

  @Test
  public void nullProviderCommitsWithoutLocking() throws Exception {
    @SuppressWarnings("unchecked")
    CoordinatedCommit<String> commit = mock(CoordinatedCommit.class);
    when(commit.commitUnderLock(any())).thenReturn("ok");

    MutationCoordinator coordinator = new MutationCoordinator(null, config(), null);
    String result = coordinator.execute(planWith(KEY_A, KEY_B), commit);

    assertEquals(result, "ok");
    verify(commit, times(1)).commitUnderLock(any());
  }

  @Test
  public void happyPathLocksInSortedOrderAndUnlocksInReverse() throws Exception {
    CoordinationLockProvider lockProvider = mock(CoordinationLockProvider.class);
    when(lockProvider.tryLock(anyString(), anyLong(), anyLong())).thenReturn(true);

    @SuppressWarnings("unchecked")
    CoordinatedCommit<String> commit = mock(CoordinatedCommit.class);
    when(commit.commitUnderLock(any())).thenReturn("ok");

    MutationCoordinator coordinator = new MutationCoordinator(lockProvider, config(), null);
    String result = coordinator.execute(planWith(KEY_A, KEY_B), commit);

    assertEquals(result, "ok");
    verify(commit, times(1)).commitUnderLock(any());

    InOrder inOrder = inOrder(lockProvider);
    inOrder.verify(lockProvider).tryLock(eq(LOCK_A), anyLong(), anyLong());
    inOrder.verify(lockProvider).tryLock(eq(LOCK_B), anyLong(), anyLong());
    inOrder.verify(lockProvider).unlock(LOCK_B);
    inOrder.verify(lockProvider).unlock(LOCK_A);
  }

  @Test
  public void lockTimeoutStillCommitsBestEffortAndRecordsTimeout() throws Exception {
    CoordinationLockProvider lockProvider = mock(CoordinationLockProvider.class);
    when(lockProvider.tryLock(eq(LOCK_A), anyLong(), anyLong())).thenReturn(true);
    when(lockProvider.tryLock(eq(LOCK_B), anyLong(), anyLong())).thenReturn(false);

    MetricUtils metricUtils = mock(MetricUtils.class);
    @SuppressWarnings("unchecked")
    CoordinatedCommit<String> commit = mock(CoordinatedCommit.class);
    when(commit.commitUnderLock(any())).thenReturn("ok");

    MutationCoordinator coordinator = new MutationCoordinator(lockProvider, config(), metricUtils);
    String result = coordinator.execute(planWith(KEY_A, KEY_B), commit);

    assertEquals(result, "ok");
    // Commit runs despite the missed lock (DB commit is authoritative).
    verify(commit, times(1)).commitUnderLock(any());
    verify(metricUtils).incrementMicrometer(eq(MutationCoordinator.METRIC_LOCK_TIMEOUTS), eq(1.0d));
    // The timed-out key is never held, so it is never unlocked; the acquired one is released.
    verify(lockProvider, never()).unlock(LOCK_B);
    verify(lockProvider).unlock(LOCK_A);
  }

  @Test
  public void localLockProviderSerializesSameKey() throws Exception {
    final LocalLockProvider provider = new LocalLockProvider();
    final String key = "domain/id";

    assertTrue(provider.tryLock(key, 1_000L, 0L), "first acquire should succeed");

    // While thread A holds the key, a second thread must fail a short-wait acquire on the same key.
    final AtomicBoolean secondAcquired = new AtomicBoolean(true);
    final CountDownLatch done = new CountDownLatch(1);
    final Thread contender =
        new Thread(
            () -> {
              secondAcquired.set(provider.tryLock(key, 100L, 0L));
              if (!secondAcquired.get()) {
                done.countDown();
              } else {
                provider.unlock(key);
                done.countDown();
              }
            });
    contender.start();
    assertTrue(done.await(5, TimeUnit.SECONDS), "contender thread should finish");
    assertFalse(secondAcquired.get(), "second thread must not acquire a held key");

    // After release, the key is acquirable again.
    provider.unlock(key);
    assertTrue(provider.tryLock(key, 1_000L, 0L), "acquire after release should succeed");
    provider.unlock(key);
  }
}
