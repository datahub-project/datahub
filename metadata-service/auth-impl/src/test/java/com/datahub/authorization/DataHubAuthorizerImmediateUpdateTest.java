package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for immediate policy updates: synchronous first load, key reads, the recent-changes list,
 * and ordered, joined jobs.
 */
public class DataHubAuthorizerImmediateUpdateTest {

  /**
   * A primary store plus a search index that can lag behind it. A full load lists the indexed URNs
   * and reads their content from the store, as the search-based loader does.
   */
  private static final class FakeLoader implements PolicyLoader {
    final Map<Urn, PolicyFetcher.Policy> store = new ConcurrentHashMap<>();
    final Set<Urn> indexed = ConcurrentHashMap.newKeySet();
    // What a full load returns for a URN instead of the stored value, like a stale cache.
    final Map<Urn, PolicyFetcher.Policy> staleListing = new ConcurrentHashMap<>();
    final AtomicInteger fullLoads = new AtomicInteger();
    final AtomicInteger keyReads = new AtomicInteger();
    volatile int failuresLeft = 0;
    volatile int keyReadFailuresLeft = 0;
    volatile CountDownLatch entered;
    volatile CountDownLatch release;

    void add(PolicyFetcher.Policy policy, boolean indexNow) {
      store.put(policy.getUrn(), policy);
      if (indexNow) {
        indexed.add(policy.getUrn());
      }
    }

    @Override
    public List<PolicyFetcher.Policy> loadAll() {
      fullLoads.incrementAndGet();
      if (entered != null) {
        entered.countDown();
      }
      if (release != null) {
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (failuresLeft > 0) {
        failuresLeft--;
        throw new IllegalStateException("simulated search error");
      }
      List<PolicyFetcher.Policy> result = new ArrayList<>();
      for (Urn urn : indexed) {
        if (staleListing.containsKey(urn)) {
          result.add(staleListing.get(urn));
        } else if (store.containsKey(urn)) {
          result.add(store.get(urn));
        }
      }
      return result;
    }

    @Override
    public Map<Urn, PolicyFetcher.Policy> loadByUrns(Set<Urn> urns) {
      keyReads.incrementAndGet();
      if (release != null) {
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (keyReadFailuresLeft > 0) {
        keyReadFailuresLeft--;
        throw new IllegalStateException("simulated database error");
      }
      Map<Urn, PolicyFetcher.Policy> result = new HashMap<>();
      for (Urn urn : urns) {
        if (store.containsKey(urn)) {
          result.put(urn, store.get(urn));
        }
      }
      return result;
    }
  }

  private static Urn urn(String id) {
    return UrnUtils.getUrn("urn:li:dataHubPolicy:" + id);
  }

  private static PolicyFetcher.Policy policy(String id, Long lastUpdated) {
    return policy(id, lastUpdated, id);
  }

  /** A policy stored under {@code urnId} and shown as {@code name}. */
  private static PolicyFetcher.Policy policy(String name, Long lastUpdated, String urnId) {
    DataHubPolicyInfo info =
        new DataHubPolicyInfo()
            .setDisplayName(name)
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(List.of("EDIT_ENTITY")));
    if (lastUpdated != null) {
      info.setLastUpdatedTimestamp(lastUpdated);
    }
    return new PolicyFetcher.Policy(urn(urnId), info);
  }

  private static DataHubAuthorizer authorizer(PolicyLoader loader, int refreshSeconds) {
    return authorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(), loader, refreshSeconds);
  }

  private static DataHubAuthorizer authorizer(
      OperationContext context, PolicyLoader loader, int refreshSeconds) {
    return new DataHubAuthorizer(
        context,
        mock(EntityClient.class),
        mock(GroupService.class),
        10,
        refreshSeconds,
        DataHubAuthorizer.AuthorizationMode.DEFAULT,
        1000,
        loader);
  }

  private static List<String> allNames(DataHubAuthorizer authorizer) {
    List<String> names = new ArrayList<>();
    for (DataHubPolicyInfo info :
        authorizer.currentSnapshot().getOrDefault(DataHubAuthorizer.ALL, List.of())) {
      names.add(info.getDisplayName());
    }
    return names;
  }

  private static void waitFor(java.util.function.BooleanSupplier condition) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
  }

  @Test
  public void testInitialLoadIsSynchronous() {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 120);

    assertTrue(authorizer.initialLoad(1, 0));

    assertEquals(allNames(authorizer), List.of("p1"));
    assertTrue(authorizer.isImmediateUpdateEnabled());
  }

  @Test
  public void testUpdateReadsOnlyTheChangedPolicyByKey() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    // Committed, but the search index does not list it yet.
    loader.add(policy("p2", 2L), false);
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p2", "p1"));
    assertEquals(loader.fullLoads.get(), 1, "an update must not run a full load");
  }

  @Test
  public void testUpdateRemovesADeletedPolicy() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    loader.add(policy("p2", 2L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    loader.store.remove(urn("p2"));
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testFullLoadKeepsARecentChangeTheIndexDoesNotListYet() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    loader.add(policy("p2", 2L), false);
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);
    // A timer reload while the index still lags must not drop p2.
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p2", "p1"));
  }

  @Test
  public void testFullLoadDoesNotOverwriteAFreshKeyRead() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    // p1 is updated; the full load still returns the old value (for example from a cache).
    loader.staleListing.put(urn("p1"), loader.store.get(urn("p1")));
    loader.add(policy("p1-new", 2L, "p1"), true);
    authorizer.requestUpdate(List.of(urn("p1"))).get(10, TimeUnit.SECONDS);
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p1-new"));
  }

  @Test
  public void testFullLoadDropsADeletedPolicyTheIndexStillLists() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    loader.add(policy("p2", 2L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    // Deleted, but the full load still returns it.
    loader.staleListing.put(urn("p2"), loader.store.remove(urn("p2")));
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testJoinedFullLoadReadsItsUrnsEvenWithAZeroWindow() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.setRecentChangeWindow(Duration.ZERO);
    authorizer.initialLoad(1, 0);
    loader.entered = new CountDownLatch(1);
    loader.release = new CountDownLatch(1);
    CompletableFuture<Void> running = authorizer.requestLoad();
    assertTrue(loader.entered.await(10, TimeUnit.SECONDS));

    // Queued behind the running load: an update and a full load merge into one job.
    loader.add(policy("p2", 2L), false);
    CompletableFuture<Void> update = authorizer.requestUpdate(List.of(urn("p2")));
    CompletableFuture<Void> load = authorizer.requestLoad();
    loader.release.countDown();
    running.get(10, TimeUnit.SECONDS);
    update.get(10, TimeUnit.SECONDS);
    load.get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p2", "p1"));
  }

  @Test
  public void testRecentChangeExpires() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.setRecentChangeWindow(Duration.ofMillis(50));
    authorizer.initialLoad(1, 0);

    loader.add(policy("p2", 2L), false);
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);
    Thread.sleep(100);
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    // After the window, the cache follows the index again.
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testUpdateBeforeTheFirstLoadLoadsEverything() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    loader.add(policy("p2", 2L), false);
    DataHubAuthorizer authorizer = authorizer(loader, 0);

    // No initial load: a key read alone would leave the cache with only p2.
    authorizer.requestUpdate(List.of(urn("p2"))).get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p2", "p1"));
  }

  @Test
  public void testRequestsDuringARunningJobJoinOneLaterJob() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    loader.entered = new CountDownLatch(1);
    loader.release = new CountDownLatch(1);

    CompletableFuture<Void> first = authorizer.requestLoad();
    assertTrue(loader.entered.await(10, TimeUnit.SECONDS), "first load did not start");

    // The first load is running. These requests may follow commits it did not see, so they must
    // get a later job, and they should all share that one job.
    loader.add(policy("p1", 1L), true);
    loader.add(policy("p2", 2L), false);
    List<CompletableFuture<Void>> later = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      later.add(authorizer.requestLoad());
    }
    later.add(authorizer.requestUpdate(List.of(urn("p2"))));

    loader.release.countDown();
    first.get(10, TimeUnit.SECONDS);
    for (CompletableFuture<Void> f : later) {
      f.get(10, TimeUnit.SECONDS);
    }

    assertEquals(loader.fullLoads.get(), 2);
    // The joined full load also read the waiting URN.
    assertEquals(allNames(authorizer), List.of("p2", "p1"));
  }

  @Test
  public void testFailedLoadKeepsThePreviousPolicies() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    loader.store.clear();
    loader.failuresLeft = 1;
    CompletableFuture<Void> failed = authorizer.requestLoad();

    assertThrows(ExecutionException.class, () -> failed.get(10, TimeUnit.SECONDS));
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testInitialLoadRetriesThenContinuesWithoutPolicies() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    loader.failuresLeft = 10;
    DataHubAuthorizer authorizer = authorizer(loader, 120);

    assertFalse(authorizer.initialLoad(3, 0));
    assertEquals(loader.fullLoads.get(), 3);
    assertEquals(allNames(authorizer), List.of());

    // Search recovers; the next requested load succeeds.
    loader.failuresLeft = 0;
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testFailedStartupRetriesBeforeTheRefreshInterval() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    loader.failuresLeft = 2;
    DataHubAuthorizer authorizer = authorizer(loader, 120);
    authorizer.setFirstLoadRetryMillis(50);

    assertFalse(authorizer.initialLoad(1, 0));

    // No request and no timer tick: the retry after a failed first load loads p1.
    waitFor(() -> !allNames(authorizer).isEmpty());
    assertEquals(allNames(authorizer), List.of("p1"));
    int calls = loader.fullLoads.get();
    Thread.sleep(300);
    assertEquals(loader.fullLoads.get(), calls, "retries stop after the first success");
  }

  @Test
  public void testFailedLoadsKeepOneRetryChain() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.failuresLeft = Integer.MAX_VALUE;
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.setFirstLoadRetryMillis(100);

    assertFalse(authorizer.initialLoad(1, 0));
    // More failed loads from other paths while search is down.
    for (int i = 0; i < 4; i++) {
      try {
        authorizer.requestLoad().get(10, TimeUnit.SECONDS);
      } catch (ExecutionException expected) {
        // Search is down.
      }
    }
    int before = loader.fullLoads.get();
    Thread.sleep(1_000);

    // One chain retries about 10 times per second; five chains would retry about 50 times.
    int retries = loader.fullLoads.get() - before;
    assertTrue(retries <= 15, "retries in 1 s: " + retries);
    loader.failuresLeft = 0;
  }

  @Test
  public void testUrnBreaksTimestampTies() {
    FakeLoader loader = new FakeLoader();
    loader.add(policy("b", 5L), true);
    loader.add(policy("a", 5L), true);
    DataHubAuthorizer authorizer = authorizer(loader, 0);

    authorizer.initialLoad(1, 0);

    assertEquals(allNames(authorizer), List.of("a", "b"));
  }

  @Test
  public void testInvalidateCacheRequestsALoad() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.add(policy("p1", 1L), true);

    authorizer.invalidateCache();
    // A request made after invalidateCache joins or follows its load, so it sees p1 either way.
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testTimerLoadsWithoutARequest() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 1);
    authorizer.initialLoad(1, 0);
    loader.add(policy("p1", 1L), true);

    // No request and no invalidateCache: only the 1 s timer can load p1.
    waitFor(() -> !allNames(authorizer).isEmpty());
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testTimerSurvivesAnError() throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getMetricUtils()).thenReturn(Optional.of(metrics));
    // Each timer tick requests a load, and that request throws an Error.
    doThrow(new AssertionError("simulated error"))
        .when(metrics)
        .increment(DataHubAuthorizer.class, "policyCache.load.requested", 1);
    authorizer(context, new FakeLoader(), 1);

    // A scheduled task that throws is never run again, so a second tick proves the timer lives.
    verify(metrics, timeout(10_000).atLeast(2))
        .increment(DataHubAuthorizer.class, "policyCache.load.requested", 1);
  }

  @Test
  public void testTimerReportsAStuckLoad() throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getMetricUtils()).thenReturn(Optional.of(metrics));
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(context, loader, 1);
    authorizer.initialLoad(1, 0);
    loader.entered = new CountDownLatch(1);
    loader.release = new CountDownLatch(1);
    try {
      // The timer queues a load that hangs; its next ticks still run and report it.
      verify(metrics, timeout(10_000))
          .increment(DataHubAuthorizer.class, "policyCache.load.stuck", 1);
    } finally {
      loader.release.countDown();
    }
  }

  @Test
  public void testLoadAndUpdateMetrics() throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getMetricUtils()).thenReturn(Optional.of(metrics));
    FakeLoader loader = new FakeLoader();
    loader.add(policy("p1", 1L), true);
    DataHubAuthorizer authorizer = authorizer(context, loader, 0);

    assertTrue(authorizer.initialLoad(1, 0));
    // A job completes before its metrics are recorded, so wait for them.
    verify(metrics, timeout(5_000))
        .increment(DataHubAuthorizer.class, "policyCache.load.requested", 1);
    verify(metrics, timeout(5_000)).increment(DataHubAuthorizer.class, "policyCache.load.run", 1);
    verify(metrics, timeout(5_000))
        .setGaugeValue(DataHubAuthorizer.class, "policyCache.policies", 1);
    verify(metrics, timeout(5_000))
        .time(eq(DataHubAuthorizer.class.getName() + ".policyCache.load.duration"), anyLong());

    authorizer.requestUpdate(List.of(urn("p1"))).get(10, TimeUnit.SECONDS);
    verify(metrics, timeout(5_000)).increment(DataHubAuthorizer.class, "policyCache.update.run", 1);
    verify(metrics, timeout(5_000))
        .time(eq(DataHubAuthorizer.class.getName() + ".policyCache.update.duration"), anyLong());
    verify(metrics, never())
        .increment(eq(DataHubAuthorizer.class), eq("policyCache.load.failed"), anyDouble());

    loader.failuresLeft = 1;
    try {
      authorizer.requestLoad().get(10, TimeUnit.SECONDS);
      fail("the load must fail");
    } catch (ExecutionException expected) {
      // The simulated search error.
    }
    verify(metrics, timeout(5_000).atLeastOnce())
        .increment(DataHubAuthorizer.class, "policyCache.load.failed", 1);
  }

  @Test
  public void testWithoutLoaderImmediateUpdatesAreOff() throws Exception {
    DataHubAuthorizer authorizer = authorizer(null, 0);
    assertFalse(authorizer.isImmediateUpdateEnabled());
    // Requests complete at once and do nothing.
    authorizer.requestUpdate(List.of(urn("p1"))).get(1, TimeUnit.SECONDS);
  }

  @Test
  public void testPolicyChangeWaitsForTheKeyRead() {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.add(policy("p1", 1L), false);

    authorizer.onPolicyChange(Set.of(urn("p1")));

    // No extra wait: the call returns only after a key read that started after it has finished.
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testPolicyChangeStopsWaitingAfterTheLimit() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    authorizer.setPolicyChangeWaitMillis(50);
    loader.add(policy("p1", 1L), false);
    loader.release = new CountDownLatch(1);

    long start = System.nanoTime();
    authorizer.onPolicyChange(Set.of(urn("p1")));
    long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(waitedMillis < 5_000, "the wait must stop at the limit, waited " + waitedMillis);
    loader.release.countDown();
    // The slow read still completes after the caller stops waiting.
    authorizer.requestUpdate(Set.of(urn("p1"))).get(10, TimeUnit.SECONDS);
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testInterruptStopsTheWaitWithoutKeepingTheInterrupt() {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.release = new CountDownLatch(1);

    Thread.currentThread().interrupt();
    try {
      authorizer.onPolicyChange(Set.of(urn("p1")));
      // The committed write's change log is sent next on this thread; it must not see an interrupt.
      assertFalse(Thread.interrupted());
    } finally {
      Thread.interrupted();
      loader.release.countDown();
    }
  }

  @Test
  public void testInvalidateCacheLoadsOnlyWithoutTheObserver() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    authorizer.setPolicyWritesObserved(true);
    authorizer.invalidateCache();
    // Any load that invalidateCache queued would run before this one on the single load thread.
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertEquals(loader.fullLoads.get(), 2, "initial load and the request above only");

    authorizer.setPolicyWritesObserved(false);
    authorizer.invalidateCache();
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertTrue(loader.fullLoads.get() >= 3);
  }

  @Test
  public void testFailedObservedReadRequestsOneMore() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.add(policy("p1", 1L), false);
    loader.keyReadFailuresLeft = 1;

    authorizer.onPolicyChange(Set.of(urn("p1")));

    // The failed read is followed by one more, so the change applies without the timer.
    waitFor(() -> !allNames(authorizer).isEmpty());
    assertEquals(allNames(authorizer), List.of("p1"));
    Thread.sleep(300);
    // The failed read and exactly one more: no retry loop, and no full load.
    assertEquals(loader.keyReads.get(), 2);
    assertEquals(loader.fullLoads.get(), 1);
  }

  @DataProvider
  public Object[][] noWaitValues() {
    return new Object[][] {{0L}, {-1L}};
  }

  @Test(dataProvider = "noWaitValues")
  public void testNoWaitRequestsAReadWithoutATimeout(long waitMillis) throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getMetricUtils()).thenReturn(Optional.of(metrics));
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            context,
            mock(EntityClient.class),
            mock(GroupService.class),
            10,
            0,
            DataHubAuthorizer.AuthorizationMode.DEFAULT,
            1000,
            loader);
    authorizer.initialLoad(1, 0);
    authorizer.setPolicyChangeWaitMillis(waitMillis);
    loader.add(policy("p1", 1L), false);
    loader.release = new CountDownLatch(1);

    long start = System.nanoTime();
    authorizer.onPolicyChange(Set.of(urn("p1")));
    long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    loader.release.countDown();

    assertTrue(waited < 5_000, "waited " + waited + " ms");
    // A negative wait is set to 0. Not waiting is not a timeout, so there is no timeout metric.
    verify(metrics, never())
        .increment(eq(DataHubAuthorizer.class), eq("policyCache.observer.timeout"), anyDouble());
    // The read still runs in the background.
    authorizer.requestUpdate(Set.of(urn("p1"))).get(10, TimeUnit.SECONDS);
    assertEquals(allNames(authorizer), List.of("p1"));
  }
}
