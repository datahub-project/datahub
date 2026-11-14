package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Tests for the SQL policy source: synchronous first load, ordered and joined loads. */
public class DataHubAuthorizerSqlSourceTest {

  /** A loader whose result can change between calls, and that can fail or block on demand. */
  private static final class FakeLoader implements PolicyLoader {
    final List<PolicyFetcher.Policy> policies = new CopyOnWriteArrayList<>();
    final AtomicInteger calls = new AtomicInteger();
    volatile int failuresLeft = 0;
    volatile int skipped = 0;
    volatile CountDownLatch entered;
    volatile CountDownLatch release;

    @Override
    public Result loadAll() {
      calls.incrementAndGet();
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
        throw new IllegalStateException("simulated database error");
      }
      return new Result(new ArrayList<>(policies), skipped);
    }
  }

  private static PolicyFetcher.Policy policy(String id, Long lastUpdated) {
    DataHubPolicyInfo info =
        new DataHubPolicyInfo()
            .setDisplayName(id)
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(List.of("EDIT_ENTITY")));
    if (lastUpdated != null) {
      info.setLastUpdatedTimestamp(lastUpdated);
    }
    return new PolicyFetcher.Policy(UrnUtils.getUrn("urn:li:dataHubPolicy:" + id), info);
  }

  private static DataHubAuthorizer authorizer(PolicyLoader loader, int refreshSeconds) {
    return new DataHubAuthorizer(
        TestOperationContexts.systemContextNoSearchAuthorization(),
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

  @Test
  public void testInitialLoadIsSynchronous() {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    DataHubAuthorizer authorizer = authorizer(loader, 120);

    assertTrue(authorizer.initialLoad(1, 0));

    assertEquals(allNames(authorizer), List.of("p1"));
    assertTrue(authorizer.isSqlSourceEnabled());
  }

  @Test
  public void testRequestedLoadReadsTheCurrentData() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    DataHubAuthorizer authorizer = authorizer(loader, 120);
    authorizer.initialLoad(1, 0);

    loader.policies.add(policy("p2", 2L));
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);

    assertEquals(allNames(authorizer), List.of("p2", "p1"));
  }

  @Test
  public void testRequestsDuringARunningLoadJoinOneLaterLoad() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    loader.entered = new CountDownLatch(1);
    loader.release = new CountDownLatch(1);

    CompletableFuture<Void> first = authorizer.requestLoad();
    assertTrue(loader.entered.await(10, TimeUnit.SECONDS), "first load did not start");

    // The first load is running. These requests may follow commits it did not see, so they must
    // get a later load, and they should all share that one load.
    loader.policies.add(policy("p1", 1L));
    List<CompletableFuture<Void>> later = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      later.add(authorizer.requestLoad());
    }

    loader.release.countDown();
    first.get(10, TimeUnit.SECONDS);
    for (CompletableFuture<Void> f : later) {
      f.get(10, TimeUnit.SECONDS);
    }

    assertEquals(loader.calls.get(), 2);
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testFailedLoadKeepsThePreviousPolicies() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);

    loader.policies.clear();
    loader.failuresLeft = 1;
    CompletableFuture<Void> failed = authorizer.requestLoad();

    assertThrows(ExecutionException.class, () -> failed.get(10, TimeUnit.SECONDS));
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testInitialLoadRetriesThenContinuesWithoutPolicies() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    loader.failuresLeft = 10;
    DataHubAuthorizer authorizer = authorizer(loader, 120);

    assertFalse(authorizer.initialLoad(3, 0));
    assertEquals(loader.calls.get(), 3);
    assertEquals(allNames(authorizer), List.of());

    // The database recovers; the next requested load succeeds.
    loader.failuresLeft = 0;
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testFailedStartupRetriesBeforeTheRefreshInterval() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    loader.failuresLeft = 2;
    DataHubAuthorizer authorizer = authorizer(loader, 120);
    authorizer.setFirstLoadRetryMillis(50);

    assertFalse(authorizer.initialLoad(1, 0));

    // No request and no timer tick: the retry after a failed first load loads p1.
    long deadline = System.currentTimeMillis() + 10_000;
    while (allNames(authorizer).isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(allNames(authorizer), List.of("p1"));
    int calls = loader.calls.get();
    Thread.sleep(300);
    assertEquals(loader.calls.get(), calls, "retries stop after the first success");
  }

  @Test
  public void testFailedLoadsKeepOneRetryChain() throws Exception {
    FakeLoader loader = new FakeLoader();
    loader.failuresLeft = Integer.MAX_VALUE;
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.setFirstLoadRetryMillis(100);

    assertFalse(authorizer.initialLoad(1, 0));
    // More failed loads from other paths while the database is down.
    for (int i = 0; i < 4; i++) {
      try {
        authorizer.requestLoad().get(10, TimeUnit.SECONDS);
      } catch (ExecutionException expected) {
        // The database is down.
      }
    }
    int before = loader.calls.get();
    Thread.sleep(1_000);

    // One chain retries about 10 times per second; five chains would retry about 50 times.
    int retries = loader.calls.get() - before;
    assertTrue(retries <= 15, "retries in 1 s: " + retries);
    loader.failuresLeft = 0;
  }

  @Test
  public void testUrnBreaksTimestampTies() {
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("b", 5L));
    loader.policies.add(policy("a", 5L));
    DataHubAuthorizer authorizer = authorizer(loader, 0);

    authorizer.initialLoad(1, 0);

    assertEquals(allNames(authorizer), List.of("a", "b"));
  }

  @Test
  public void testInvalidateCacheRequestsALoad() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.policies.add(policy("p1", 1L));

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
    loader.policies.add(policy("p1", 1L));

    // No request and no invalidateCache: only the 1 s timer can load p1.
    long deadline = System.currentTimeMillis() + 10_000;
    while (allNames(authorizer).isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testLoadMetrics() throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getMetricUtils()).thenReturn(Optional.of(metrics));
    FakeLoader loader = new FakeLoader();
    loader.policies.add(policy("p1", 1L));
    loader.skipped = 2;
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

    assertTrue(authorizer.initialLoad(1, 0));
    verify(metrics).increment(DataHubAuthorizer.class, "policyCache.load.requested", 1);
    verify(metrics).increment(DataHubAuthorizer.class, "policyCache.load.run", 1);
    verify(metrics).increment(DataHubAuthorizer.class, "policyCache.load.skipped", 2);
    verify(metrics).setGaugeValue(DataHubAuthorizer.class, "policyCache.policies", 1);
    verify(metrics)
        .time(eq(DataHubAuthorizer.class.getName() + ".policyCache.load.duration"), anyLong());
    verify(metrics, never())
        .increment(eq(DataHubAuthorizer.class), eq("policyCache.load.failed"), anyDouble());

    loader.failuresLeft = 1;
    try {
      authorizer.requestLoad().get(10, TimeUnit.SECONDS);
      fail("the load must fail");
    } catch (ExecutionException expected) {
      // The simulated database error.
    }
    verify(metrics, atLeastOnce()).increment(DataHubAuthorizer.class, "policyCache.load.failed", 1);
  }

  @Test
  public void testWithoutLoaderTheSqlSourceIsOff() {
    DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            mock(EntityClient.class),
            mock(GroupService.class),
            10,
            0,
            DataHubAuthorizer.AuthorizationMode.DEFAULT,
            1000,
            null);
    assertFalse(authorizer.isSqlSourceEnabled());
  }

  @Test
  public void testPolicyChangeWaitsForTheLoad() {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.policies.add(policy("p1", 1L));

    authorizer.onPolicyChange();

    // No extra wait: the call returns only after a load that started after it has finished.
    assertEquals(allNames(authorizer), List.of("p1"));
  }

  @Test
  public void testPolicyChangeStopsWaitingAfterTheLimit() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    authorizer.setPolicyChangeWaitMillis(50);
    loader.policies.add(policy("p1", 1L));
    loader.release = new CountDownLatch(1);

    long start = System.nanoTime();
    authorizer.onPolicyChange();
    long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(waitedMillis < 5_000, "the wait must stop at the limit, waited " + waitedMillis);
    loader.release.countDown();
    // The slow load still completes after the caller stops waiting.
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
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
      authorizer.onPolicyChange();
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
    assertEquals(loader.calls.get(), 2, "initial load and the request above only");

    authorizer.setPolicyWritesObserved(false);
    authorizer.invalidateCache();
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertTrue(loader.calls.get() >= 3);
  }

  @Test
  public void testFailedObservedLoadRequestsOneMore() throws Exception {
    FakeLoader loader = new FakeLoader();
    DataHubAuthorizer authorizer = authorizer(loader, 0);
    authorizer.initialLoad(1, 0);
    loader.policies.add(policy("p1", 1L));
    loader.failuresLeft = 1;

    authorizer.onPolicyChange();

    // The failed load is followed by one more, so the change applies without the timer.
    long deadline = System.currentTimeMillis() + 10_000;
    while (allNames(authorizer).isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(allNames(authorizer), List.of("p1"));
    Thread.sleep(300);
    // The first load, the failed load, and exactly one more: no retry loop.
    assertEquals(loader.calls.get(), 3);
  }

  @DataProvider
  public Object[][] noWaitValues() {
    return new Object[][] {{0L}, {-1L}};
  }

  @Test(dataProvider = "noWaitValues")
  public void testNoWaitRequestsALoadWithoutATimeout(long waitMillis) throws Exception {
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
    loader.policies.add(policy("p1", 1L));
    loader.release = new CountDownLatch(1);

    long start = System.nanoTime();
    authorizer.onPolicyChange();
    long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    loader.release.countDown();

    assertTrue(waited < 5_000, "waited " + waited + " ms");
    // A negative wait is set to 0. Not waiting is not a timeout, so there is no timeout metric.
    verify(metrics, never())
        .increment(eq(DataHubAuthorizer.class), eq("policyCache.observer.timeout"), anyDouble());
    // The load still runs in the background.
    authorizer.requestLoad().get(10, TimeUnit.SECONDS);
    assertEquals(allNames(authorizer), List.of("p1"));
  }
}
