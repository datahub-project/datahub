package com.datahub.authorization;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/** Tests for the SQL policy source: synchronous first load, ordered and joined loads. */
public class DataHubAuthorizerSqlSourceTest {

  /** A loader whose result can change between calls, and that can fail or block on demand. */
  private static final class FakeLoader implements PolicyLoader {
    final List<PolicyFetcher.Policy> policies = new CopyOnWriteArrayList<>();
    final AtomicInteger calls = new AtomicInteger();
    volatile int failuresLeft = 0;
    volatile CountDownLatch entered;
    volatile CountDownLatch release;

    @Override
    public List<PolicyFetcher.Policy> loadAll() {
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
      return new ArrayList<>(policies);
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
}
