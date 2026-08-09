package com.linkedin.metadata.entity.lock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HazelcastEntityWriteLockTest {

  private static final String MAP = "test-entity-write-lock";

  private HazelcastInstance hz;
  private IMap<String, Boolean> map;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    Config config = new Config();
    config.setInstanceName("ewl-test-" + UUID.randomUUID());
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    hz = Hazelcast.newHazelcastInstance(config);
    map = hz.getMap(MAP);

    opContext = mock(OperationContext.class);
    when(opContext.getMetricUtils()).thenReturn(Optional.empty());
  }

  @AfterMethod
  public void teardown() {
    if (hz != null) {
      hz.shutdown();
    }
  }

  @Test
  public void acquireLocksThenReleaseUnlocks() {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);

    EntityWriteLock.LockHandle handle = lock.acquire(opContext, List.of("urn:a", "urn:b"));
    assertTrue(map.isLocked("urn:a"));
    assertTrue(map.isLocked("urn:b"));

    handle.close();
    assertFalse(map.isLocked("urn:a"));
    assertFalse(map.isLocked("urn:b"));
  }

  /**
   * Thundering-herd core: two batches {a,b,c} and {d,b,e} share the hot key b. While batch-1 holds
   * its set, b cannot be acquired by anyone else (serialized), but the disjoint key d IS free
   * (unrelated writers proceed). After batch-1 releases, b is acquirable again. Deterministic:
   * latch-sequenced, tryLock(0) probes — no timeouts, so no flakiness.
   */
  @Test
  public void sharedHotKeySerializesWhileDisjointKeysProceed() throws Exception {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);

    ExecutorService batch1 = Executors.newSingleThreadExecutor();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      batch1.submit(
          () -> {
            EntityWriteLock.LockHandle h =
                lock.acquire(opContext, List.of("urn:a", "urn:b", "urn:c"));
            held.countDown();
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
              Thread.currentThread().interrupt();
            }
            h.close();
          });
      assertTrue(held.await(10, TimeUnit.SECONDS));

      // b is held by batch-1 -> a second writer cannot take it (serialized on the hot key).
      assertFalse(map.tryLock("urn:b", 0, TimeUnit.SECONDS), "hot key b must be serialized");
      // d is disjoint -> a second writer proceeds immediately (no false contention).
      assertTrue(map.tryLock("urn:d", 0, TimeUnit.SECONDS), "disjoint key d must be free");
      map.unlock("urn:d");

      release.countDown();
    } finally {
      release.countDown();
      batch1.shutdown();
      assertTrue(batch1.awaitTermination(10, TimeUnit.SECONDS));
    }

    // After batch-1 released, b is acquirable again (the queue drains).
    assertFalse(map.isLocked("urn:b"));
    assertTrue(map.tryLock("urn:b", 0, TimeUnit.SECONDS));
    map.unlock("urn:b");
  }

  /** Fully disjoint batches never contend — both acquire immediately. */
  @Test
  public void disjointBatchesDoNotContend() {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);

    EntityWriteLock.LockHandle h1 = lock.acquire(opContext, List.of("urn:a", "urn:b", "urn:c"));
    EntityWriteLock.LockHandle h2 = lock.acquire(opContext, List.of("urn:d", "urn:e", "urn:f"));
    for (String u : List.of("urn:a", "urn:b", "urn:c", "urn:d", "urn:e", "urn:f")) {
      assertTrue(map.isLocked(u));
    }
    h1.close();
    h2.close();
  }

  /** Hazelcast unavailable: acquire degrades to lockless (no throw), handle close is a no-op. */
  @Test
  public void degradesLocklessWhenHazelcastDown() {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 1, 60);
    hz.shutdown();

    EntityWriteLock.LockHandle handle = lock.acquire(opContext, List.of("urn:a"));
    handle.close(); // must not throw
  }
}
