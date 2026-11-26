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
import java.util.concurrent.Future;
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
   * Thundering-herd core: batches {a,b,c} and {d,b,e} share the hot key b. While batch-1 holds its
   * set, b cannot be acquired by anyone else (serialized), but the disjoint key d IS free
   * (unrelated writers proceed). After batch-1 releases, b is acquirable again. Deterministic:
   * latch-sequenced with tryLock(0) probes — no timeouts, so no flakiness.
   */
  @Test
  public void sharedHotKeySerializesWhileDisjointKeysProceed() throws Exception {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);

    ExecutorService batch1 = Executors.newSingleThreadExecutor();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<?> holder = null;
    try {
      holder =
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

      // b held by batch-1 -> a second writer cannot take it (hot key serialized).
      assertFalse(map.tryLock("urn:b", 0, TimeUnit.SECONDS), "hot key b must be serialized");
      // d is disjoint -> a second writer proceeds immediately (no false contention).
      assertTrue(map.tryLock("urn:d", 0, TimeUnit.SECONDS), "disjoint key d must be free");
      map.unlock("urn:d");

      release.countDown();
    } finally {
      release.countDown();
      batch1.shutdown();
      assertTrue(batch1.awaitTermination(10, TimeUnit.SECONDS));
      // Surface any exception thrown by the holder task — notably h.close() (the release path);
      // an executor otherwise swallows it and the test would pass despite a broken release.
      if (holder != null) {
        holder.get(5, TimeUnit.SECONDS);
      }
    }

    // After batch-1 released, b drains and is acquirable again.
    assertFalse(map.isLocked("urn:b"));
    assertTrue(map.tryLock("urn:b", 0, TimeUnit.SECONDS));
    map.unlock("urn:b");
  }

  /**
   * Two disjoint batches on DIFFERENT threads don't block each other — while one thread holds
   * {a,b,c}, another acquires {d,e,f} immediately. Cross-thread (not same-thread, which would pass
   * even under false contention because IMap locks are reentrant per thread).
   */
  @Test
  public void disjointBatchesDoNotContend() throws Exception {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);
    ExecutorService other = Executors.newSingleThreadExecutor();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      other.submit(
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

      // Disjoint set on this thread acquires immediately despite the other thread holding a,b,c.
      EntityWriteLock.LockHandle mine = lock.acquire(opContext, List.of("urn:d", "urn:e", "urn:f"));
      for (String u : List.of("urn:d", "urn:e", "urn:f")) {
        assertTrue(map.isLocked(u));
      }
      mine.close();
      release.countDown();
    } finally {
      release.countDown();
      other.shutdown();
      assertTrue(other.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /**
   * Two batches request the SAME two URNs in OPPOSITE input orders, concurrently and repeatedly.
   * Sorted acquisition must prevent ABBA — both always complete within the bound. A real deadlock
   * would surface as a Future.get timeout (test failure), not flakiness.
   */
  @Test
  public void overlappingReverseOrderBatchesDoNotDeadlock() throws Exception {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < 25; i++) {
        Future<?> f1 =
            pool.submit(() -> lock.acquire(opContext, List.of("urn:y", "urn:x")).close());
        Future<?> f2 =
            pool.submit(() -> lock.acquire(opContext, List.of("urn:x", "urn:y")).close());
        f1.get(10, TimeUnit.SECONDS);
        f2.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Release tolerates the lock being freed underneath (lease expiry): close() must not throw. */
  @Test
  public void releaseToleratesLockFreedUnderneath() {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 5, 60);
    EntityWriteLock.LockHandle handle = lock.acquire(opContext, List.of("urn:a"));
    assertTrue(map.isLocked("urn:a"));
    map.forceUnlock("urn:a"); // simulate lease expiry — lock freed out from under the holder
    handle.close(); // must not throw even though the lock is already gone
    assertFalse(map.isLocked("urn:a"));
  }

  /** Hazelcast unavailable: acquire degrades to lockless (no throw), handle close is a no-op. */
  @Test
  public void degradesLocklessWhenHazelcastDown() {
    HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 1, 60);
    hz.shutdown();

    EntityWriteLock.LockHandle handle = lock.acquire(opContext, List.of("urn:a"));
    handle.close(); // must not throw
  }

  /**
   * A waiter that can't acquire a genuinely-held key within the timeout proceeds WITHOUT the lock
   * (best-effort), never blocking or throwing, and never stealing/releasing the holder's lock. Uses
   * acquireTimeout=0 so the contended tryLock returns immediately — deterministic, no sleeps.
   */
  @Test
  public void acquireTimesOutOnHeldKeyAndProceedsLockless() throws Exception {
    final HazelcastEntityWriteLock lock = new HazelcastEntityWriteLock(hz, MAP, 0, 60);
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final ExecutorService holder = Executors.newSingleThreadExecutor();
    try {
      holder.submit(
          () -> {
            map.lock(
                "urn:x"); // Hazelcast IMap locks are thread-owned — hold it off the test thread
            held.countDown();
            release.await(10, TimeUnit.SECONDS);
            map.unlock("urn:x");
            return null;
          });
      assertTrue(held.await(10, TimeUnit.SECONDS));

      // Contended acquire with timeout=0 returns immediately without acquiring (the test would hang
      // if it blocked). The key stays owned by the holder; the no-op handle must not release it.
      EntityWriteLock.LockHandle handle = lock.acquire(opContext, List.of("urn:x"));
      assertTrue(map.isLocked("urn:x"), "waiter must not have acquired the held key");
      handle.close();
      assertTrue(map.isLocked("urn:x"), "close must not release a lock the waiter never held");
    } finally {
      release.countDown();
      holder.shutdown();
      assertTrue(holder.awaitTermination(10, TimeUnit.SECONDS));
    }
    assertTrue(map.tryLock("urn:x", 0, TimeUnit.SECONDS), "key is free once the holder releases");
  }
}
