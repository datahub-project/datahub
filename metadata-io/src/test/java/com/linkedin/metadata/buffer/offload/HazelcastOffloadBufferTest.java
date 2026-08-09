package com.linkedin.metadata.buffer.offload;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import java.io.Serializable;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Behavior tests for the framework {@link HazelcastOffloadBuffer}: the two merge/sizing matrices
 * the two known uses rely on (hooks = NO_COALESCE + REJECT_AT_CAP; retention = KEEP_MAX_LONG +
 * EVICT_LRU), plus the shared drain-lock / paging / CAS / sequence infra. Mirrors {@code
 * HazelcastCoalesceBufferTest}'s isolated-instance style.
 */
public class HazelcastOffloadBufferTest {

  private static final String MAP_NAME = "test-offload-pending";
  private static final String LOCK_MAP_NAME = "test-offload-drain-lock";
  private static final String SEQ_MAP_NAME = "test-offload-pending.seq";

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    if (hazelcastInstance != null) {
      hazelcastInstance.shutdown();
      hazelcastInstance = null;
    }
  }

  private static HazelcastInstance newIsolatedInstance() {
    Config config = new Config();
    config.setInstanceName("offload-buffer-test-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  private HazelcastOffloadBuffer<TestKey, Long> noCoalesceBuffer(int maxPending) {
    return new HazelcastOffloadBuffer<>(
        hazelcastInstance,
        MAP_NAME,
        LOCK_MAP_NAME,
        SEQ_MAP_NAME,
        maxPending,
        MergePolicy.NO_COALESCE,
        SizingPolicy.REJECT_AT_CAP,
        new SeqDrainOrder<>(),
        "test",
        null);
  }

  private HazelcastOffloadBuffer<TestKey, Long> keepMaxBuffer() {
    return new HazelcastOffloadBuffer<>(
        hazelcastInstance,
        MAP_NAME,
        LOCK_MAP_NAME,
        SEQ_MAP_NAME,
        100_000,
        MergePolicy.KEEP_MAX_LONG,
        SizingPolicy.EVICT_LRU,
        new SeqDrainOrder<>(),
        "test",
        null);
  }

  @Test
  public void testNoCoalesceKeepsEveryDistinctKey() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    // Distinct keys (unique sequence) — no coalescing; every entry survives.
    buffer.enqueue(new TestKey("a", 1L), 1L);
    buffer.enqueue(new TestKey("a", 2L), 2L);
    buffer.enqueue(new TestKey("a", 3L), 3L);

    List<Map.Entry<TestKey, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 3);
  }

  @Test
  public void testKeepMaxLongCoalescesToMaxValue() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = keepMaxBuffer();
    TestKey key = new TestKey("a", 0L);
    buffer.enqueue(key, 5L);
    buffer.enqueue(key, 2L);
    buffer.enqueue(key, 9L);

    List<Map.Entry<TestKey, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getKey(), key);
    assertEquals(batch.get(0).getValue().longValue(), 9L);
  }

  @Test
  public void testKeepMaxLongRejectsNonLongValue() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, String> buffer =
        new HazelcastOffloadBuffer<>(
            hazelcastInstance,
            MAP_NAME,
            LOCK_MAP_NAME,
            SEQ_MAP_NAME,
            100_000,
            MergePolicy.KEEP_MAX_LONG,
            SizingPolicy.EVICT_LRU,
            new SeqDrainOrder<>(),
            "test",
            null);
    assertThrows(
        IllegalArgumentException.class, () -> buffer.enqueue(new TestKey("a", 0L), "not-a-long"));
  }

  @Test
  public void testRejectAtCapReturnsFalseAndDoesNotStore() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(2);
    assertTrue(buffer.enqueue(new TestKey("a", 1L), 1L));
    assertTrue(buffer.enqueue(new TestKey("a", 2L), 2L));
    // At cap → reject (caller sync fallback, no loss). Entry must NOT be stored.
    assertFalse(buffer.enqueue(new TestKey("a", 3L), 3L));
    assertEquals(buffer.drain(10).size(), 2);
  }

  @Test
  public void testNextSequenceIsMonotonicAndUnique() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    long a = buffer.nextSequence();
    long b = buffer.nextSequence();
    long c = buffer.nextSequence();
    assertTrue(b > a);
    assertTrue(c > b);
  }

  @Test
  public void testDrainRespectsLimitAndFifoOrder() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    for (int i = 0; i < 5; i++) {
      buffer.enqueue(new TestKey("k" + i, i), (long) i);
    }
    List<Map.Entry<TestKey, Long>> page = buffer.drain(2);
    assertEquals(page.size(), 2);
    // FIFO by sequence (SeqDrainOrder): the two lowest-sequence entries come first.
    assertTrue(page.get(0).getKey().seq <= page.get(1).getKey().seq);
    // drain is non-destructive (PagingPredicate restarts each call); a wider drain sees all 5.
    assertEquals(buffer.drain(10).size(), 5);
  }

  @Test
  public void testRemoveIfSameOnlyRemovesMatchingValue() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    TestKey key = new TestKey("a", 1L);
    buffer.enqueue(key, 3L);

    assertFalse(buffer.removeIfSame(key, 999L));
    assertTrue(buffer.removeIfSame(key, 3L));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  public void testRequeueReplacesValueInPlace() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    TestKey key = new TestKey("a", 1L);
    buffer.enqueue(key, 3L);
    buffer.requeue(key, 7L); // same key → value update, not a new entry
    List<Map.Entry<TestKey, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getValue().longValue(), 7L);
  }

  @Test
  public void testDrainLockIsMutuallyExclusiveAndTokenFenced() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);

    Object token = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(token);
    // Non-reentrant: a second acquire by anyone fails while held.
    assertNull(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));

    buffer.releaseDrainLock("drain", token);
    Object token2 = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(token2);
    // A stale (previous) token must NOT release the freshly-acquired lock — token2 still owns it.
    buffer.releaseDrainLock("drain", token);
    assertNull(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));
    buffer.releaseDrainLock("drain", token2);
  }

  @Test
  public void testDrainWithNonComparableKey() {
    // TestKey is Serializable but NOT Comparable. drain()'s PagingPredicate must use the supplied
    // comparator (SeqDrainOrder) rather than natural ordering, which would ClassCastException.
    hazelcastInstance = newIsolatedInstance();
    HazelcastOffloadBuffer<TestKey, Long> buffer = noCoalesceBuffer(100_000);
    for (int i = 0; i < 5; i++) {
      buffer.enqueue(new TestKey("urn:li:dataset:(...t" + i + ")", i), (long) i);
    }
    assertEquals(buffer.drain(3).size(), 3);
    // Non-destructive: a wider drain still sees all 5 (PagingPredicate restarts each call).
    assertEquals(buffer.drain(10).size(), 5);
  }

  /** Simple Serializable, non-Comparable key carrying a sequence for FIFO drain. */
  static final class TestKey implements Serializable {
    private static final long serialVersionUID = 1L;
    final String id;
    final long seq;

    TestKey(String id, long seq) {
      this.id = id;
      this.seq = seq;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TestKey)) {
        return false;
      }
      return seq == ((TestKey) o).seq && id.equals(((TestKey) o).id);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(id, seq);
    }

    @Override
    public String toString() {
      return "TestKey{id='" + id + "', seq=" + seq + "}";
    }
  }

  /** Serializable drain comparator ordering by key sequence (FIFO), value-type agnostic. */
  static final class SeqDrainOrder<V> implements Comparator<Map.Entry<TestKey, V>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Map.Entry<TestKey, V> a, Map.Entry<TestKey, V> b) {
      int bySeq = Long.compare(a.getKey().seq, b.getKey().seq);
      if (bySeq != 0) {
        return bySeq;
      }
      return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
    }
  }
}
