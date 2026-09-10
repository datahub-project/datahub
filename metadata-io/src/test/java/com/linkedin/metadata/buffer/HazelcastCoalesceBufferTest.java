package com.linkedin.metadata.buffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BinaryOperator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class HazelcastCoalesceBufferTest {

  private static final String MAP_NAME = "test-pending";
  private static final String LOCK_MAP_NAME = "test-drain-lock";
  private static final String KEY =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)|status";

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
    config.setInstanceName("coalesce-buffer-test-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  @Test
  public void testMergeKeepsMaxValueOnCoalesce() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<String> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);

    buffer.merge(KEY, 5L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(KEY, 2L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(KEY, 9L, CoalesceBuffers.KEEP_MAX_LONG);

    List<Map.Entry<String, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getKey(), KEY);
    assertEquals(batch.get(0).getValue().longValue(), 9L);
  }

  @Test
  public void testMergeRejectsNonKeepMaxLongPolicy() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<String> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    BinaryOperator<Long> notKeepMaxLong = (a, b) -> a;

    assertThrows(UnsupportedOperationException.class, () -> buffer.merge(KEY, 1L, notKeepMaxLong));
  }

  @Test
  public void testDrainRespectsLimit() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<String> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    for (int i = 0; i < 5; i++) {
      buffer.merge(KEY + "-" + i, (long) i, CoalesceBuffers.KEEP_MAX_LONG);
    }
    assertEquals(buffer.drain(2).size(), 2);
  }

  @Test
  public void testRemoveIfSameOnlyRemovesMatchingValue() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<String> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);
    buffer.merge(KEY, 3L, CoalesceBuffers.KEEP_MAX_LONG);

    assertFalse(buffer.removeIfSame(KEY, 999L));
    assertTrue(buffer.removeIfSame(KEY, 3L));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  public void testDrainLockIsMutuallyExclusive() {
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<String> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);

    Object token = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(token);
    assertNull(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));

    buffer.releaseDrainLock("drain", token);
    Object token2 = buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60));
    assertNotNull(token2);
    buffer.releaseDrainLock("drain", token2);
  }

  @Test
  public void testDrainWithNonComparableKey() {
    // RetentionKey (the real prod key) is Serializable but NOT Comparable. drain()'s
    // PagingPredicate must supply its own comparator; otherwise natural ordering casts the key to
    // Comparable and throws ClassCastException. String-keyed tests above cannot catch this.
    hazelcastInstance = newIsolatedInstance();
    HazelcastCoalesceBuffer<RetentionKey> buffer =
        new HazelcastCoalesceBuffer<>(hazelcastInstance, MAP_NAME, LOCK_MAP_NAME, null);

    for (int i = 0; i < 5; i++) {
      buffer.merge(
          new SimpleRetentionKey(
              "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.t" + i + ",PROD)", "status"),
          (long) i,
          CoalesceBuffers.KEEP_MAX_LONG);
    }

    assertEquals(buffer.drain(3).size(), 3);
    assertEquals(buffer.drain(10).size(), 5);
  }
}
