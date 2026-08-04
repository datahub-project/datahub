package com.linkedin.metadata.buffer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class LocalCoalesceBufferTest {

  private static final String KEY =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)|status";
  private static final String OTHER_KEY =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.overflow,PROD)|status";

  @Test
  public void testMergeKeepsMaxValueOnCoalesce() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 100, null);

    buffer.merge(KEY, 5L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(KEY, 2L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(KEY, 9L, CoalesceBuffers.KEEP_MAX_LONG);

    List<Map.Entry<String, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getKey(), KEY);
    assertEquals(batch.get(0).getValue().longValue(), 9L);
  }

  @Test
  public void testMergeDropsNewKeysWhenBufferFull() {
    MetricUtils mockMetricUtils = mock(MetricUtils.class);
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 1, mockMetricUtils);

    buffer.merge(KEY, 1L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(OTHER_KEY, 1L, CoalesceBuffers.KEEP_MAX_LONG);

    List<Map.Entry<String, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getKey(), KEY);
    verify(mockMetricUtils, times(1))
        .increment(eq(LocalCoalesceBuffer.class), eq("test-buffer_overflow"), eq(1.0d));
  }

  @Test
  public void testMergeAllowsExistingKeyUpdateWhenBufferFull() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 1, null);

    buffer.merge(KEY, 1L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge(KEY, 7L, CoalesceBuffers.KEEP_MAX_LONG);

    List<Map.Entry<String, Long>> batch = buffer.drain(10);
    assertEquals(batch.size(), 1);
    assertEquals(batch.get(0).getValue().longValue(), 7L);
  }

  @Test
  public void testRemoveIfSameOnlyRemovesMatchingValue() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 100, null);
    buffer.merge(KEY, 3L, CoalesceBuffers.KEEP_MAX_LONG);

    assertFalse(buffer.removeIfSame(KEY, 999L));
    assertTrue(buffer.removeIfSame(KEY, 3L));
    assertTrue(buffer.drain(10).isEmpty());
  }

  @Test
  public void testDrainLockIsMutuallyExclusive() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 100, null);

    assertTrue(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));
    assertFalse(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));

    buffer.releaseDrainLock("drain");
    assertTrue(buffer.tryAcquireDrainLock("drain", Duration.ofSeconds(60)));
    buffer.releaseDrainLock("drain");
  }

  @Test
  public void testDrainLocksAreIndependentPerName() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 100, null);

    assertTrue(buffer.tryAcquireDrainLock("drain-a", Duration.ofSeconds(60)));
    assertTrue(buffer.tryAcquireDrainLock("drain-b", Duration.ofSeconds(60)));

    buffer.releaseDrainLock("drain-a");
    buffer.releaseDrainLock("drain-b");
  }

  @Test
  public void testDrainRespectsLimit() {
    LocalCoalesceBuffer<String, Long> buffer =
        new LocalCoalesceBuffer<>("test-buffer", 100, null);
    buffer.merge("k1", 1L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge("k2", 2L, CoalesceBuffers.KEEP_MAX_LONG);
    buffer.merge("k3", 3L, CoalesceBuffers.KEEP_MAX_LONG);

    assertEquals(buffer.drain(2).size(), 2);
  }
}
