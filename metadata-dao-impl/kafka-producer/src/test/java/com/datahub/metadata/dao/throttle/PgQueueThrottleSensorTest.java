package com.datahub.metadata.dao.throttle;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class PgQueueThrottleSensorTest {

  private static final String VERSIONED_TOPIC = "MetadataChangeLog_Versioned_v1";
  private static final String TIMESERIES_TOPIC = "MetadataChangeLog_Timeseries_v1";
  private static final String CONSUMER_GROUP = "generic-mae-consumer-job-client";

  @Test
  public void testIsThrottledReturnsFalseWhenDisabled() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    MetadataChangeProposalConfig.ThrottlesConfig config = disabledConfig();

    PgQueueThrottleSensor sensor = buildSensor(store, config);

    assertFalse(sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
    assertFalse(sensor.isThrottled(ThrottleType.MCL_TIMESERIES_LAG));
  }

  @Test
  public void testIsThrottledReturnsFalseWhenNoLagData() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(1);

    PgQueueThrottleSensor sensor = buildSensor(store, config);

    assertFalse(
        sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "No refresh called, so no lag data should exist");
  }

  @Test
  public void testIsThrottledReturnsTrueWhenMedianLagExceedsThreshold() {
    MetadataQueueStore store = mockStoreWithLag(3, new long[] {10, 20, 30}, new long[] {5, 8, 12});

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(1);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertTrue(
        sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "Median lag should exceed threshold of 1");
  }

  @Test
  public void testIsThrottledReturnsFalseWhenMedianLagBelowThreshold() {
    MetadataQueueStore store = mockStoreWithLag(3, new long[] {10, 20, 30}, new long[] {9, 19, 29});

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(5);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertFalse(
        sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "Median lag of 1 should be below threshold of 5");
  }

  @Test
  public void testComputeNextBackOffReturnsZeroWhenNotThrottled() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    MetadataChangeProposalConfig.ThrottlesConfig config = disabledConfig();

    PgQueueThrottleSensor sensor = buildSensor(store, config);

    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 0L);
  }

  @Test
  public void testComputeNextBackOffExponentialWhenThrottled() {
    // nextExclusive=[11,21,31], committed=[5,8,12] => lag=[5,12,18], median=12
    MetadataQueueStore store = mockStoreWithLag(3, new long[] {11, 21, 31}, new long[] {5, 8, 12});

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(1);
    config
        .getVersioned()
        .setInitialIntervalMs(1)
        .setMultiplier(2)
        .setMaxAttempts(5)
        .setMaxIntervalMs(8);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertTrue(sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG));

    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 1L, "initial");
    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 2L, "2^1");
    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 4L, "2^2");
    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 8L, "2^3 capped");
    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 8L, "max interval");
    assertEquals(sensor.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), -1L, "max attempts");
  }

  @Test
  public void testGetMedianOddSizeList() {
    // 3 partitions: nextExclusive=[4,7,10], committed=[1,2,3]
    // lag = max(0, (4-1)-1)=2, max(0,(7-1)-2)=4, max(0,(10-1)-3)=6  => sorted [2,4,6] => median=4
    MetadataQueueStore store = mockStoreWithLag(3, new long[] {4, 7, 10}, new long[] {1, 2, 3});

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(0);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertEquals(sensor.getLag().get(ThrottleType.MCL_VERSIONED_LAG), Long.valueOf(4L));
  }

  @Test
  public void testGetMedianEvenSizeList() {
    // 4 partitions: nextExclusive=[3,5,9,13], committed=[1,2,3,4]
    // lag = (3-1)-1=1, (5-1)-2=2, (9-1)-3=5, (13-1)-4=8 => sorted [1,2,5,8] => median=(2+5)/2=3
    MetadataQueueStore store =
        mockStoreWithLag(4, new long[] {3, 5, 9, 13}, new long[] {1, 2, 3, 4});

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(0);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertEquals(sensor.getLag().get(ThrottleType.MCL_VERSIONED_LAG), Long.valueOf(3L));
  }

  @Test
  public void testRefreshPopulatesLagFromStore() {
    int partitions = 3;
    long topicId = 42L;
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    QueueTopicMetadata topicMeta = new QueueTopicMetadata(topicId, partitions, Optional.empty());

    when(store.fetchTopic(VERSIONED_TOPIC)).thenReturn(Optional.of(topicMeta));
    when(store.fetchTopic(TIMESERIES_TOPIC)).thenReturn(Optional.of(topicMeta));

    when(store.partitionMaxEnqueueSeqs(topicId, partitions))
        .thenReturn(Map.of(0, 4L, 1, 9L, 2, 14L));

    // committed offsets: 2, 4, 6
    when(store.getCommittedOffset(eq(CONSUMER_GROUP), eq(topicId), eq(0))).thenReturn(2L);
    when(store.getCommittedOffset(eq(CONSUMER_GROUP), eq(topicId), eq(1))).thenReturn(4L);
    when(store.getCommittedOffset(eq(CONSUMER_GROUP), eq(topicId), eq(2))).thenReturn(6L);

    // lag = max(0, maxSeq - committed) = [2,5,8], median = 5
    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(0);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    assertTrue(sensor.getLag().isEmpty(), "No lag before refresh");

    sensor.refresh();

    Map<ThrottleType, Long> lag = sensor.getLag();
    assertFalse(lag.isEmpty(), "Lag should be populated after refresh");
    assertEquals(lag.get(ThrottleType.MCL_VERSIONED_LAG), Long.valueOf(5L));
    assertEquals(lag.get(ThrottleType.MCL_TIMESERIES_LAG), Long.valueOf(5L));
  }

  @Test
  public void testRefreshWithNoTopicReturnsZeroLag() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(anyString())).thenReturn(Optional.empty());

    MetadataChangeProposalConfig.ThrottlesConfig config = enabledConfig(1);

    PgQueueThrottleSensor sensor = buildSensor(store, config);
    sensor.refresh();

    assertEquals(sensor.getLag().get(ThrottleType.MCL_VERSIONED_LAG), Long.valueOf(0L));
    assertEquals(sensor.getLag().get(ThrottleType.MCL_TIMESERIES_LAG), Long.valueOf(0L));
    assertFalse(sensor.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
  }

  // ---- helpers ----

  private static PgQueueThrottleSensor buildSensor(
      MetadataQueueStore store, MetadataChangeProposalConfig.ThrottlesConfig config) {
    return PgQueueThrottleSensor.builder()
        .metadataQueueStore(store)
        .config(config)
        .mclConsumerGroupId(CONSUMER_GROUP)
        .versionedTopicName(VERSIONED_TOPIC)
        .timeseriesTopicName(TIMESERIES_TOPIC)
        .build()
        .addCallback(throttleEvent -> ThrottleControl.NONE);
  }

  /**
   * Creates a mock store where both versioned and timeseries topics share the same lag setup.
   *
   * @param partitions number of partitions
   * @param nextExclusive per-partition nextExclusiveSeq values (length == partitions)
   * @param committed per-partition committed offsets (length == partitions)
   */
  private static MetadataQueueStore mockStoreWithLag(
      int partitions, long[] nextExclusive, long[] committed) {
    long topicId = 1L;
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    QueueTopicMetadata topicMeta = new QueueTopicMetadata(topicId, partitions, Optional.empty());

    when(store.fetchTopic(VERSIONED_TOPIC)).thenReturn(Optional.of(topicMeta));
    when(store.fetchTopic(TIMESERIES_TOPIC)).thenReturn(Optional.of(topicMeta));

    Map<Integer, Long> maxMap = new java.util.HashMap<>();
    for (int p = 0; p < partitions; p++) {
      maxMap.put(p, Math.max(0L, nextExclusive[p] - 1));
      when(store.getCommittedOffset(anyString(), eq(topicId), eq(p))).thenReturn(committed[p]);
    }
    when(store.partitionMaxEnqueueSeqs(topicId, partitions)).thenReturn(maxMap);

    return store;
  }

  private static MetadataChangeProposalConfig.ThrottlesConfig disabledConfig() {
    MetadataChangeProposalConfig.ThrottlesConfig config =
        new MetadataChangeProposalConfig.ThrottlesConfig().setUpdateIntervalMs(0);
    config.setVersioned(new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    config.setTimeseries(new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    return config;
  }

  private static MetadataChangeProposalConfig.ThrottlesConfig enabledConfig(int threshold) {
    MetadataChangeProposalConfig.ThrottlesConfig config =
        new MetadataChangeProposalConfig.ThrottlesConfig().setUpdateIntervalMs(0);
    config.setVersioned(
        new MetadataChangeProposalConfig.ThrottleConfig()
            .setEnabled(true)
            .setThreshold(threshold)
            .setInitialIntervalMs(1)
            .setMultiplier(1)
            .setMaxAttempts(1)
            .setMaxIntervalMs(1));
    config.setTimeseries(new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    return config;
  }
}
