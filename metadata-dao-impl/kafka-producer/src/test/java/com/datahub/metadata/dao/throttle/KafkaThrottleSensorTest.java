package com.datahub.metadata.dao.throttle;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.Topics;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class KafkaThrottleSensorTest {
  private static final List<String> STANDARD_TOPICS =
      List.of(Topics.METADATA_CHANGE_LOG_VERSIONED, Topics.METADATA_CHANGE_LOG_TIMESERIES);
  private static final String STANDARD_MCL_CONSUMER_GROUP_ID = "generic-mae-consumer-job-client";
  private static final String LOST_OFFSETS = "no longer has committed offsets on MCL topic ";
  private static final String NO_OFFSETS = "has no committed offsets on MCL topic ";

  @Test
  public void testLagCalculation() throws ExecutionException, InterruptedException {
    // 3 partitions
    // Consumer offsets: 1, 2, 3
    // End offsets: 2, 4, 6
    // Lag: 1, 2, 3
    // MedianLag: 2
    AdminClient mockAdmin =
        mockKafka(
            generateLag(
                STANDARD_TOPICS,
                topicPart -> (long) topicPart.partition() + 1,
                topicPart -> ((long) topicPart.partition() + 1) * 2,
                3));

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(noSchedulerConfig().getThrottle())
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build()
            .addCallback((throttleEvent -> ThrottleControl.NONE));

    // Refresh calculations
    test.refresh();

    assertEquals(
        test.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 2L,
            ThrottleType.MCL_TIMESERIES_LAG, 2L));
  }

  @Test
  public void testThrottle() throws ExecutionException, InterruptedException {
    MetadataChangeProposalConfig.ThrottlesConfig noThrottleConfig =
        noSchedulerConfig().getThrottle();
    noThrottleConfig
        .getVersioned()
        .setThreshold(10)
        .setInitialIntervalMs(1)
        .setMultiplier(1)
        .setMaxAttempts(1)
        .setMaxIntervalMs(1);

    MetadataChangeProposalConfig.ThrottlesConfig throttleConfig = noSchedulerConfig().getThrottle();
    throttleConfig
        .getVersioned()
        .setThreshold(1)
        .setInitialIntervalMs(1)
        .setMultiplier(1)
        .setMaxAttempts(1)
        .setMaxIntervalMs(1);

    // 3 partitions
    // Consumer offsets: 1, 2, 3
    // End offsets: 2, 4, 6
    // Lag: 1, 2, 3
    // MedianLag: 2
    AdminClient mockAdmin =
        mockKafka(
            generateLag(
                STANDARD_TOPICS,
                topicPart -> (long) topicPart.partition() + 1,
                topicPart -> ((long) topicPart.partition() + 1) * 2,
                3));

    Consumer<Boolean> pauseFunction = mock(Consumer.class);

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(noThrottleConfig)
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build()
            .addCallback(
                (throttleEvent -> {
                  pauseFunction.accept(throttleEvent.isThrottled());
                  return ThrottleControl.builder()
                      .callback(
                          throttleResume -> pauseFunction.accept(throttleResume.isThrottled()))
                      .build();
                }));

    // Refresh calculations
    test.refresh();
    assertEquals(
        test.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 2L,
            ThrottleType.MCL_TIMESERIES_LAG, 2L));
    assertFalse(
        test.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "Expected not throttling, lag is below threshold");
    assertFalse(test.isThrottled(ThrottleType.MCL_TIMESERIES_LAG));
    test.throttle();
    verifyNoInteractions(pauseFunction);
    reset(pauseFunction);

    KafkaThrottleSensor test2 = test.toBuilder().config(throttleConfig).build();
    // Refresh calculations
    test2.refresh();
    assertEquals(
        test2.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 2L,
            ThrottleType.MCL_TIMESERIES_LAG, 2L));
    assertTrue(
        test2.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "Expected throttling, lag is above threshold.");
    assertFalse(
        test2.isThrottled(ThrottleType.MCL_TIMESERIES_LAG),
        "Expected not throttling. Timeseries is disabled");
    test2.throttle();

    // verify 1ms pause and resume
    verify(pauseFunction).accept(eq(true));
    verify(pauseFunction).accept(eq(false));
    verifyNoMoreInteractions(pauseFunction);
  }

  @Test
  public void testBackOff() throws ExecutionException, InterruptedException {
    MetadataChangeProposalConfig.ThrottlesConfig throttleConfig = noSchedulerConfig().getThrottle();
    throttleConfig
        .getVersioned()
        .setThreshold(1)
        .setInitialIntervalMs(1)
        .setMultiplier(2)
        .setMaxAttempts(5)
        .setMaxIntervalMs(8);

    // 3 partitions
    // Consumer offsets: 1, 2, 3
    // End offsets: 2, 4, 6
    // Lag: 1, 2, 3
    // MedianLag: 2
    AdminClient mockAdmin =
        mockKafka(
            generateLag(
                STANDARD_TOPICS,
                topicPart -> (long) topicPart.partition() + 1,
                topicPart -> ((long) topicPart.partition() + 1) * 2,
                3));

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(throttleConfig)
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build()
            .addCallback((throttleEvent -> ThrottleControl.NONE));

    // Refresh calculations
    test.refresh();
    assertEquals(
        test.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 2L,
            ThrottleType.MCL_TIMESERIES_LAG, 2L));
    assertTrue(
        test.isThrottled(ThrottleType.MCL_VERSIONED_LAG),
        "Expected throttling, lag is above threshold.");
    assertFalse(
        test.isThrottled(ThrottleType.MCL_TIMESERIES_LAG),
        "Expected no throttling. Timeseries is disabled");

    assertEquals(
        test.computeNextBackOff(ThrottleType.MCL_TIMESERIES_LAG),
        0L,
        "Expected no backoff. Timeseries is disabled.");

    assertEquals(test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 1L, "Expected initial 1");
    assertEquals(
        test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 2L, "Expected second 2^1");
    assertEquals(test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 4L, "Expected third 2^2");
    assertEquals(
        test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), 8L, "Expected fourth 2^3");
    assertEquals(
        test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG),
        8L,
        "Expected fifth max interval at 8");
    assertEquals(
        test.computeNextBackOff(ThrottleType.MCL_VERSIONED_LAG), -1L, "Expected max attempts");
  }

  @Test
  public void testScheduler() throws ExecutionException, InterruptedException {
    MetadataChangeProposalConfig config = new MetadataChangeProposalConfig();
    MetadataChangeProposalConfig.ThrottlesConfig throttlesConfig =
        new MetadataChangeProposalConfig.ThrottlesConfig()
            .setUpdateIntervalMs(10); // configure fast update for test
    throttlesConfig.setVersioned(
        new MetadataChangeProposalConfig.ThrottleConfig()
            .setEnabled(true) // enable 1 throttle config to activate
        );
    throttlesConfig.setTimeseries(
        new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    config.setThrottle(throttlesConfig);

    // 1 lag, 1 partition
    AdminClient mockAdmin =
        mockKafka(generateLag(STANDARD_TOPICS, topicPart -> 1L, topicPart -> 2L, 1));

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(throttlesConfig)
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build()
            .addCallback((throttleEvent -> ThrottleControl.NONE));

    try {
      test.start();
      Thread.sleep(50);
      assertEquals(
          test.getLag(),
          Map.of(
              ThrottleType.MCL_VERSIONED_LAG, 1L,
              ThrottleType.MCL_TIMESERIES_LAG, 1L),
          "Expected lag updated");
    } finally {
      test.stop();
    }
  }

  @Test
  public void testTopicWithoutCommittedOffsetsHasNoLag()
      throws ExecutionException, InterruptedException {
    // The consumer group has committed on the versioned topic only (lag 1, 2, 3 -> median 2)
    AdminClient mockAdmin =
        mockKafka(
            generateLag(
                List.of(STANDARD_TOPICS.get(0)),
                topicPart -> (long) topicPart.partition() + 1,
                topicPart -> ((long) topicPart.partition() + 1) * 2,
                3));

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(noSchedulerConfig().getThrottle())
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build();

    test.refresh();

    assertEquals(
        test.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 2L,
            ThrottleType.MCL_TIMESERIES_LAG, 0L));
  }

  @Test
  public void testSchedulerKeepsRefreshingAfterARefreshWithoutCommittedOffsets()
      throws ExecutionException, InterruptedException {
    // The first refresh sees a consumer group with no committed offsets, later ones a lag of 1
    Pair<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, Long>> lag =
        generateLag(STANDARD_TOPICS, topicPart -> 1L, topicPart -> 2L, 1);
    AdminClient mockAdmin = mockKafka(lag);
    KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> consumerFuture = mock(KafkaFuture.class);
    when(consumerFuture.get()).thenReturn(Map.of(), lag.getFirst());
    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(consumerFuture);
    when(mockAdmin.listConsumerGroupOffsets(anyString())).thenReturn(offsetsResult);

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(highThresholdSchedulerConfig())
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build();

    assertLagEventuallyRefreshed(test);
  }

  @Test
  public void testSchedulerKeepsRefreshingAfterARefreshFails()
      throws ExecutionException, InterruptedException {
    // The first refresh fails in the admin client, later ones see a lag of 1
    AdminClient mockAdmin =
        mockKafka(generateLag(STANDARD_TOPICS, topicPart -> 1L, topicPart -> 2L, 1));
    ListConsumerGroupOffsetsResult offsetsResult =
        mockAdmin.listConsumerGroupOffsets(STANDARD_MCL_CONSUMER_GROUP_ID);
    when(mockAdmin.listConsumerGroupOffsets(anyString()))
        .thenThrow(new KafkaException("broker unavailable"))
        .thenReturn(offsetsResult);

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(highThresholdSchedulerConfig())
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build();

    assertLagEventuallyRefreshed(test);
  }

  @Test
  public void testFreshInstallWithoutCommittedOffsetsReportsNoLagAndLogsOnce()
      throws ExecutionException, InterruptedException {
    // The consumer group has never committed on either topic
    AdminClient mockAdmin = mockKafka(Pair.of(Map.of(), Map.of()));
    MetadataChangeProposalConfig.ThrottlesConfig throttleConfig = noSchedulerConfig().getThrottle();
    throttleConfig.getVersioned().setThreshold(1);

    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(throttleConfig)
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build();

    ListAppender<ILoggingEvent> logs = captureLogs();
    try {
      test.refresh();
      test.refresh();
    } finally {
      stopCapturingLogs(logs);
    }

    assertEquals(
        test.getLag(),
        Map.of(
            ThrottleType.MCL_VERSIONED_LAG, 0L,
            ThrottleType.MCL_TIMESERIES_LAG, 0L));
    assertFalse(test.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
    assertTrue(logs(logs, Level.WARN, LOST_OFFSETS).isEmpty(), "Nothing was lost, so no warning");
    for (String topic : STANDARD_TOPICS) {
      assertEquals(
          logs(logs, Level.INFO, NO_OFFSETS + topic).size(),
          1,
          "Expected one line per topic, not one per refresh: " + topic);
    }
  }

  @Test
  public void testOffsetsVanishingAfterLagWasSeenReportsNoLagAndWarnsOncePerTransition()
      throws ExecutionException, InterruptedException {
    // Both topics, lag 1, 2, 3 -> median 2. Refreshes see: lag, no offsets, no offsets, lag
    // again, no offsets. Versioned throttling is on, timeseries throttling is off.
    Pair<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, Long>> lag =
        generateLag(
            STANDARD_TOPICS,
            topicPart -> (long) topicPart.partition() + 1,
            topicPart -> ((long) topicPart.partition() + 1) * 2,
            3);
    AdminClient mockAdmin = mockKafka(lag);
    KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> consumerFuture = mock(KafkaFuture.class);
    when(consumerFuture.get())
        .thenReturn(lag.getFirst(), Map.of(), Map.of(), lag.getFirst(), Map.of());
    ListConsumerGroupOffsetsResult offsetsResult = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(consumerFuture);
    when(mockAdmin.listConsumerGroupOffsets(anyString())).thenReturn(offsetsResult);

    MetadataChangeProposalConfig.ThrottlesConfig throttleConfig = noSchedulerConfig().getThrottle();
    throttleConfig.getVersioned().setThreshold(1);
    KafkaThrottleSensor test =
        KafkaThrottleSensor.builder()
            .config(throttleConfig)
            .kafkaAdmin(mockAdmin)
            .versionedTopicName(STANDARD_TOPICS.get(0))
            .timeseriesTopicName(STANDARD_TOPICS.get(1))
            .entityRegistry(mock(EntityRegistry.class))
            .mclConsumerGroupId(STANDARD_MCL_CONSUMER_GROUP_ID)
            .build();
    String versionedTopic = STANDARD_TOPICS.get(0);
    String timeseriesTopic = STANDARD_TOPICS.get(1);

    ListAppender<ILoggingEvent> logs = captureLogs();
    try {
      test.refresh();
      assertEquals(test.getLag().get(ThrottleType.MCL_VERSIONED_LAG), 2L);
      assertTrue(test.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
      assertTrue(logs(logs, Level.WARN, LOST_OFFSETS).isEmpty());

      // Offsets vanish: zero lag, not the last value, so throttling stops; one warning naming it
      test.refresh();
      assertEquals(test.getLag().get(ThrottleType.MCL_VERSIONED_LAG), 0L);
      assertFalse(test.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
      List<String> warnings = logs(logs, Level.WARN, LOST_OFFSETS);
      assertEquals(warnings.size(), 1, warnings.toString());
      assertTrue(warnings.get(0).contains(versionedTopic), warnings.get(0));
      assertTrue(warnings.get(0).contains("last median lag was 2 messages"), warnings.get(0));
      // Timeseries throttling is off, so losing its offsets changes nothing: INFO, not WARN
      assertEquals(logs(logs, Level.INFO, NO_OFFSETS + timeseriesTopic).size(), 1);

      // Still no offsets: no repeat
      test.refresh();
      assertEquals(test.getLag().get(ThrottleType.MCL_VERSIONED_LAG), 0L);
      assertEquals(logs(logs, Level.WARN, LOST_OFFSETS).size(), 1);
      assertEquals(logs(logs, Level.INFO, NO_OFFSETS + timeseriesTopic).size(), 1);

      // Offsets come back: lag and throttling return, and the recovery is logged
      test.refresh();
      assertEquals(test.getLag().get(ThrottleType.MCL_VERSIONED_LAG), 2L);
      assertTrue(test.isThrottled(ThrottleType.MCL_VERSIONED_LAG));
      assertEquals(
          logs(logs, Level.INFO, "has committed offsets on MCL topic " + versionedTopic + ";")
              .size(),
          1,
          logs.list.toString());

      // And vanish again: warned again
      test.refresh();
      assertEquals(test.getLag().get(ThrottleType.MCL_VERSIONED_LAG), 0L);
      assertEquals(logs(logs, Level.WARN, LOST_OFFSETS + versionedTopic).size(), 2);
    } finally {
      stopCapturingLogs(logs);
    }
  }

  private static ListAppender<ILoggingEvent> captureLogs() {
    Logger logger = (Logger) LoggerFactory.getLogger(KafkaThrottleSensor.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void stopCapturingLogs(ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(KafkaThrottleSensor.class)).detachAppender(appender);
    appender.stop();
  }

  /** Messages logged at {@code level} that contain {@code text}. */
  private static List<String> logs(ListAppender<ILoggingEvent> appender, Level level, String text) {
    return appender.list.stream()
        .filter(event -> event.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(message -> message.contains(text))
        .collect(Collectors.toList());
  }

  /**
   * Refreshes every 10ms with versioned throttling on and a threshold above any lag here, so
   * throttle() never backs off between refreshes.
   */
  private static MetadataChangeProposalConfig.ThrottlesConfig highThresholdSchedulerConfig() {
    MetadataChangeProposalConfig.ThrottlesConfig throttlesConfig =
        new MetadataChangeProposalConfig.ThrottlesConfig().setUpdateIntervalMs(10);
    throttlesConfig.setVersioned(
        new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(true).setThreshold(1000));
    throttlesConfig.setTimeseries(
        new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    return throttlesConfig;
  }

  /**
   * Starts the sensor and waits for a lag of 1 on both topics. Lag is computed for the timeseries
   * topic too, even though its throttling is off.
   */
  private static void assertLagEventuallyRefreshed(KafkaThrottleSensor test)
      throws InterruptedException {
    Map<ThrottleType, Long> expected =
        Map.of(ThrottleType.MCL_VERSIONED_LAG, 1L, ThrottleType.MCL_TIMESERIES_LAG, 1L);
    try {
      test.start();
      long deadline = System.currentTimeMillis() + 5_000;
      while (!expected.equals(test.getLag()) && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(test.getLag(), expected, "Expected lag refreshed by a later run");
    } finally {
      test.stop();
    }
  }

  private static MetadataChangeProposalConfig noSchedulerConfig() {
    MetadataChangeProposalConfig config = new MetadataChangeProposalConfig();
    MetadataChangeProposalConfig.ThrottlesConfig throttlesConfig =
        new MetadataChangeProposalConfig.ThrottlesConfig()
            .setUpdateIntervalMs(0); // no scheduler, manual update
    throttlesConfig.setVersioned(
        new MetadataChangeProposalConfig.ThrottleConfig()
            .setEnabled(true) // enable 1 throttle config to activate
        );
    throttlesConfig.setTimeseries(
        new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    config.setThrottle(throttlesConfig);
    return config;
  }

  private static Pair<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, Long>>
      generateLag(
          Collection<String> topicNames,
          Function<TopicPartition, Long> consumerOffset,
          Function<TopicPartition, Long> endOffset,
          int partitions) {

    Set<TopicPartition> topicPartitions =
        topicNames.stream()
            .flatMap(
                topicName ->
                    IntStream.range(0, partitions)
                        .mapToObj(partitionNum -> new TopicPartition(topicName, partitionNum)))
            .collect(Collectors.toSet());

    Map<TopicPartition, OffsetAndMetadata> consumerOffsetMap =
        topicPartitions.stream()
            .map(
                topicPartition ->
                    Map.entry(
                        topicPartition,
                        new OffsetAndMetadata(consumerOffset.apply(topicPartition))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<TopicPartition, Long> endOffsetMap =
        topicPartitions.stream()
            .map(topicPartition -> Map.entry(topicPartition, endOffset.apply(topicPartition)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return Pair.of(consumerOffsetMap, endOffsetMap);
  }

  private static AdminClient mockKafka(
      Pair<Map<TopicPartition, OffsetAndMetadata>, Map<TopicPartition, Long>> offsetPair)
      throws ExecutionException, InterruptedException {

    AdminClient mockKafkaAdmin = mock(AdminClient.class);

    // consumer offsets
    ListConsumerGroupOffsetsResult mockConsumerOffsetsResult =
        mock(ListConsumerGroupOffsetsResult.class);
    KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> mockConsumerFuture =
        mock(KafkaFuture.class);
    when(mockConsumerOffsetsResult.partitionsToOffsetAndMetadata()).thenReturn(mockConsumerFuture);
    when(mockConsumerFuture.get()).thenReturn(offsetPair.getFirst());
    when(mockKafkaAdmin.listConsumerGroupOffsets(anyString()))
        .thenReturn(mockConsumerOffsetsResult);

    // end offsets
    ListOffsetsResult mockOffsetsResult = mock(ListOffsetsResult.class);
    KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> mockOffsetFuture =
        mock(KafkaFuture.class);
    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resultMap =
        offsetPair.getSecond().entrySet().stream()
            .map(
                entry -> {
                  ListOffsetsResult.ListOffsetsResultInfo mockInfo =
                      mock(ListOffsetsResult.ListOffsetsResultInfo.class);
                  when(mockInfo.offset()).thenReturn(entry.getValue());
                  return Map.entry(entry.getKey(), mockInfo);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    when(mockOffsetFuture.get()).thenReturn(resultMap);
    when(mockOffsetsResult.all()).thenReturn(mockOffsetFuture);
    when(mockKafkaAdmin.listOffsets(anyMap())).thenReturn(mockOffsetsResult);

    return mockKafkaAdmin;
  }
}
