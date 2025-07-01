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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class KafkaThrottleSensorTest {
  private static final List<String> STANDARD_TOPICS =
      List.of(Topics.METADATA_CHANGE_LOG_VERSIONED, Topics.METADATA_CHANGE_LOG_TIMESERIES);
  private static final String STANDARD_MCL_CONSUMER_GROUP_ID = "generic-mae-consumer-job-client";

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
