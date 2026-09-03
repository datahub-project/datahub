package com.linkedin.metadata.queue;

import static org.testng.Assert.expectThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.testng.annotations.Test;

public class MetadataQueueStoreDefaultsTest {

  private final MetadataQueueStore store =
      new MetadataQueueStore() {
        @Override
        public Optional<QueueTopicMetadata> fetchTopic(String topicName) {
          return Optional.empty();
        }

        @Override
        public Map<Integer, Long> partitionNextExclusiveSeqs(long topicId, int partitionCount) {
          return Map.of();
        }

        @Override
        public OptionalLong minEnqueueSeqAtOrAfter(
            long topicId, int partitionId, Instant minEnqueuedAt) {
          return OptionalLong.empty();
        }

        @Override
        public OptionalLong minEnqueueSeq(long topicId, int partitionId) {
          return OptionalLong.empty();
        }

        @Override
        public List<QueueLogPeekRow> peekTopicLog(
            long topicId, Map<Integer, Long> partitionToMinExclusiveSeq, int limit) {
          return List.of();
        }

        @Override
        public long ensureTopic(String topicName, QueueTopicDefaults defaults) {
          return 0L;
        }

        @Override
        public QueueMessageHandle enqueue(
            String topicName,
            String routingKey,
            QueueTopicDefaults defaults,
            int priority,
            byte[] payload,
            Optional<String> contentType,
            List<QueueMessageHeader> headers,
            PgQueuePayloadCompression payloadCompression) {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<QueueMessageHandle> enqueueBatch(
            List<EnqueueBatchItem> items, QueueTopicDefaults defaults) {
          return List.of();
        }
      };

  @Test
  public void defaultMethods_throwUnsupported() {
    expectThrows(UnsupportedOperationException.class, () -> store.partitionMaxEnqueueSeqs(1L, 1));
    expectThrows(
        UnsupportedOperationException.class, () -> store.detectOffsetAheadOfLog("g", 1L, 1));
    expectThrows(
        UnsupportedOperationException.class,
        () -> store.receiveBatchForGroup("g", 1L, List.of(0), "owner", Duration.ofSeconds(1), 1));
    expectThrows(
        UnsupportedOperationException.class, () -> store.commitForGroup("g", List.of(), true));
    expectThrows(
        UnsupportedOperationException.class,
        () -> store.extendVisibilityForGroup("g", List.of(), "owner", Duration.ofSeconds(1)));
    expectThrows(UnsupportedOperationException.class, () -> store.getCommittedOffset("g", 1L, 0));
    expectThrows(UnsupportedOperationException.class, () -> store.registerConsumer("g", 1L));
    expectThrows(UnsupportedOperationException.class, () -> store.listRegisteredConsumers(1L));
    expectThrows(UnsupportedOperationException.class, () -> store.unregisterConsumer("g", 1L));
    expectThrows(
        UnsupportedOperationException.class,
        () ->
            store.resetConsumerOffsets(
                ConsumerOffsetResetSpec.builder().consumerGroup("g").topicName("t").build()));
    expectThrows(UnsupportedOperationException.class, () -> store.applyRetention());
  }
}
