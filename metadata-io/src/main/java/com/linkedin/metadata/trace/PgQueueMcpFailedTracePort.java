package com.linkedin.metadata.trace;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public final class PgQueueMcpFailedTracePort implements McpFailedTracePort {

  private final MetadataQueueStore metadataQueueStore;
  private final String topicName;
  private final ExecutorService executorService;
  private final long timeoutSeconds;
  private final int peekBatchLimit;
  private final int peekMaxRounds;
  private final Deserializer<GenericRecord> avroDeserializer;

  public PgQueueMcpFailedTracePort(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nonnull String topicName,
      @Nonnull ExecutorService executorService,
      long timeoutSeconds,
      int peekBatchLimit,
      int peekMaxRounds,
      @Nonnull Deserializer<GenericRecord> avroDeserializer) {
    this.metadataQueueStore = metadataQueueStore;
    this.topicName = topicName;
    this.executorService = executorService;
    this.timeoutSeconds = timeoutSeconds;
    this.peekBatchLimit = peekBatchLimit;
    this.peekMaxRounds = peekMaxRounds;
    this.avroDeserializer = avroDeserializer;
  }

  @Override
  @Nonnull
  public Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>
      findMessages(
          @Nonnull Map<Urn, List<String>> urnAspectPairs,
          @Nonnull String traceId,
          @Nullable Long traceTimestampMillis) {

    List<
            CompletableFuture<
                Map.Entry<
                    Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>>>
        futures =
            urnAspectPairs.entrySet().stream()
                .map(
                    entry ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              try {
                                Optional<QueueTopicMetadata> topic =
                                    metadataQueueStore.fetchTopic(topicName);
                                if (topic.isEmpty()) {
                                  return Map.entry(
                                      entry.getKey(),
                                      Collections
                                          .<String,
                                              Pair<
                                                  ConsumerRecord<String, GenericRecord>,
                                                  SystemMetadata>>
                                              emptyMap());
                                }
                                long topicId = topic.get().id();
                                int partition =
                                    PgQueueTracePartitionUtil.partitionForUrn(
                                        entry.getKey(), topic.get().partitionCount());
                                return Map.entry(
                                    entry.getKey(),
                                    findMessagesOneUrn(
                                        entry.getKey(),
                                        entry.getValue(),
                                        traceId,
                                        traceTimestampMillis,
                                        topicId,
                                        partition));
                              } catch (Exception e) {
                                log.error("Error processing trace for URN: {}", entry.getKey(), e);
                                return Map.entry(
                                    entry.getKey(),
                                    Collections
                                        .<String,
                                            Pair<
                                                ConsumerRecord<String, GenericRecord>,
                                                SystemMetadata>>
                                            emptyMap());
                              }
                            },
                            executorService))
                .collect(Collectors.toList());

    try {
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .thenApply(
              v ->
                  futures.stream()
                      .map(CompletableFuture::join)
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a)))
          .get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Error processing parallel trace requests", e);
      throw new RuntimeException("Failed to process parallel trace requests", e);
    }
  }

  private Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>
      findMessagesOneUrn(
          Urn urn,
          List<String> aspectNames,
          String traceId,
          Long traceTimestampMillis,
          long topicId,
          int partition) {

    long cursor = inclusivePeekLowerBound(topicId, partition, traceTimestampMillis);
    Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> results =
        new HashMap<>();
    List<String> remaining = new ArrayList<>(aspectNames);

    for (int round = 0; round < peekMaxRounds && !remaining.isEmpty(); round++) {
      List<QueueLogPeekRow> rows =
          metadataQueueStore.peekTopicLog(
              topicId, Collections.singletonMap(partition, cursor), peekBatchLimit);
      if (rows.isEmpty()) {
        break;
      }
      long maxSeq = cursor;
      for (QueueLogPeekRow row : rows) {
        maxSeq = Math.max(maxSeq, row.handle().enqueueSeq());
        if (!Objects.equals(row.routingKey(), urn.toString())) {
          continue;
        }
        GenericRecord decoded = decode(row);
        ConsumerRecord<String, GenericRecord> cr =
            new ConsumerRecord<>(
                topicName, partition, row.handle().enqueueSeq(), row.routingKey(), decoded);
        for (String aspectName : new ArrayList<>(remaining)) {
          matchFailed(cr, traceId, aspectName)
              .ifPresent(
                  p -> {
                    results.put(aspectName, p);
                    remaining.remove(aspectName);
                  });
        }
      }
      if (maxSeq < cursor) {
        break;
      }
      cursor = maxSeq + 1;
    }
    return results;
  }

  private long inclusivePeekLowerBound(long topicId, int partition, Long traceTimestampMillis) {
    if (traceTimestampMillis != null) {
      OptionalLong atOrAfter =
          metadataQueueStore.minEnqueueSeqAtOrAfter(
              topicId, partition, java.time.Instant.ofEpochMilli(traceTimestampMillis));
      if (atOrAfter.isPresent()) {
        return atOrAfter.getAsLong();
      }
    }
    return metadataQueueStore.minEnqueueSeq(topicId, partition).orElse(1L);
  }

  private GenericRecord decode(QueueLogPeekRow row) {
    byte[] inner = PgQueuePayloadCodec.decode(row.payload(), row.payloadCompression());
    return avroDeserializer.deserialize(topicName, inner);
  }

  private static Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> matchFailed(
      ConsumerRecord<String, GenericRecord> consumerRecord, String traceId, String aspectName) {
    try {
      return Optional.ofNullable(
              consumerRecord.value() == null
                  ? null
                  : EventUtils.avroToPegasusFailedMCP(consumerRecord.value()))
          .filter(
              event ->
                  KafkaTraceReader.traceIdMatch(
                          event.getMetadataChangeProposal().getSystemMetadata(), traceId)
                      && aspectName.equals(event.getMetadataChangeProposal().getAspectName()))
          .map(
              event ->
                  Pair.of(consumerRecord, event.getMetadataChangeProposal().getSystemMetadata()));
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nonnull
  public Optional<FailedMetadataChangeProposal> read(@Nullable GenericRecord genericRecord) {
    try {
      return Optional.ofNullable(
          genericRecord == null ? null : EventUtils.avroToPegasusFailedMCP(genericRecord));
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }
}
