package com.linkedin.metadata.trace;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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

/**
 * MCP async trace backed by pgQueue log reads. Scans are bounded by {@link #peekBatchLimit} and
 * {@link #peekMaxRounds} per URN/partition to avoid unbounded table reads.
 */
@Slf4j
public final class PgQueueMcpPendingTracePort implements McpPendingTracePort {

  private final MetadataQueueStore metadataQueueStore;
  private final String topicName;
  private final String consumerGroupId;
  private final ExecutorService executorService;
  private final long timeoutSeconds;
  private final int peekBatchLimit;
  private final int peekMaxRounds;
  private final Deserializer<GenericRecord> avroDeserializer;

  public PgQueueMcpPendingTracePort(
      @Nonnull MetadataQueueStore metadataQueueStore,
      @Nonnull String topicName,
      @Nonnull String consumerGroupId,
      @Nonnull ExecutorService executorService,
      long timeoutSeconds,
      int peekBatchLimit,
      int peekMaxRounds,
      @Nonnull Deserializer<GenericRecord> avroDeserializer) {
    this.metadataQueueStore = metadataQueueStore;
    this.topicName = topicName;
    this.consumerGroupId = consumerGroupId;
    this.executorService = executorService;
    this.timeoutSeconds = timeoutSeconds;
    this.peekBatchLimit = peekBatchLimit;
    this.peekMaxRounds = peekMaxRounds;
    this.avroDeserializer = avroDeserializer;
  }

  @Override
  @Nonnull
  public Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      @Nonnull Map<Urn, List<String>> urnAspectPairs,
      @Nonnull String traceId,
      @Nullable Long traceTimestampMillis) {
    return tracePendingStatuses(urnAspectPairs, traceId, traceTimestampMillis, false);
  }

  @Override
  @Nonnull
  public Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      @Nonnull Map<Urn, List<String>> urnAspectPairs,
      @Nonnull String traceId,
      @Nullable Long traceTimestampMillis,
      boolean skipCache) {

    List<CompletableFuture<Map.Entry<Urn, Map<String, TraceStorageStatus>>>> futures =
        urnAspectPairs.entrySet().stream()
            .map(
                entry ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          try {
                            Map<String, TraceStorageStatus> result =
                                tracePendingStatuses(
                                    entry.getKey(),
                                    entry.getValue(),
                                    traceId,
                                    traceTimestampMillis,
                                    skipCache);
                            return Map.entry(entry.getKey(), result);
                          } catch (Exception e) {
                            log.error(
                                "Error processing trace status for URN: {}", entry.getKey(), e);
                            return Map.entry(
                                entry.getKey(), Collections.<String, TraceStorageStatus>emptyMap());
                          }
                        },
                        executorService))
            .collect(Collectors.toList());

    try {
      List<Map.Entry<Urn, Map<String, TraceStorageStatus>>> results =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
              .thenApply(
                  v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
              .get(timeoutSeconds, TimeUnit.SECONDS);

      return results.stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing));
    } catch (Exception e) {
      log.error("Error processing parallel trace status requests", e);
      throw new RuntimeException("Failed to process parallel trace status requests", e);
    }
  }

  private Map<String, TraceStorageStatus> tracePendingStatuses(
      Urn urn,
      Collection<String> aspectNames,
      String traceId,
      Long traceTimestampMillis,
      boolean skipCache) {
    Optional<QueueTopicMetadata> topic = metadataQueueStore.fetchTopic(topicName);
    if (topic.isEmpty()) {
      return aspectNames.stream()
          .collect(
              Collectors.toMap(
                  a -> a,
                  a ->
                      TraceStorageStatus.ok(
                          TraceWriteStatus.UNKNOWN, "Missing pgQueue topic metadata.")));
    }
    long topicId = topic.get().id();
    int partitionCount = topic.get().partitionCount();
    int partition = PgQueueTracePartitionUtil.partitionForUrn(urn, partitionCount);
    long committed = metadataQueueStore.getCommittedOffset(consumerGroupId, topicId, partition);

    Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> messages =
        findMessages(urn, aspectNames, traceId, traceTimestampMillis, topicId, partition);

    return aspectNames.stream()
        .collect(
            Collectors.toMap(
                aspectName -> aspectName,
                aspectName -> {
                  Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata> message =
                      messages.get(aspectName);
                  if (message != null && committed < message.getFirst().offset()) {
                    return TraceStorageStatus.ok(
                        TraceWriteStatus.PENDING, "Consumer has not processed offset.");
                  }
                  return TraceStorageStatus.fail(
                      TraceWriteStatus.ERROR, "Consumer has processed past the offset.");
                }));
  }

  private Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> findMessages(
      Urn urn,
      Collection<String> aspectNames,
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
          matchMcp(cr, traceId, aspectName)
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
              topicId, partition, Instant.ofEpochMilli(traceTimestampMillis));
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

  private static Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> matchMcp(
      ConsumerRecord<String, GenericRecord> consumerRecord, String traceId, String aspectName) {
    try {
      return Optional.ofNullable(
              consumerRecord.value() == null
                  ? null
                  : EventUtils.avroToPegasusMCP(consumerRecord.value()))
          .filter(
              event ->
                  KafkaTraceReader.traceIdMatch(event.getSystemMetadata(), traceId)
                      && aspectName.equals(event.getAspectName()))
          .map(event -> Pair.of(consumerRecord, event.getSystemMetadata()));
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }
}
