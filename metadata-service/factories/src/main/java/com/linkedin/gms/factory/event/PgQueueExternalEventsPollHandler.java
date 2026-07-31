package com.linkedin.gms.factory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import io.datahubproject.event.ExternalEventsOffsetCodec;
import io.datahubproject.event.ExternalEventsPollHandler;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * External Events API backed by pgQueue message tables (same Avro payloads as Kafka; uses Schema
 * Registry for deserialization).
 */
@Slf4j
public class PgQueueExternalEventsPollHandler implements ExternalEventsPollHandler {

  private final MetadataQueueStore store;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  private final Deserializer<GenericRecord> deserializer;
  private final ObjectMapper objectMapper;
  @Nullable private final PgQueueSetupOptions pgQueueSetupOptions;

  public PgQueueExternalEventsPollHandler(
      @Nonnull MetadataQueueStore store,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull Deserializer<GenericRecord> deserializer,
      @Nonnull ObjectMapper objectMapper,
      @Nullable KafkaConfiguration kafkaConfiguration) {
    this.store = store;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.deserializer = deserializer;
    this.objectMapper = objectMapper;
    this.pgQueueSetupOptions = postgresSqlSetupProperties.buildPgQueueOptions(kafkaConfiguration);
  }

  @Override
  public ExternalEvents poll(
      @Nonnull String physicalTopicName,
      @Nullable String offsetId,
      int limit,
      long timeoutMs,
      @Nullable Integer lookbackWindowDays)
      throws Exception {

    QueueTopicDefaults defaults =
        pgQueueSetupOptions != null
            ? QueueTopicDefaults.resolveForTopic(pgQueueSetupOptions, physicalTopicName)
            : new QueueTopicDefaults(1, 0, 0L, 0L, false, null);

    Optional<QueueTopicMetadata> metaOpt = store.fetchTopic(physicalTopicName);
    final long topicId;
    final int partitionCount;
    if (metaOpt.isPresent()) {
      topicId = metaOpt.get().id();
      partitionCount = metaOpt.get().partitionCount();
    } else {
      topicId = store.ensureTopic(physicalTopicName, defaults);
      partitionCount = store.fetchTopic(physicalTopicName).orElseThrow().partitionCount();
    }

    Map<TopicPartition, Long> partitionOffsets =
        resolvePartitionOffsets(
            physicalTopicName, topicId, partitionCount, offsetId, lookbackWindowDays);

    Map<Integer, Long> pgBounds = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> e : partitionOffsets.entrySet()) {
      pgBounds.put(e.getKey().partition(), e.getValue());
    }

    List<GenericRecord> messages = new ArrayList<>();
    Map<TopicPartition, Long> latestOffsets = new LinkedHashMap<>(partitionOffsets);
    long startTime = System.currentTimeMillis();

    while (messages.size() < limit) {
      long remaining = timeoutMs - (System.currentTimeMillis() - startTime);
      if (remaining <= 0) {
        break;
      }
      int batchLimit = limit - messages.size();
      List<QueueLogPeekRow> rows = store.peekTopicLog(topicId, pgBounds, batchLimit);
      if (rows.isEmpty()) {
        try {
          Thread.sleep(Math.min(100L, Math.max(1L, remaining)));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        continue;
      }
      for (QueueLogPeekRow row : rows) {
        byte[] avroWire = PgQueuePayloadCodec.decode(row.payload(), row.payloadCompression());
        GenericRecord record = deserializer.deserialize(physicalTopicName, avroWire);
        if (record == null) {
          continue;
        }
        messages.add(record);
        TopicPartition tp = new TopicPartition(physicalTopicName, row.handle().partitionId());
        long next = row.handle().enqueueSeq() + 1;
        latestOffsets.put(tp, next);
        pgBounds.put(row.handle().partitionId(), next);
        if (messages.size() >= limit) {
          break;
        }
      }
    }

    return ExternalEventsService.buildExternalEvents(
        messages,
        ExternalEventsOffsetCodec.encodeOffsetId(latestOffsets, objectMapper),
        messages.size());
  }

  private Map<TopicPartition, Long> resolvePartitionOffsets(
      String physicalTopicName,
      long topicId,
      int partitionCount,
      @Nullable String offsetId,
      @Nullable Integer lookbackWindowDays)
      throws Exception {

    Map<TopicPartition, Long> out = new LinkedHashMap<>();
    if (offsetId != null) {
      return ExternalEventsOffsetCodec.decodeOffsetId(offsetId, objectMapper);
    }
    if (lookbackWindowDays != null && lookbackWindowDays > 0) {
      log.info(
          "No offset id provided, will lookback {} days for topic {}",
          lookbackWindowDays,
          physicalTopicName);
      long targetMillis = System.currentTimeMillis() - (lookbackWindowDays * 24L * 60 * 60 * 1000);
      Instant minInstant = Instant.ofEpochMilli(targetMillis);
      Map<Integer, Long> tails = store.partitionNextExclusiveSeqs(topicId, partitionCount);
      for (int p = 0; p < partitionCount; p++) {
        OptionalLong at = store.minEnqueueSeqAtOrAfter(topicId, p, minInstant);
        long startSeq;
        if (at.isPresent()) {
          startSeq = at.getAsLong();
        } else {
          OptionalLong earliest = store.minEnqueueSeq(topicId, p);
          startSeq = earliest.isPresent() ? earliest.getAsLong() : tails.get(p);
        }
        out.put(new TopicPartition(physicalTopicName, p), startSeq);
      }
      return out;
    }

    Map<Integer, Long> tails = store.partitionNextExclusiveSeqs(topicId, partitionCount);
    for (int p = 0; p < partitionCount; p++) {
      out.put(new TopicPartition(physicalTopicName, p), tails.get(p));
    }
    return out;
  }
}
