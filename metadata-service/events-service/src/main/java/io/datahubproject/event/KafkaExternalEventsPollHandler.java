package io.datahubproject.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.event.kafka.CheckedConsumer;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.naming.TimeLimitExceededException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class KafkaExternalEventsPollHandler implements ExternalEventsPollHandler {

  private final KafkaConsumerPool consumerPool;
  private final ObjectMapper objectMapper;

  public KafkaExternalEventsPollHandler(
      @Nonnull KafkaConsumerPool consumerPool, @Nonnull ObjectMapper objectMapper) {
    this.consumerPool = consumerPool;
    this.objectMapper = objectMapper;
  }

  @Override
  public ExternalEvents poll(
      @Nonnull String physicalTopicName,
      @Nullable String offsetId,
      int limit,
      long timeoutMs,
      @Nullable Integer lookbackWindowDays)
      throws Exception {
    List<GenericRecord> messages = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    CheckedConsumer checkedConsumer =
        consumerPool.borrowConsumer(timeoutMs, TimeUnit.MILLISECONDS, physicalTopicName);
    if (checkedConsumer == null) {
      throw new TimeLimitExceededException("Too many simultaneous requests, retry again later.");
    }

    try {
      KafkaConsumer<String, GenericRecord> consumer = checkedConsumer.getConsumer();
      List<TopicPartition> partitions =
          consumer.partitionsFor(physicalTopicName).stream()
              .map(
                  partitionInfo -> new TopicPartition(physicalTopicName, partitionInfo.partition()))
              .collect(Collectors.toList());
      consumer.assign(partitions);

      Map<TopicPartition, Long> partitionOffsets =
          getPartitionOffsets(
              physicalTopicName, offsetId, consumer, partitions, lookbackWindowDays);

      for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue());
      }

      Map<TopicPartition, Long> latestOffsets = new HashMap<>(partitionOffsets);
      int fetchedRecords = 0;

      while (fetchedRecords < limit) {
        long timeRemaining = timeoutMs - (System.currentTimeMillis() - startTime);
        if (timeRemaining <= 0L) {
          break;
        }
        ConsumerRecords<String, GenericRecord> records =
            consumer.poll(Duration.ofMillis(Math.min(1000, timeRemaining)));
        for (ConsumerRecord<String, GenericRecord> record : records) {
          messages.add(record.value());
          latestOffsets.put(
              new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
          fetchedRecords++;
          if (fetchedRecords >= limit) {
            break;
          }
        }
      }

      return ExternalEventsService.buildExternalEvents(
          messages,
          ExternalEventsOffsetCodec.encodeOffsetId(latestOffsets, objectMapper),
          fetchedRecords);
    } finally {
      consumerPool.returnConsumer(checkedConsumer);
    }
  }

  @Override
  public void shutdown() {
    consumerPool.shutdownPool();
  }

  private Map<TopicPartition, Long> getPartitionOffsets(
      @Nonnull String topic,
      @Nullable String offsetId,
      @Nonnull KafkaConsumer<String, GenericRecord> consumer,
      @Nonnull List<TopicPartition> partitions,
      @Nullable Integer lookbackWindowDays)
      throws Exception {
    Map<TopicPartition, Long> partitionOffsets;
    if (offsetId == null) {
      if (lookbackWindowDays != null && lookbackWindowDays > 0) {
        log.info(
            "No offset id provided, will lookback {} days for topic {}", lookbackWindowDays, topic);
        partitionOffsets = seekToNDaysAgo(consumer, partitions, lookbackWindowDays);
      } else {
        consumer.seekToEnd(partitions);
        partitionOffsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
          long offset = consumer.position(partition);
          partitionOffsets.put(partition, offset);
        }
      }
    } else {
      partitionOffsets = ExternalEventsOffsetCodec.decodeOffsetId(offsetId, objectMapper);
    }
    return partitionOffsets;
  }

  private Map<TopicPartition, Long> seekToNDaysAgo(
      @Nonnull KafkaConsumer<String, GenericRecord> consumer,
      @Nonnull List<TopicPartition> partitions,
      int daysAgo) {
    long currentTimeMillis = System.currentTimeMillis();
    long targetTimestamp = currentTimeMillis - (daysAgo * 24 * 60 * 60 * 1000L);

    final Map<TopicPartition, Long> timestampsToSearch =
        partitions.stream()
            .collect(Collectors.toMap(partition -> partition, partition -> targetTimestamp));

    final Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
        consumer.offsetsForTimes(timestampsToSearch, Duration.ofSeconds(10));

    final Map<TopicPartition, Long> partitionOffsets = new HashMap<>();

    for (TopicPartition partition : partitions) {
      OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
      if (offsetAndTimestamp != null) {
        long offset = offsetAndTimestamp.offset();
        consumer.seek(partition, offset);
        partitionOffsets.put(partition, offset);
      } else {
        consumer.seekToBeginning(java.util.Collections.singleton(partition));
        long earliestOffset = consumer.position(partition);
        partitionOffsets.put(partition, earliestOffset);
      }
    }

    return partitionOffsets;
  }
}
