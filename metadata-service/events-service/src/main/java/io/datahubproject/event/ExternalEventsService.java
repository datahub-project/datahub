package io.datahubproject.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.event.exception.UnsupportedTopicException;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import io.datahubproject.event.models.v1.ExternalEvent;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class ExternalEventsService {

  public static final String PLATFORM_EVENT_TOPIC_NAME = "PlatformEvent_v1";
  private static final Set<String> ALLOWED_TOPICS = Set.of(PLATFORM_EVENT_TOPIC_NAME);
  private final KafkaConsumerPool consumerPool;
  private final ObjectMapper objectMapper;
  private final Map<String, String>
      topicNames; // Mapping of standard topic name to customer-specific topic name.
  private final int defaultPollTimeoutSeconds;
  private final int defaultLimit;

  public ExternalEventsService(
      @Nonnull final KafkaConsumerPool consumerPool,
      @Nonnull final ObjectMapper objectMapper,
      @Nonnull final Map<String, String> topicNames,
      final int defaultPollTimeoutSeconds,
      final int defaultLimit) {
    this.consumerPool = Objects.requireNonNull(consumerPool, "consumerPool must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.topicNames = Objects.requireNonNull(topicNames, "topicNames must not be null");
    this.defaultPollTimeoutSeconds = defaultPollTimeoutSeconds;
    this.defaultLimit = defaultLimit;
  }

  /**
   * Fetches a batch of events from the given topic starting from the given offset ID.
   *
   * @param topic the topic to fetch events from
   * @param offsetId the string offset ID (encodes partition-specific offsets)
   * @param limit the maximum number of events to fetch
   * @param pollTimeoutSeconds the maximum amount of time to wait on the poll request
   * @param lookbackWindowDays the number of days to look-back from latest if no offset id is
   *     provided.
   * @return the set of events fetched, along with the new offset ID
   * @throws Exception if there's an error decoding the offsetId or interacting with Kafka
   */
  public ExternalEvents poll(
      @Nonnull final String topic,
      @Nullable final String offsetId,
      @Nullable final Integer limit,
      @Nullable final Integer pollTimeoutSeconds,
      @Nullable final Integer lookbackWindowDays)
      throws Exception {
    if (!ALLOWED_TOPICS.contains(topic)) {
      throw new UnsupportedTopicException(topic);
    }

    String finalTopic = remapExternalTopicName(topic);

    KafkaConsumer<String, GenericRecord> consumer = consumerPool.borrowConsumer();
    List<GenericRecord> messages = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    long timeout =
        (pollTimeoutSeconds != null ? pollTimeoutSeconds : defaultPollTimeoutSeconds) * 1000L;

    try {
      List<TopicPartition> partitions =
          consumer.partitionsFor(finalTopic).stream()
              .map(partitionInfo -> new TopicPartition(finalTopic, partitionInfo.partition()))
              .collect(Collectors.toList());
      consumer.assign(partitions);

      Map<TopicPartition, Long> partitionOffsets =
          getPartitionOffsets(topic, offsetId, consumer, partitions, lookbackWindowDays);

      for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
        consumer.seek(entry.getKey(), entry.getValue());
      }

      Map<TopicPartition, Long> latestOffsets = new HashMap<>(partitionOffsets);
      int fetchedRecords = 0;
      int finalLimit = limit != null ? limit : defaultLimit;

      while (fetchedRecords < finalLimit) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, GenericRecord> record : records) {
          messages.add(record.value());
          latestOffsets.put(
              new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
          fetchedRecords++;
          if (fetchedRecords >= finalLimit) {
            break;
          }
        }
        if (System.currentTimeMillis() - startTime > timeout) {
          break;
        }
      }

      return convertToExternalEvents(messages, encodeOffsetId(latestOffsets), fetchedRecords);
    } finally {
      consumerPool.returnConsumer(consumer);
    }
  }

  private Map<TopicPartition, Long> getPartitionOffsets(
      @Nonnull final String topic,
      @Nullable final String offsetId,
      @Nonnull final KafkaConsumer<String, GenericRecord> consumer,
      @Nonnull final List<TopicPartition> partitions,
      @Nullable final Integer lookbackWindowDays)
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
      partitionOffsets = decodeOffsetId(offsetId);
    }
    return partitionOffsets;
  }

  /**
   * Seeks to a specific number of days from latest of the topic.
   *
   * @param consumer the consumer to use to seek
   * @param partitions the partitions we are reading from
   * @param daysAgo number of days to consider
   * @return
   */
  private Map<TopicPartition, Long> seekToNDaysAgo(
      @Nonnull final KafkaConsumer<String, GenericRecord> consumer,
      @Nonnull final List<TopicPartition> partitions,
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
        consumer.seekToBeginning(Collections.singleton(partition));
        long earliestOffset = consumer.position(partition);
        partitionOffsets.put(partition, earliestOffset);
      }
    }

    return partitionOffsets;
  }

  /**
   * Encodes partition-specific offsets into a Base64 string for the offset ID.
   *
   * @param offsets the partition-specific offsets
   * @return the encoded offset ID (Base64-encoded)
   * @throws Exception if encoding fails
   */
  private String encodeOffsetId(final Map<TopicPartition, Long> offsets) throws Exception {
    // Convert the map to a new map with String keys instead of TopicPartition
    Map<String, Long> serializedOffsets =
        offsets.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().topic() + "-" + entry.getKey().partition(),
                    Map.Entry::getValue));

    // Convert the map to a JSON string and then Base64 encode it
    String json = objectMapper.writeValueAsString(serializedOffsets);
    return Base64.getEncoder().encodeToString(json.getBytes());
  }

  /**
   * Decodes the Base64-encoded offset ID back into partition-specific offsets.
   *
   * @param offsetId the Base64-encoded offset ID
   * @return the map of partition-specific offsets
   * @throws Exception if decoding fails
   */
  private Map<TopicPartition, Long> decodeOffsetId(final String offsetId) throws Exception {
    // Decode the Base64-encoded string to a JSON string
    String json = new String(Base64.getDecoder().decode(offsetId));

    // Deserialize the JSON string to a map with String keys (topic-partition)
    Map<String, Long> serializedOffsets =
        objectMapper.readValue(
            json,
            objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Long.class));

    // Convert the map with String keys back into a map with TopicPartition keys
    return serializedOffsets.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> {
                  String[] parts = entry.getKey().split("-");
                  return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
                },
                Map.Entry::getValue));
  }

  /**
   * Converts the raw Kafka messages into an ExternalEvents object.
   *
   * @param messages the list of raw event messages
   * @param newOffsetId the new offset ID after consuming the batch
   * @param count the number of records fetched
   * @return ExternalEvents object
   */
  private ExternalEvents convertToExternalEvents(
      final List<GenericRecord> messages, final String newOffsetId, long count) {

    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setOffsetId(newOffsetId); // New encoded offset ID
    externalEvents.setCount(count);

    List<ExternalEvent> externalEventList =
        messages.stream()
            .map(
                message -> {
                  ExternalEvent externalEvent = new ExternalEvent();
                  externalEvent.setContentType("application/json"); // Assuming JSON content type
                  try {
                    externalEvent.setValue(convertGenericRecordToJson(message));
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to convert GenericRecord to JSON!", e);
                  }
                  return externalEvent;
                })
            .collect(Collectors.toList());

    externalEvents.setEvents(externalEventList);
    return externalEvents;
  }

  public static String convertGenericRecordToJson(GenericRecord record) throws IOException {
    // Create a byte output stream to capture the JSON output
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // Create an Avro JsonEncoder
    Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);

    // Create a GenericDatumWriter for the GenericRecord
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());

    // Write the GenericRecord to JSON
    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    outputStream.flush();

    // Convert output stream to string
    return outputStream.toString();
  }

  public void shutdown() {
    // Shutdown the entire pool
    consumerPool.shutdownPool();
  }

  private String remapExternalTopicName(@Nonnull final String topicName) {
    if (this.topicNames.containsKey(topicName)) {
      return this.topicNames.get(topicName);
    }
    // No topic found.
    throw new UnsupportedTopicException(topicName);
  }
}
