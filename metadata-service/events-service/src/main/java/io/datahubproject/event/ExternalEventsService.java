package io.datahubproject.event;

import io.datahubproject.event.exception.UnsupportedTopicException;
import io.datahubproject.event.models.v1.ExternalEvent;
import io.datahubproject.event.models.v1.ExternalEvents;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

@Slf4j
public class ExternalEventsService {

  public static final String PLATFORM_EVENT_TOPIC_NAME = "PlatformEvent_v1";
  public static final String METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME =
      "MetadataChangeLog_Versioned_v1";
  public static final String METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME =
      "MetadataChangeLog_Timeseries_v1";

  private final Map<String, String> topicNames;
  private final Set<String> pollAllowedTopics;
  private final ExternalEventsPollHandler pollHandler;
  private final int defaultPollTimeoutSeconds;
  private final int defaultLimit;

  public ExternalEventsService(
      @Nonnull Set<String> pollAllowedTopics,
      @Nonnull ExternalEventsPollHandler pollHandler,
      @Nonnull Map<String, String> topicNames,
      final int defaultPollTimeoutSeconds,
      final int defaultLimit) {
    this.pollAllowedTopics =
        Objects.requireNonNull(pollAllowedTopics, "pollAllowedTopics must not be null");
    this.pollHandler = Objects.requireNonNull(pollHandler, "pollHandler must not be null");
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
   */
  public ExternalEvents poll(
      @Nonnull final String topic,
      @Nullable final String offsetId,
      @Nullable final Integer limit,
      @Nullable final Integer pollTimeoutSeconds,
      @Nullable final Integer lookbackWindowDays)
      throws Exception {
    if (!pollAllowedTopics.contains(topic)) {
      throw new UnsupportedTopicException(topic);
    }
    String finalTopic = remapExternalTopicName(topic);
    long timeoutMs =
        (pollTimeoutSeconds != null ? pollTimeoutSeconds : defaultPollTimeoutSeconds) * 1000L;
    int lim = limit != null ? limit : defaultLimit;
    return pollHandler.poll(finalTopic, offsetId, lim, timeoutMs, lookbackWindowDays);
  }

  public static ExternalEvents buildExternalEvents(
      final List<GenericRecord> messages, final String newOffsetId, long count) {

    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setOffsetId(newOffsetId);
    externalEvents.setCount(count);

    List<ExternalEvent> externalEventList =
        messages.stream()
            .map(
                message -> {
                  ExternalEvent externalEvent = new ExternalEvent();
                  externalEvent.setContentType("application/json");
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

  public void shutdown() {
    pollHandler.shutdown();
  }

  private String remapExternalTopicName(@Nonnull final String topicName) {
    if (this.topicNames.containsKey(topicName)) {
      return this.topicNames.get(topicName);
    }
    throw new UnsupportedTopicException(topicName);
  }

  public static String convertGenericRecordToJson(GenericRecord record) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());

    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    outputStream.flush();

    return outputStream.toString();
  }
}
