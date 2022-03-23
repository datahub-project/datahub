package com.linkedin.metadata.dao.producer;

import com.datahub.util.exception.ModelConversionException;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * <p>The topic names that this emits to can be controlled by constructing this with a {@link TopicConvention}.
 * If none is given, defaults to a {@link TopicConventionImpl} with the default delimiter of an underscore (_).
 */
@Slf4j
public class KafkaEventProducer implements EventProducer {

  private final Producer<String, ? extends IndexedRecord> _producer;
  private final Optional<Callback> _callback;
  private final TopicConvention _topicConvention;

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   */
  public KafkaEventProducer(@Nonnull final Producer<String, ? extends IndexedRecord> producer,
      @Nonnull final TopicConvention topicConvention) {
    this(producer, topicConvention, null);
  }

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param callback The {@link Callback} to invoke when the request is completed
   */
  public KafkaEventProducer(@Nonnull final Producer<String, ? extends IndexedRecord> producer,
      @Nonnull final TopicConvention topicConvention, @Nullable final Callback callback) {
    _producer = producer;
    _callback = Optional.ofNullable(callback);
    _topicConvention = topicConvention;
  }

  @Override
  @WithSpan
  @Deprecated
  public void produceMetadataAuditEvent(@Nonnull Urn urn, @Nullable Snapshot oldSnapshot, @Nonnull Snapshot newSnapshot,
      @Nullable SystemMetadata oldSystemMetadata, @Nullable SystemMetadata newSystemMetadata,
      MetadataAuditOperation operation) {
    final MetadataAuditEvent metadataAuditEvent = new MetadataAuditEvent();
    if (newSnapshot != null) {
      metadataAuditEvent.setNewSnapshot(newSnapshot);
    }
    if (oldSnapshot != null) {
      metadataAuditEvent.setOldSnapshot(oldSnapshot);
    }
    if (oldSystemMetadata != null) {
      metadataAuditEvent.setOldSystemMetadata(oldSystemMetadata);
    }
    if (newSystemMetadata != null) {
      metadataAuditEvent.setNewSystemMetadata(newSystemMetadata);
    }
    if (operation != null) {
      metadataAuditEvent.setOperation(operation);
    }

    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataAuditEvent: %s",
          urn,
          metadataAuditEvent.toString()));
      record = EventUtils.pegasusToAvroMAE(metadataAuditEvent);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MAE to Avro: %s", metadataAuditEvent.toString()), e);
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    if (_callback.isPresent()) {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record),
          _callback.get());
    } else {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record),
          (metadata, e) -> {
            if (e != null) {
              log.error(String.format("Failed to emit MAE for entity with urn %s", urn), e);
            } else {
              log.debug(String.format("Successfully emitted MAE for entity with urn %s at offset %s, partition %s, topic %s",
                  urn,
                  metadata.offset(),
                  metadata.partition(),
                  metadata.topic()));
            }
          });
    }
  }

  @Override
  @WithSpan
  public void produceMetadataChangeLog(@Nonnull final Urn urn, @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataChangeLog: %s",
          urn,
          metadataChangeLog.toString()));
      record = EventUtils.pegasusToAvroMCL(metadataChangeLog);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MAE to Avro: %s", metadataChangeLog.toString()), e);
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    String topic = _topicConvention.getMetadataChangeLogVersionedTopicName();
    if (aspectSpec.isTimeseries()) {
      topic = _topicConvention.getMetadataChangeLogTimeseriesTopicName();
    }

    if (_callback.isPresent()) {
      _producer.send(new ProducerRecord(topic, urn.toString(), record), _callback.get());
    } else {
      _producer.send(new ProducerRecord(topic, urn.toString(), record), (metadata, e) -> {
        if (e != null) {
          log.error(String.format("Failed to emit MCL for entity with urn %s", urn), e);
        } else {
          log.debug(String.format("Successfully emitted MCL for entity with urn %s at offset %s, partition %s, topic %s",
              urn,
              metadata.offset(),
              metadata.partition(),
              metadata.topic()));
        }
      });
    }
  }

  @Override
  public void producePlatformEvent(@Nonnull String name, @Nullable String key, @Nonnull PlatformEvent event) {
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus Event to Avro Event urn %s\nEvent: %s",
          name,
          event.toString()));
      record = EventUtils.pegasusToAvroPE(event);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus Platform Event to Avro: %s", event.toString()), e);
      throw new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e);
    }

    final Callback callback = _callback.orElseGet(() -> (metadata, e) -> {
      if (e != null) {
        log.error(String.format("Failed to emit Platform Event for entity with name %s", name), e);
      } else {
        log.debug(String.format(
            "Successfully emitted Platform Event for entity with name %s at offset %s, partition %s, topic %s", name,
            metadata.offset(), metadata.partition(), metadata.topic()));
      }
    });

    final String topic = _topicConvention.getPlatformEventTopicName();
    _producer.send(new ProducerRecord(topic, key == null ? name : key, record), callback);
  }

  @VisibleForTesting
  static boolean isValidAspectSpecificTopic(@Nonnull String topic) {
    return Arrays.stream(Topics.class.getFields()).anyMatch(field -> field.getName().equals(topic));
  }
}
