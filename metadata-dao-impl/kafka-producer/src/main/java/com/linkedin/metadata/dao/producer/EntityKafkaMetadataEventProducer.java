package com.linkedin.metadata.dao.producer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.Configs;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * <p>The topic names that this emits to can be controlled by constructing this with a {@link TopicConvention}.
 * If none is given, defaults to a {@link TopicConventionImpl} with the default delimiter of an underscore (_).
 */
@Slf4j
public class EntityKafkaMetadataEventProducer implements EntityEventProducer {

  private final Producer<String, ? extends IndexedRecord> _producer;
  private final Optional<Callback> _callback;
  private final TopicConvention _topicConvention;

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   */
  public EntityKafkaMetadataEventProducer(@Nonnull final Producer<String, ? extends IndexedRecord> producer,
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
  public EntityKafkaMetadataEventProducer(@Nonnull final Producer<String, ? extends IndexedRecord> producer,
      @Nonnull final TopicConvention topicConvention, @Nullable final Callback callback) {
    _producer = producer;
    _callback = Optional.ofNullable(callback);
    _topicConvention = topicConvention;
  }

  @Override
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
      log.debug(String.format(String.format("Converting Pegasus snapshot to Avro snapshot urn %s", urn),
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
  public void produceMetadataChangeLog(@Nonnull final Urn urn, @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    GenericRecord record;
    try {
      log.debug(String.format(String.format("Converting Pegasus snapshot to Avro snapshot urn %s", urn),
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
  public void produceAspectSpecificMetadataAuditEvent(@Nonnull final Urn urn, @Nullable final RecordTemplate oldValue,
      @Nonnull final RecordTemplate newValue, @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata, @Nonnull final MetadataAuditOperation operation) {
    // TODO switch to convention once versions are annotated in the schema
    final String topicKey = ModelUtils.getAspectSpecificMAETopicName(urn, newValue);
    if (!isValidAspectSpecificTopic(topicKey)) {
      log.warn("The event topic for entity {} and aspect {}, expected to be {}, has not been registered.",
          urn.getClass().getCanonicalName(), newValue.getClass().getCanonicalName(), topicKey);
      return;
    }

    String topic;
    Class<? extends SpecificRecord> maeAvroClass;
    RecordTemplate metadataAuditEvent;
    try {
      topic = (String) Topics.class.getField(topicKey).get(null);
      maeAvroClass = Configs.TOPIC_SCHEMA_CLASS_MAP.get(topic);
      metadataAuditEvent = (RecordTemplate) EventUtils.getPegasusClass(maeAvroClass).newInstance();

      metadataAuditEvent.getClass().getMethod("setUrn", urn.getClass()).invoke(metadataAuditEvent, urn);
      metadataAuditEvent.getClass().getMethod("setNewValue", newValue.getClass()).invoke(metadataAuditEvent, newValue);
      if (oldValue != null) {
        metadataAuditEvent.getClass()
            .getMethod("setOldValue", oldValue.getClass())
            .invoke(metadataAuditEvent, oldValue);
      }
    } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
        | InstantiationException | InvocationTargetException e) {
      throw new IllegalArgumentException("Failed to compose the Pegasus aspect specific MAE", e);
    }

    GenericRecord record;
    try {
      record = EventUtils.pegasusToAvroAspectSpecificMXE(maeAvroClass, metadataAuditEvent);
    } catch (NoSuchFieldException | IOException | IllegalAccessException e) {
      throw new ModelConversionException("Failed to convert Pegasus aspect specific MAE to Avro", e);
    }

    if (_callback.isPresent()) {
      _producer.send(new ProducerRecord(topic, urn.toString(), record), _callback.get());
    } else {
      _producer.send(new ProducerRecord(topic, urn.toString(), record));
    }
  }

  @VisibleForTesting
  static boolean isValidAspectSpecificTopic(@Nonnull String topic) {
    return Arrays.stream(Topics.class.getFields()).anyMatch(field -> field.getName().equals(topic));
  }
}
