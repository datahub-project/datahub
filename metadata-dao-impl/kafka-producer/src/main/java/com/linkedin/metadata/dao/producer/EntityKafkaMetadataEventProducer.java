package com.linkedin.metadata.dao.producer;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.Configs;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * A Kafka implementation of {@link BaseMetadataEventProducer}.
 *
 * <p>The topic names that this emits to can be controlled by constructing this with a {@link TopicConvention}. If
 * none is given, defaults to a {@link TopicConventionImpl} with the default delimiter of an underscore (_).
 */
@Slf4j
public class EntityKafkaMetadataEventProducer {

  private final Producer<String, ? extends IndexedRecord> _producer;
  private final Optional<Callback> _callback;
  private final TopicConvention _topicConvention;
  private final EntityRegistry _entityRegistry;

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   */
  public EntityKafkaMetadataEventProducer(
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention) {
    this(producer, topicConvention, null, null);
  }

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   */
  public EntityKafkaMetadataEventProducer(
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention,
      @Nonnull EntityRegistry entityRegistry) {
    this(producer, topicConvention, entityRegistry, null);
  }

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param callback The {@link Callback} to invoke when the request is completed
   */
  public EntityKafkaMetadataEventProducer(
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention,
      @Nullable Callback callback) {

    this(producer, topicConvention, null, callback);
  }

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param callback The {@link Callback} to invoke when the request is completed
   */
  public EntityKafkaMetadataEventProducer(
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable Callback callback) {
    _producer = producer;
    _callback = Optional.ofNullable(callback);
    _topicConvention = topicConvention;
    _entityRegistry = entityRegistry;
  }

  public void produceSnapshotBasedMetadataChangeEvent(
      @Nonnull Urn urn,
      @Nonnull RecordTemplate newValue) {
    MetadataChangeEvent metadataChangeEvent = new MetadataChangeEvent();
    metadataChangeEvent.setProposedSnapshot(makeSnapshot(urn, newValue));

    GenericRecord record;
    try {
      record = EventUtils.pegasusToAvroMCE(metadataChangeEvent);
    } catch (IOException e) {
      throw new ModelConversionException("Failed to convert Pegasus MCE to Avro", e);
    }

    if (_callback.isPresent()) {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataChangeEventTopicName(), urn.toString(), record),
          _callback.get());
    } else {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataChangeEventTopicName(), urn.toString(), record));
    }
  }

  public void produceMetadataAuditEvent(
      @Nonnull Urn urn,
      @Nullable RecordTemplate oldValue,
      @Nonnull RecordTemplate newValue) {

    MetadataAuditEvent metadataAuditEvent = new MetadataAuditEvent();
    metadataAuditEvent.setNewSnapshot(makeSnapshot(urn, newValue));
    if (oldValue != null) {
      metadataAuditEvent.setOldSnapshot(makeSnapshot(urn, oldValue));
    }

    GenericRecord record;
    try {
      record = EventUtils.pegasusToAvroMAE(metadataAuditEvent);
    } catch (IOException e) {
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    if (_callback.isPresent()) {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record),
          _callback.get());
    } else {
      _producer.send(new ProducerRecord(_topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record));
    }
  }

  public void produceAspectSpecificMetadataAuditEvent(
      @Nonnull Urn urn,
      @Nullable RecordTemplate oldValue,
      @Nonnull RecordTemplate newValue) {
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

  @Nonnull
  private Snapshot makeSnapshot(@Nonnull Urn urn, @Nonnull RecordTemplate value) {

    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(urn.getEntityType());
    final Class<? extends UnionTemplate> aspectUnionClass = getUnionTemplateClass(entitySpec.getAspectTyperefSchema());

    final Snapshot snapshot = new Snapshot();
    final List<UnionTemplate> aspects = new ArrayList<>();

    aspects.add(ModelUtils.newAspectUnion(aspectUnionClass, value));

    // We are using the Generic Dao :) --> Eventually this should just be required. It is duplicated with other information.
    final Optional<AspectSpec> maybeKeySpec = entitySpec.getAspectSpecs().stream().filter(spec -> spec.isKey()).findFirst();

    // Currently for backwards compat: If there is a key spec, then add the key aspect.
    if (maybeKeySpec.isPresent()) {
      final AspectSpec keySpec = maybeKeySpec.get();
      final RecordDataSchema keySchema = keySpec.getPegasusSchema();
      final RecordTemplate keyRecord = EntityKeyUtils.convertUrnToEntityKey(urn, keySchema);
      aspects.add(ModelUtils.newAspectUnion(aspectUnionClass, keyRecord));
    }

    RecordUtils.setSelectedRecordTemplateInUnion(snapshot, ModelUtils.newSnapshot(getRecordTemplateClass(entitySpec.getSnapshotSchema()), urn, aspects));
    return snapshot;
  }

  private Class<? extends UnionTemplate> getUnionTemplateClass(final NamedDataSchema schema) {
    try {
      return Class.forName(schema.getFullName()).asSubclass(UnionTemplate.class);
    } catch (ClassNotFoundException e) {
      throw new ModelConversionException(
          String.format("Failed to find union template associated with schema named %s", schema.getFullName()));
    }
  }

  private Class<? extends RecordTemplate> getRecordTemplateClass(final RecordDataSchema schema) {
    try {
      return Class.forName(schema.getFullName()).asSubclass(RecordTemplate.class);
    } catch (ClassNotFoundException e) {
      throw new ModelConversionException(
          String.format("Failed to find record template associated with aspect schema named %s", schema.getFullName()));
    }
  }

  @VisibleForTesting
  static boolean isValidAspectSpecificTopic(@Nonnull String topic) {
    return Arrays.stream(Topics.class.getFields()).anyMatch(field -> field.getName().equals(topic));
  }
}
