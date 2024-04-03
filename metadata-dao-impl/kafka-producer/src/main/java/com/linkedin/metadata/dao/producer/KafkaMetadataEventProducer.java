package com.linkedin.metadata.dao.producer;

import com.datahub.metadata.dao.producer.BaseMetadataEventProducer;
import com.datahub.util.ModelUtils;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * A Kafka implementation of {@link BaseMetadataEventProducer}.
 *
 * <p>The topic names that this emits to can be controlled by constructing this with a {@link
 * TopicConvention}. If none is given, defaults to a {@link TopicConventionImpl} with the default
 * delimiter of an underscore (_).
 */
@Slf4j
public class KafkaMetadataEventProducer<
        SNAPSHOT extends RecordTemplate, ASPECT_UNION extends UnionTemplate, URN extends Urn>
    extends BaseMetadataEventProducer<SNAPSHOT, ASPECT_UNION, URN> {

  private final Producer<String, ? extends IndexedRecord> _producer;
  private final Optional<Callback> _callback;
  private final TopicConvention _topicConvention;

  /**
   * Constructor.
   *
   * @param snapshotClass The snapshot class for the produced events
   * @param aspectUnionClass The aspect union in the snapshot
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   */
  public KafkaMetadataEventProducer(
      @Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention) {
    this(snapshotClass, aspectUnionClass, producer, topicConvention, null);
  }

  /**
   * Constructor.
   *
   * @param snapshotClass The snapshot class for the produced events
   * @param aspectUnionClass The aspect union in the snapshot
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param callback The {@link Callback} to invoke when the request is completed
   */
  public KafkaMetadataEventProducer(
      @Nonnull Class<SNAPSHOT> snapshotClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Producer<String, ? extends IndexedRecord> producer,
      @Nonnull TopicConvention topicConvention,
      @Nullable Callback callback) {
    super(snapshotClass, aspectUnionClass);
    _producer = producer;
    _callback = Optional.ofNullable(callback);
    _topicConvention = topicConvention;
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceSnapshotBasedMetadataChangeEvent(
      @Nonnull URN urn, @Nonnull ASPECT newValue) {
    MetadataChangeEvent metadataChangeEvent = new MetadataChangeEvent();
    metadataChangeEvent.setProposedSnapshot(makeSnapshot(urn, newValue));

    GenericRecord record;
    try {
      record = EventUtils.pegasusToAvroMCE(metadataChangeEvent);
    } catch (IOException e) {
      throw new ModelConversionException("Failed to convert Pegasus MCE to Avro", e);
    }

    if (_callback.isPresent()) {
      _producer.send(
          new ProducerRecord(
              _topicConvention.getMetadataChangeEventTopicName(), urn.toString(), record),
          _callback.get());
    } else {
      _producer.send(
          new ProducerRecord(
              _topicConvention.getMetadataChangeEventTopicName(), urn.toString(), record));
    }
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceMetadataAuditEvent(
      @Nonnull URN urn, @Nullable ASPECT oldValue, @Nonnull ASPECT newValue) {

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
      _producer.send(
          new ProducerRecord(
              _topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record),
          _callback.get());
    } else {
      _producer.send(
          new ProducerRecord(
              _topicConvention.getMetadataAuditEventTopicName(), urn.toString(), record));
    }
  }

  @Override
  public <ASPECT extends RecordTemplate> void produceAspectSpecificMetadataAuditEvent(
      @Nonnull URN urn, @Nullable ASPECT oldValue, @Nonnull ASPECT newValue) {
    // Aspect Specific MAE not supported.
    // TODO: Remove references to this class.
    throw new UnsupportedOperationException();
  }

  @Nonnull
  private Snapshot makeSnapshot(@Nonnull URN urn, @Nonnull RecordTemplate value) {
    Snapshot snapshot = new Snapshot();

    List<ASPECT_UNION> aspects = new ArrayList<>();
    aspects.add(ModelUtils.newAspectUnion(_aspectUnionClass, value));
    RecordUtils.setSelectedRecordTemplateInUnion(
        snapshot, ModelUtils.newSnapshot(_snapshotClass, urn, aspects));
    return snapshot;
  }

  @VisibleForTesting
  static boolean isValidAspectSpecificTopic(@Nonnull String topic) {
    return Arrays.stream(Topics.class.getFields()).anyMatch(field -> field.getName().equals(topic));
  }
}
