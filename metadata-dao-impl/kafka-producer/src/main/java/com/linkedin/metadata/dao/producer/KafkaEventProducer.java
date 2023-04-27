package com.linkedin.metadata.dao.producer;

import com.datahub.util.exception.ModelConversionException;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.TopicConventionImpl;
import com.linkedin.mxe.Topics;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.util.Arrays;
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
  private final TopicConvention _topicConvention;
  private final KafkaHealthChecker _kafkaHealthChecker;

  /**
   * Constructor.
   *
   * @param producer The Kafka {@link Producer} to use
   * @param topicConvention the convention to use to get kafka topic names
   * @param kafkaHealthChecker The {@link Callback} to invoke when the request is completed
   */
  public KafkaEventProducer(@Nonnull final Producer<String, ? extends IndexedRecord> producer,
      @Nonnull final TopicConvention topicConvention, @Nonnull final KafkaHealthChecker kafkaHealthChecker) {
    _producer = producer;
    _topicConvention = topicConvention;
    _kafkaHealthChecker = kafkaHealthChecker;
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
          metadataAuditEvent));
      record = EventUtils.pegasusToAvroMAE(metadataAuditEvent);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MAE to Avro: %s", metadataAuditEvent), e);
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    String topic = _topicConvention.getMetadataAuditEventTopicName();
    _producer.send(new ProducerRecord(topic, urn.toString(), record),
            _kafkaHealthChecker.getKafkaCallBack("MAE", urn.toString()));
  }

  @Override
  @WithSpan
  public void produceMetadataChangeLog(@Nonnull final Urn urn, @Nonnull AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataChangeLog: %s",
          urn,
          metadataChangeLog));
      record = EventUtils.pegasusToAvroMCL(metadataChangeLog);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MAE to Avro: %s", metadataChangeLog), e);
      throw new ModelConversionException("Failed to convert Pegasus MAE to Avro", e);
    }

    String topic = _topicConvention.getMetadataChangeLogVersionedTopicName();
    if (aspectSpec.isTimeseries()) {
      topic = _topicConvention.getMetadataChangeLogTimeseriesTopicName();
    }
    _producer.send(new ProducerRecord(topic, urn.toString(), record),
            _kafkaHealthChecker.getKafkaCallBack("MCL", urn.toString()));
  }

  @Override
  @WithSpan
  public void produceMetadataChangeProposal(@Nonnull final Urn urn, @Nonnull final MetadataChangeProposal
      metadataChangeProposal) {
    GenericRecord record;

    try {
      log.debug(String.format("Converting Pegasus snapshot to Avro snapshot urn %s\nMetadataChangeProposal: %s",
          urn,
          metadataChangeProposal));
      record = EventUtils.pegasusToAvroMCP(metadataChangeProposal);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus MCP to Avro: %s", metadataChangeProposal), e);
      throw new ModelConversionException("Failed to convert Pegasus MCP to Avro", e);
    }

    String topic = _topicConvention.getMetadataChangeProposalTopicName();
    _producer.send(new ProducerRecord(topic, urn.toString(), record),
            _kafkaHealthChecker.getKafkaCallBack("MCP", urn.toString()));
  }

  @Override
  public void producePlatformEvent(@Nonnull String name, @Nullable String key, @Nonnull PlatformEvent event) {
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus Event to Avro Event urn %s\nEvent: %s",
          name,
          event));
      record = EventUtils.pegasusToAvroPE(event);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus Platform Event to Avro: %s", event), e);
      throw new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e);
    }

    final String topic = _topicConvention.getPlatformEventTopicName();
    _producer.send(new ProducerRecord(topic, key == null ? name : key, record),
            _kafkaHealthChecker.getKafkaCallBack("Platform Event", name));
  }

  @Override
  public void produceDataHubUpgradeHistoryEvent(@Nonnull DataHubUpgradeHistoryEvent event) {
    GenericRecord record;
    try {
      log.debug(String.format("Converting Pegasus Event to Avro Event\nEvent: %s", event));
      record = EventUtils.pegasusToAvroDUHE(event);
    } catch (IOException e) {
      log.error(String.format("Failed to convert Pegasus DataHub Upgrade History Event to Avro: %s", event), e);
      throw new ModelConversionException("Failed to convert Pegasus Platform Event to Avro", e);
    }

    final String topic = _topicConvention.getDataHubUpgradeHistoryTopicName();
    _producer.send(new ProducerRecord(topic, event.getVersion(), record), _kafkaHealthChecker
            .getKafkaCallBack("History Event", "Event Version: " + event.getVersion()));
  }

  @VisibleForTesting
  static boolean isValidAspectSpecificTopic(@Nonnull String topic) {
    return Arrays.stream(Topics.class.getFields()).anyMatch(field -> field.getName().equals(topic));
  }
}
