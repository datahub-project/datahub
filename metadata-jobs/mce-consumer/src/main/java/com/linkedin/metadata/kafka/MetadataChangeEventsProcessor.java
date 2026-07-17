package com.linkedin.metadata.kafka;

import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.event.PgQueueEventProducer;
import com.linkedin.metadata.kafka.config.MetadataChangeEventsProcessorCondition;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.FailedMetadataChangeEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.Topics;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(MetadataChangeEventsProcessorCondition.class)
@Import({RestliEntityClientFactory.class})
public class MetadataChangeEventsProcessor {

  @NonNull private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  @Nullable private final Producer<String, IndexedRecord> kafkaProducer;
  @Nullable private final EventProducer kafkaEventProducer;

  @Value(
      "${FAILED_METADATA_CHANGE_EVENT_NAME:${KAFKA_FMCE_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_EVENT
          + "}}")
  private String fmceTopicName;

  @Value("${METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:mce-consumer-job-client}")
  private String mceConsumerGroupId;

  @Autowired
  public MetadataChangeEventsProcessor(
      @NonNull OperationContext systemOperationContext,
      SystemEntityClient entityClient,
      @Autowired(required = false) @Qualifier("kafkaProducer")
          Producer<String, IndexedRecord> kafkaProducer,
      @Autowired(required = false) @Qualifier("kafkaEventProducer")
          EventProducer kafkaEventProducer) {
    this.systemOperationContext = systemOperationContext;
    this.entityClient = entityClient;
    this.kafkaProducer = kafkaProducer;
    this.kafkaEventProducer = kafkaEventProducer;
  }

  /**
   * Used by {@link MetadataChangeEventsKafkaListener} (Kafka only; pgQueue does not consume MCE).
   */
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    InboundMetadataEnvelope<GenericRecord> envelope =
        InboundMetadataEnvelope.fromKafka(consumerRecord, mceConsumerGroupId);
    systemOperationContext.withSpan(
        "consume",
        () -> {
          systemOperationContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      MetricUtils.recordInboundMessageQueueLag(
                          metricUtils,
                          this.getClass(),
                          envelope.getLogicalTopic(),
                          envelope.getConsumerGroupId() != null
                              ? envelope.getConsumerGroupId()
                              : mceConsumerGroupId,
                          envelope.getEnqueuedAtMillis(),
                          envelope.getMessagingSystem(),
                          envelope.getPriority()));
          processGenericRecord(envelope.getPayload(), envelope.getEnqueuedAtMillis());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  private void processGenericRecord(GenericRecord record, long timestampMillis) {
    log.info(
        "Got MCE event (timestamp: {}) value size implied by Avro processing", timestampMillis);

    log.debug("Record {}", record);

    MetadataChangeEvent event = new MetadataChangeEvent();

    try {
      event = EventUtils.avroToPegasusMCE(record);
      log.debug("MetadataChangeEvent {}", event);
      if (event.hasProposedSnapshot()) {
        processProposedSnapshot(event);
      }
    } catch (Throwable throwable) {
      log.error("MCE Processor Error", throwable);
      log.error("Message: {}", record);
      sendFailedMCE(event, throwable);
    }
  }

  private void sendFailedMCE(@Nonnull MetadataChangeEvent event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeEvent failedMetadataChangeEvent =
        createFailedMCEEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord =
          EventUtils.pegasusToAvroFailedMCE(failedMetadataChangeEvent);
      log.debug("Sending FailedMessages to topic - {}", fmceTopicName);
      log.info(
          "Error while processing MCE: FailedMetadataChangeEvent - {}", failedMetadataChangeEvent);
      if (kafkaProducer != null) {
        kafkaProducer.send(new ProducerRecord<>(fmceTopicName, genericFailedMCERecord));
      } else if (kafkaEventProducer instanceof PgQueueEventProducer pq) {
        pq.publishRawTopicConfluentAvro(fmceTopicName, "", genericFailedMCERecord, "FMCE");
      } else {
        log.error(
            "Cannot emit FailedMetadataChangeEvent: no kafkaProducer and kafkaEventProducer is not PgQueueEventProducer");
      }
    } catch (IOException e) {
      log.error(
          "Error while sending FailedMetadataChangeEvent: Exception  - {}, FailedMetadataChangeEvent - {}",
          e.getStackTrace(),
          failedMetadataChangeEvent);
    }
  }

  @Nonnull
  private FailedMetadataChangeEvent createFailedMCEEvent(
      @Nonnull MetadataChangeEvent event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeEvent fmce = new FailedMetadataChangeEvent();
    fmce.setError(ExceptionUtils.getStackTrace(throwable));
    fmce.setMetadataChangeEvent(event);
    return fmce;
  }

  @Deprecated
  private void processProposedSnapshot(@Nonnull MetadataChangeEvent metadataChangeEvent)
      throws RemoteInvocationException {
    final Snapshot snapshotUnion = metadataChangeEvent.getProposedSnapshot();
    final Entity entity = new Entity().setValue(snapshotUnion);
    entityClient.updateWithSystemMetadata(
        systemOperationContext, entity, metadataChangeEvent.getSystemMetadata());
  }
}
