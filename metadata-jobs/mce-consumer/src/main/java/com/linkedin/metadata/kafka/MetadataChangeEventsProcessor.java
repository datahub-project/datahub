package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.FailedMetadataChangeEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.mxe.Topics;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Conditional(MetadataChangeProposalProcessorCondition.class)
@Import({RestliEntityClientFactory.class})
@EnableKafka
@RequiredArgsConstructor
public class MetadataChangeEventsProcessor {

  @NonNull private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final Producer<String, IndexedRecord> kafkaProducer;

  private final Histogram kafkaLagStats =
      MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Value(
      "${FAILED_METADATA_CHANGE_EVENT_NAME:${KAFKA_FMCE_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_EVENT
          + "}}")
  private String fmceTopicName;

  @KafkaListener(
      id = "${METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:mce-consumer-job-client}",
      topics =
          "${METADATA_CHANGE_EVENT_NAME:${KAFKA_MCE_TOPIC_NAME:"
              + Topics.METADATA_CHANGE_EVENT
              + "}}",
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  @Deprecated
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    systemOperationContext.withSpan(
        "consume",
        () -> {
          kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
          final GenericRecord record = consumerRecord.value();

          log.info(
              "Got MCE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
              consumerRecord.key(),
              consumerRecord.topic(),
              consumerRecord.partition(),
              consumerRecord.offset(),
              consumerRecord.serializedValueSize(),
              consumerRecord.timestamp());

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
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
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
      kafkaProducer.send(new ProducerRecord<>(fmceTopicName, genericFailedMCERecord));
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
    // TODO: GMS Auth Part 2: Get the actor identity from the event header itself.
    entityClient.updateWithSystemMetadata(
        systemOperationContext, entity, metadataChangeEvent.getSystemMetadata());
  }
}
