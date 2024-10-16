package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Import({RestliEntityClientFactory.class})
@Conditional(MetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class MetadataChangeProposalsProcessor {
  private static final String CONSUMER_GROUP_ID_VALUE =
      "${METADATA_CHANGE_PROPOSAL_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client}";

  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final Producer<String, IndexedRecord> kafkaProducer;

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final KafkaListenerEndpointRegistry registry;
  private final ConfigurationProvider provider;

  private final Histogram kafkaLagStats =
      MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Value(
      "${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:"
          + Topics.FAILED_METADATA_CHANGE_PROPOSAL
          + "}")
  private String fmcpTopicName;

  @Value(CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    if (kafkaThrottle != null
        && provider
            .getMetadataChangeProposal()
            .getThrottle()
            .getComponents()
            .getMceConsumer()
            .isEnabled()) {
      log.info("MCE Consumer Throttle Enabled");
      kafkaThrottle.addCallback(
          (throttleEvent) -> {
            Optional<MessageListenerContainer> container =
                Optional.ofNullable(registry.getListenerContainer(mceConsumerGroupId));
            if (container.isEmpty()) {
              log.warn(
                  "Expected container was missing: {} throttle is not possible.",
                  mceConsumerGroupId);
            } else {
              if (throttleEvent.isThrottled()) {
                container.ifPresent(MessageListenerContainer::pause);
                return ThrottleControl.builder()
                    // resume consumer after sleep
                    .callback(
                        (resumeEvent) -> container.ifPresent(MessageListenerContainer::resume))
                    .build();
              }
            }

            return ThrottleControl.NONE;
          });
    } else {
      log.info("MCE Consumer Throttle Disabled");
    }
  }

  @KafkaListener(
      id = CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "consume").time()) {
      kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
      final GenericRecord record = consumerRecord.value();

      log.info(
          "Got MCP event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
          consumerRecord.key(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.serializedValueSize(),
          consumerRecord.timestamp());

      log.debug("Record {}", record);

      MetadataChangeProposal event = new MetadataChangeProposal();
      try {
        event = EventUtils.avroToPegasusMCP(record);
        log.debug("MetadataChangeProposal {}", event);
        // TODO: Get this from the event itself.
        String urn = entityClient.ingestProposal(systemOperationContext, event, false);
        log.info("Successfully processed MCP event urn: {}", urn);
      } catch (Throwable throwable) {
        log.error("MCP Processor Error", throwable);
        log.error("Message: {}", record);
        sendFailedMCP(event, throwable);
      }
    }
  }

  private void sendFailedMCP(@Nonnull MetadataChangeProposal event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal failedMetadataChangeProposal =
        createFailedMCPEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord =
          EventUtils.pegasusToAvroFailedMCP(failedMetadataChangeProposal);
      log.debug("Sending FailedMessages to topic - {}", fmcpTopicName);
      log.info(
          "Error while processing FMCP: FailedMetadataChangeProposal - {}",
          failedMetadataChangeProposal);
      kafkaProducer.send(new ProducerRecord<>(fmcpTopicName, genericFailedMCERecord));
    } catch (IOException e) {
      log.error(
          "Error while sending FailedMetadataChangeProposal: Exception  - {}, FailedMetadataChangeProposal - {}",
          e.getStackTrace(),
          failedMetadataChangeProposal);
    }
  }

  @Nonnull
  private FailedMetadataChangeProposal createFailedMCPEvent(
      @Nonnull MetadataChangeProposal event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setError(ExceptionUtils.getStackTrace(throwable));
    fmcp.setMetadataChangeProposal(event);
    return fmcp;
  }
}
