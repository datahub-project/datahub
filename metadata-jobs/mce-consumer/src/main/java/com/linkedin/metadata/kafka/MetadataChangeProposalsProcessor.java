package com.linkedin.metadata.kafka;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeEventsProcessorCondition;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@Conditional(MetadataChangeEventsProcessorCondition.class)
@EnableKafka
public class MetadataChangeProposalsProcessor {

  private EntityClient entityClient;
  private KafkaTemplate<String, GenericRecord> kafkaTemplate;

  @Value("${GENERIC_FAILED_METADATA_CHANGE_EVENT_NAME:" + Topics.FAILED_METADATA_CHANGE_PROPOSAL + "}")
  private String fmceTopicName;

  public MetadataChangeProposalsProcessor(@Nonnull final EntityClient entityClient,
      @Nonnull final KafkaTemplate<String, GenericRecord> kafkaTemplate) {
    this.entityClient = entityClient;
    this.kafkaTemplate = kafkaTemplate;
  }

  @KafkaListener(id = "${GENERIC_METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client}", topics =
      "${GENERIC_METADATA_CHANGE_EVENT_NAME:" + Topics.METADATA_CHANGE_PROPOSAL
          + "}", containerFactory = "mceKafkaContainerFactory")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    final GenericRecord record = consumerRecord.value();
    log.debug("Record {}", record);

    MetadataChangeProposal event = new MetadataChangeProposal();
    try {
      event = EventUtils.avroToPegasusMCP(record);
      log.debug("MetadataChangeEvent {}", event);
      entityClient.ingestProposal(event);
    } catch (Throwable throwable) {
      log.error("MCE Processor Error", throwable);
      log.error("Message: {}", record);
      sendFailedMCE(event, throwable);
    }
  }

  private void sendFailedMCE(@Nonnull MetadataChangeProposal event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal failedMetadataChangeEvent = createFailedMCEEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord = EventUtils.pegasusToAvroFailedMCP(failedMetadataChangeEvent);
      log.debug("Sending FailedMessages to topic - {}", fmceTopicName);
      log.info("Error while processing generic MCE: FailedMetadataChangeProposal - {}", failedMetadataChangeEvent);
      this.kafkaTemplate.send(fmceTopicName, genericFailedMCERecord);
    } catch (IOException e) {
      log.error("Error while sending FailedMetadataChangeEvent: Exception  - {}, FailedMetadataChangeEvent - {}",
          e.getStackTrace(), failedMetadataChangeEvent);
    }
  }

  @Nonnull
  private FailedMetadataChangeProposal createFailedMCEEvent(@Nonnull MetadataChangeProposal event,
      @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setError(ExceptionUtils.getStackTrace(throwable));
    fmcp.setMetadataChangeProposal(event);
    return fmcp;
  }
}
