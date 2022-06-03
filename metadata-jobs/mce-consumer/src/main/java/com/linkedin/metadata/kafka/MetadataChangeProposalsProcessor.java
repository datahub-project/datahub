package com.linkedin.metadata.kafka;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.Topics;
import java.io.IOException;
import javax.annotation.Nonnull;
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
@Import({RestliEntityClientFactory.class, SystemAuthenticationFactory.class, KafkaEventConsumerFactory.class,
    DataHubKafkaProducerFactory.class})
@Conditional(MetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class MetadataChangeProposalsProcessor {

  private final Authentication systemAuthentication;
  private final RestliEntityClient entityClient;
  private final Producer<String, IndexedRecord> kafkaProducer;

  private final Histogram kafkaLagStats = MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  @Value("${FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.FAILED_METADATA_CHANGE_PROPOSAL + "}")
  private String fmcpTopicName;

  @KafkaListener(id = "${METADATA_CHANGE_PROPOSAL_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client}", topics =
      "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL
          + "}", containerFactory = "kafkaEventConsumer")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
    final GenericRecord record = consumerRecord.value();
    log.debug("Record {}", record);

    MetadataChangeProposal event = new MetadataChangeProposal();
    try {
      event = EventUtils.avroToPegasusMCP(record);
      log.debug("MetadataChangeProposal {}", event);
      // TODO: Get this from the event itself.
      entityClient.ingestProposal(event, this.systemAuthentication);
    } catch (Throwable throwable) {
      log.error("MCP Processor Error", throwable);
      log.error("Message: {}", record);
      sendFailedMCP(event, throwable);
    }
  }

  private void sendFailedMCP(@Nonnull MetadataChangeProposal event, @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal failedMetadataChangeProposal = createFailedMCPEvent(event, throwable);
    try {
      final GenericRecord genericFailedMCERecord = EventUtils.pegasusToAvroFailedMCP(failedMetadataChangeProposal);
      log.debug("Sending FailedMessages to topic - {}", fmcpTopicName);
      log.info("Error while processing FMCP: FailedMetadataChangeProposal - {}", failedMetadataChangeProposal);
      kafkaProducer.send(new ProducerRecord<>(fmcpTopicName, genericFailedMCERecord));
    } catch (IOException e) {
      log.error("Error while sending FailedMetadataChangeProposal: Exception  - {}, FailedMetadataChangeProposal - {}",
          e.getStackTrace(), failedMetadataChangeProposal);
    }
  }

  @Nonnull
  private FailedMetadataChangeProposal createFailedMCPEvent(@Nonnull MetadataChangeProposal event,
      @Nonnull Throwable throwable) {
    final FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setError(ExceptionUtils.getStackTrace(throwable));
    fmcp.setMetadataChangeProposal(event);
    return fmcp;
  }
}
