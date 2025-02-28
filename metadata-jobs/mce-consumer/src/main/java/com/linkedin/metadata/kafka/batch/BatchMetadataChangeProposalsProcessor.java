package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class BatchMetadataChangeProposalsProcessor {
  private final OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;
  private final EventProducer kafkaProducer;

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

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    KafkaListenerUtil.registerThrottle(kafkaThrottle, provider, registry, mceConsumerGroupId);
  }

  @KafkaListener(
      id = MCP_CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = MCP_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    List<MetadataChangeProposal> metadataChangeProposals = new ArrayList<>(consumerRecords.size());
    String topicName = null;

    for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
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

      if (topicName == null) {
        topicName = consumerRecord.topic();
      }

      final MetadataChangeProposal event;
      try {
        event = EventUtils.avroToPegasusMCP(record);
        metadataChangeProposals.add(event);
      } catch (IOException e) {
        log.error(
            "Unrecoverable message deserialization error. Cannot forward to failure topic.", e);
      }
    }

    List<SystemMetadata> systemMetadataList =
        metadataChangeProposals.stream().map(MetadataChangeProposal::getSystemMetadata).toList();
    systemOperationContext.withQueueSpan(
        "consume",
        systemMetadataList,
        topicName,
        () -> {
          try {
            List<String> urns =
                entityClient.batchIngestProposals(
                    systemOperationContext, metadataChangeProposals, false);
            log.info("Successfully processed MCP event urns: {}", urns);
          } catch (Throwable throwable) {
            log.error("MCP Processor Error", throwable);
            Span currentSpan = Span.current();
            currentSpan.recordException(throwable);
            currentSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
            currentSpan.setAttribute(MetricUtils.ERROR_TYPE, throwable.getClass().getName());

            kafkaProducer.produceFailedMetadataChangeProposal(
                systemOperationContext, metadataChangeProposals, throwable);
          }
        },
        BATCH_SIZE_ATTR,
        String.valueOf(metadataChangeProposals.size()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }
}
