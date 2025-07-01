package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.SIMPLE_EVENT_CONSUMER_NAME;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.metadata.kafka.config.DataHubUsageEventsProcessorCondition;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.JsonElasticEvent;
import com.linkedin.metadata.kafka.transformer.DataHubUsageEventTransformer;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableKafka
@Conditional(DataHubUsageEventsProcessorCondition.class)
@Import({SimpleKafkaConsumerFactory.class})
public class DataHubUsageEventsProcessor {

  private final ElasticsearchConnector elasticSearchConnector;
  private final DataHubUsageEventTransformer dataHubUsageEventTransformer;
  private final String indexName;
  private final OperationContext systemOperationContext;

  private final Histogram kafkaLagStats =
      MetricUtils.get().histogram(MetricRegistry.name(this.getClass(), "kafkaLag"));

  public DataHubUsageEventsProcessor(
      ElasticsearchConnector elasticSearchConnector,
      DataHubUsageEventTransformer dataHubUsageEventTransformer,
      IndexConvention indexConvention,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    this.elasticSearchConnector = elasticSearchConnector;
    this.dataHubUsageEventTransformer = dataHubUsageEventTransformer;
    this.indexName = indexConvention.getIndexName("datahub_usage_event");
    this.systemOperationContext = systemOperationContext;
  }

  @KafkaListener(
      id = "${DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_ID:datahub-usage-event-consumer-job-client}",
      topics = "${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}",
      containerFactory = SIMPLE_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, String> consumerRecord) {
    systemOperationContext.withSpan(
        "consume",
        () -> {
          kafkaLagStats.update(System.currentTimeMillis() - consumerRecord.timestamp());
          final String record = consumerRecord.value();

          log.info(
              "Got DUE event key: {}, topic: {}, partition: {}, offset: {}, value size: {}, timestamp: {}",
              consumerRecord.key(),
              consumerRecord.topic(),
              consumerRecord.partition(),
              consumerRecord.offset(),
              consumerRecord.serializedValueSize(),
              consumerRecord.timestamp());

          Optional<DataHubUsageEventTransformer.TransformedDocument> eventDocument =
              dataHubUsageEventTransformer.transformDataHubUsageEvent(record);
          if (eventDocument.isEmpty()) {
            log.warn("Failed to apply usage events transform to record: {}", record);
            return;
          }
          JsonElasticEvent elasticEvent = new JsonElasticEvent(eventDocument.get().getDocument());
          elasticEvent.setId(
              generateDocumentId(eventDocument.get().getId(), consumerRecord.offset()));
          elasticEvent.setIndex(indexName);
          elasticEvent.setActionType(ChangeType.CREATE);
          elasticSearchConnector.feedElasticEvent(elasticEvent);
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "consume"));
  }

  /**
   * DataHub Usage Event is written to an append-only index called a data stream. Due to
   * circumstances it is possible that the event's id, even though it contains an epoch millisecond,
   * results in duplicate ids in the index. The collisions will stall processing of the topic. To
   * prevent the collisions we append the last 5 digits, padded with zeros, of the kafka offset to
   * prevent the collision.
   *
   * @param eventId the event's id
   * @param kafkaOffset the kafka offset for the message
   * @return unique identifier for event
   */
  private static String generateDocumentId(String eventId, long kafkaOffset) {
    return URLEncoder.encode(
        String.format("%s_%05d", eventId, leastSignificant(kafkaOffset, 5)),
        StandardCharsets.UTF_8);
  }

  private static int leastSignificant(long kafkaOffset, int digits) {
    final String input = String.valueOf(kafkaOffset);
    if (input.length() > digits) {
      return Integer.parseInt(input.substring(input.length() - digits));
    } else {
      return Integer.parseInt(input);
    }
  }
}
