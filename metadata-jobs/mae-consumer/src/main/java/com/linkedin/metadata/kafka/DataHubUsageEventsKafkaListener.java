package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.SIMPLE_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.DataHubUsageEventsProcessorCondition;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka transport entrypoint for {@link DataHubUsageEventsProcessor}. Partitions the inbound batch
 * into affinity slices via {@link InboundBatchAffinityResolver} (single OSS slice by default;
 * deployments that register a custom resolver get one slice per affinity group) and dispatches each
 * slice independently through {@link DataHubUsageEventsProcessor#consume(OperationContext, List)}.
 *
 * <p>pgQueue uses {@link com.linkedin.metadata.kafka.config.PgQueueMaePollerSourcesConfiguration}
 * which calls the legacy {@code consume(topic, messages)} entry point on the processor.
 */
@Component
@KafkaMessagingEnabled
@Conditional(DataHubUsageEventsProcessorCondition.class)
@Import({SimpleKafkaConsumerFactory.class})
@EnableKafka
public class DataHubUsageEventsKafkaListener {

  private final DataHubUsageEventsProcessor processor;
  private final InboundBatchAffinityResolver batchAffinityResolver;
  private final OperationContext systemOperationContext;

  @Value(DataHubUsageEventsProcessor.DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE)
  private String consumerGroupId;

  @Autowired
  public DataHubUsageEventsKafkaListener(
      @Nonnull final DataHubUsageEventsProcessor processor,
      @Nonnull final InboundBatchAffinityResolver batchAffinityResolver,
      @Qualifier("systemOperationContext") @Nonnull final OperationContext systemOperationContext) {
    this.processor = processor;
    this.batchAffinityResolver = batchAffinityResolver;
    this.systemOperationContext = systemOperationContext;
  }

  @KafkaListener(
      id = DataHubUsageEventsProcessor.DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE,
      topics = "${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}",
      containerFactory = SIMPLE_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, String>> consumerRecords) {
    if (consumerRecords.isEmpty()) {
      return;
    }
    // Compute partitions once; the OSS default returns a single slice carrying the system context.
    final List<InboundBatchAffinityResolver.Slice<String>> slices =
        batchAffinityResolver.partition(consumerRecords, consumerGroupId, systemOperationContext);
    for (InboundBatchAffinityResolver.Slice<String> slice : slices) {
      processor.consume(slice.context(), slice.records());
    }
  }
}
