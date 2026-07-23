package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.event.PgQueueUsageEventPublisher;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueueUsageEventPublisherConfiguration {

  @Bean(name = {"usageEventPublisher", "dataHubUsageEventProducer"})
  public UsageEventPublisher pgQueueUsageEventPublisher(
      MetadataQueueStore metadataQueueStore,
      ObjectProvider<PostgresSqlSetupProperties> postgresSqlSetupPropertiesProvider,
      ObjectProvider<ConfigurationProvider> configurationProvider) {
    PostgresSqlSetupProperties pgProps = postgresSqlSetupPropertiesProvider.getIfAvailable();
    ConfigurationProvider cp = configurationProvider.getIfAvailable();
    KafkaConfiguration kafka = cp != null ? cp.getKafka() : null;
    PgQueueSetupOptions queueOpts = pgProps != null ? pgProps.buildPgQueueOptions(kafka) : null;
    QueueTopicDefaults defaults =
        queueOpts != null
            ? QueueTopicDefaults.fromPgQueueSetup(queueOpts)
            : new QueueTopicDefaults(
                1, 0, 0L, 0L, false, PgQueueUsageEventPublisher.JSON_CONTENT_TYPE);
    if (pgProps == null) {
      throw new IllegalStateException(
          "PostgresSqlSetupProperties is required when datahub.messaging.transport=pgqueue");
    }
    String payloadCompressionRaw = pgProps.getPgQueue().getEffectivePayloadCompression();
    if (payloadCompressionRaw == null || payloadCompressionRaw.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgQueue.producer.payloadCompression must be set (see application.yaml)");
    }
    PgQueuePayloadCompression payloadCompression =
        PgQueuePayloadCompression.fromConfig(payloadCompressionRaw);
    PgQueueUsageEventPublisher publisher =
        new PgQueueUsageEventPublisher(metadataQueueStore, queueOpts, defaults, payloadCompression);
    if (cp != null && cp.getDatahub().isReadOnly()) {
      publisher.setWritable(false);
    }
    return publisher;
  }
}
