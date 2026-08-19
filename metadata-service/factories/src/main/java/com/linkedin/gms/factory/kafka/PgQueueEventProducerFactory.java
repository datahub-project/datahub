package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.event.PgQueueEventProducer;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.TopicConvention;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * When {@code datahub.messaging.transport=pgqueue}, GMS must emit the same MCL/MCP/PE events as
 * Kafka, but into PostgreSQL via {@link MetadataQueueStore} (see {@link PgQueueEventProducer}).
 * This is the Postgres-backed counterpart to {@link DataHubKafkaEventProducerFactory}; together
 * they are the only producers of the {@code kafkaEventProducer} bean.
 */
@Configuration
@Conditional(PgQueueMessagingTransportCondition.class)
@ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
public class PgQueueEventProducerFactory {

  @Primary
  @Bean(name = "kafkaEventProducer")
  public EventProducer kafkaEventProducer(
      MetadataQueueStore metadataQueueStore,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) TopicConvention topicConvention,
      SchemaRegistryService schemaRegistryService,
      ObjectProvider<PostgresSqlSetupProperties> postgresSqlSetupPropertiesProvider,
      ObjectProvider<ConfigurationProvider> configurationProvider) {
    PostgresSqlSetupProperties pgProps = postgresSqlSetupPropertiesProvider.getIfAvailable();
    ConfigurationProvider cp = configurationProvider.getIfAvailable();
    KafkaConfiguration kafka = cp != null ? cp.getKafka() : null;
    PgQueueSetupOptions queueOpts = pgProps != null ? pgProps.buildPgQueueOptions(kafka) : null;
    QueueTopicDefaults defaults =
        queueOpts != null
            ? QueueTopicDefaults.fromPgQueueSetup(queueOpts)
            : new QueueTopicDefaults(1, 0, 0L, 0L, false, null);
    if (pgProps == null) {
      throw new IllegalStateException(
          "PostgresSqlSetupProperties is required when datahub.messaging.transport=pgqueue");
    }
    String payloadCompressionRaw = pgProps.getPgQueue().getEffectivePayloadCompression();
    if (payloadCompressionRaw == null || payloadCompressionRaw.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgQueue.producer.payloadCompression must be set (see application.yaml; default is SNAPPY via DATAHUB_PGQUEUE_PAYLOAD_COMPRESSION)");
    }
    PgQueuePayloadCompression payloadCompression =
        PgQueuePayloadCompression.fromConfig(payloadCompressionRaw);
    return new PgQueueEventProducer(
        metadataQueueStore, topicConvention, schemaRegistryService, defaults, payloadCompression);
  }
}
