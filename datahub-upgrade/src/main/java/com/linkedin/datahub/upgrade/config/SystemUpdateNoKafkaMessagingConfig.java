package com.linkedin.datahub.upgrade.config;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.KafkaMessagingDisabledCondition;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.event.PgQueueEventProducer;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.TopicConvention;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * When {@link com.linkedin.metadata.config.messaging.MessagingTransport} is not Kafka, System
 * Update must not connect to a broker (compose omits {@code kafka-broker} for pgQueue Postgres
 * stacks). We register {@link PgQueueEventProducer} as {@code duheKafkaEventProducer} so the {@code
 * DataHubStartupStep} marker is recorded on the {@code DataHubUpgradeHistory_v1} pgQueue topic. All
 * dependencies are required: this configuration only activates when transport is pgQueue, and the
 * producer must be a real Postgres-backed implementation (never a no-op).
 */
@Configuration
@Conditional(KafkaMessagingDisabledCondition.class)
public class SystemUpdateNoKafkaMessagingConfig {

  @Bean(name = "duheKafkaEventProducer")
  public EventProducer duheKafkaEventProducer(
      @Nonnull final MetadataQueueStore metadataQueueStore,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) @Nonnull
          final TopicConvention topicConvention,
      @Nonnull final SchemaRegistryService schemaRegistryService,
      @Nonnull final PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull final ConfigurationProvider configurationProvider) {
    final KafkaConfiguration kafkaConfiguration = configurationProvider.getKafka();
    final PgQueueSetupOptions pgQueueOptions =
        postgresSqlSetupProperties.buildPgQueueOptions(kafkaConfiguration);
    if (pgQueueOptions == null) {
      throw new IllegalStateException(
          "datahub.messaging.transport is non-kafka but postgres.pgQueue.enabled=false; "
              + "enable pgQueue or switch transport back to kafka.");
    }
    final QueueTopicDefaults defaults = QueueTopicDefaults.fromPgQueueSetup(pgQueueOptions);
    String payloadCompressionRaw =
        postgresSqlSetupProperties.getPgQueue().getEffectivePayloadCompression();
    if (payloadCompressionRaw == null || payloadCompressionRaw.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgQueue.producer.payloadCompression must be set (see application.yaml)");
    }
    PgQueuePayloadCompression payloadCompression =
        PgQueuePayloadCompression.fromConfig(payloadCompressionRaw);
    return new PgQueueEventProducer(
        metadataQueueStore, topicConvention, schemaRegistryService, defaults, payloadCompression);
  }
}
