package com.linkedin.gms.factory.kafka.throttle;

import static com.linkedin.gms.factory.kafka.common.AdminClientFactory.buildKafkaAdminClient;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.datahub.metadata.dao.throttle.PgQueueThrottleSensor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.MessagingTransport;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.mxe.Topics;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
public class KafkaThrottleFactory {

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String maeConsumerGroupId;

  @Value("${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_VERSIONED + "}")
  private String versionedTopicName;

  @Value(
      "${METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME:" + Topics.METADATA_CHANGE_LOG_TIMESERIES + "}")
  private String timeseriesTopicName;

  @Bean("kafkaThrottle")
  public ThrottleSensor kafkaThrottle(
      Environment environment,
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties kafkaProperties,
      final EntityRegistry entityRegistry,
      @Nullable MetadataQueueStore metadataQueueStore) {

    MetadataChangeProposalConfig mcpConfig = provider.getMetadataChangeProposal();
    if (mcpConfig.getThrottle().getUpdateIntervalMs() <= 0) {
      return new NoOpSensor();
    }

    String transport =
        environment.getProperty(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);

    if (MessagingTransport.PGQUEUE.equalsIgnoreCase(transport)) {
      if (metadataQueueStore == null) {
        throw new IllegalStateException(
            "datahub.messaging.transport=pgqueue but no MetadataQueueStore bean is available");
      }
      log.info("Using PgQueueThrottleSensor (datahub.messaging.transport=pgqueue)");
      return PgQueueThrottleSensor.builder()
          .metadataQueueStore(metadataQueueStore)
          .config(mcpConfig.getThrottle())
          .mclConsumerGroupId(maeConsumerGroupId)
          .timeseriesTopicName(timeseriesTopicName)
          .versionedTopicName(versionedTopicName)
          .build()
          .start();
    }

    if (!MessagingTransport.KAFKA.equalsIgnoreCase(transport)) {
      log.info(
          "Kafka/PgQueue throttle sensor not used (datahub.messaging.transport={}); using NoOpSensor",
          transport);
      return new NoOpSensor();
    }

    KafkaConfiguration kafkaConfiguration = provider.getKafka();

    if (!hasResolvableKafkaBootstrap(kafkaConfiguration, kafkaProperties)) {
      log.warn(
          "Kafka bootstrap servers are not configured; Kafka throttle sensor disabled (NoOpSensor)");
      return new NoOpSensor();
    }

    return KafkaThrottleSensor.builder()
        .entityRegistry(entityRegistry)
        .kafkaAdmin(buildKafkaAdminClient(kafkaConfiguration, kafkaProperties, "throttle-sensor"))
        .config(mcpConfig.getThrottle())
        .mclConsumerGroupId(maeConsumerGroupId)
        .timeseriesTopicName(timeseriesTopicName)
        .versionedTopicName(versionedTopicName)
        .build()
        .start();
  }

  /** Same resolution path as {@link com.linkedin.gms.factory.kafka.common.AdminClientFactory}. */
  private static boolean hasResolvableKafkaBootstrap(
      KafkaConfiguration kafkaConfiguration, KafkaProperties kafkaProperties) {
    String fromProducer =
        kafkaConfiguration.getProducer() != null
            ? kafkaConfiguration.getProducer().getBootstrapServers()
            : null;
    String bootstrapServers =
        StringUtils.isNotBlank(fromProducer)
            ? fromProducer
            : kafkaConfiguration.getBootstrapServers();
    if (StringUtils.isNotBlank(bootstrapServers)) {
      return true;
    }
    return kafkaProperties.getBootstrapServers() != null
        && !kafkaProperties.getBootstrapServers().isEmpty();
  }
}
