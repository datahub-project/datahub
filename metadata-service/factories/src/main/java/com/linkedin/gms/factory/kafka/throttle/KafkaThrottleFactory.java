package com.linkedin.gms.factory.kafka.throttle;

import com.datahub.metadata.dao.throttle.KafkaThrottleSensor;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.Topics;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties kafkaProperties,
      final EntityRegistry entityRegistry) {

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    MetadataChangeProposalConfig mcpConfig = provider.getMetadataChangeProposal();

    if (mcpConfig.getThrottle().getUpdateIntervalMs() > 0) {
      return KafkaThrottleSensor.builder()
          .entityRegistry(entityRegistry)
          .kafkaAdmin(kafkaAdmin(kafkaConfiguration, kafkaProperties))
          .config(mcpConfig.getThrottle())
          .mclConsumerGroupId(maeConsumerGroupId)
          .timeseriesTopicName(timeseriesTopicName)
          .versionedTopicName(versionedTopicName)
          .build()
          .start();
    } else {
      return new NoOpSensor();
    }
  }

  private static AdminClient kafkaAdmin(
      KafkaConfiguration kafkaConfiguration, final KafkaProperties kafkaProperties) {
    Map<String, Object> adminProperties = new HashMap<>(kafkaProperties.buildAdminProperties(null));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null
        && !kafkaConfiguration.getBootstrapServers().isEmpty()) {
      adminProperties.put(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
          Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092 or environment variables

    return KafkaAdminClient.create(adminProperties);
  }
}
