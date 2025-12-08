package io.datahubproject.openapi.config;

import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.openapi.operations.kafka.DataHubTopicConvention;
import io.datahubproject.openapi.operations.kafka.KafkaAdminService;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/** Factory for creating the KafkaAdminService bean. */
@Configuration
public class KafkaAdminServiceFactory {

  private static final Properties ADMIN_CONSUMER_PROPERTIES = new Properties();

  static {
    ADMIN_CONSUMER_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
  }

  @Value("${kafkaAdmin.timeoutSeconds:" + KafkaAdminService.DEFAULT_TIMEOUT_SECONDS + "}")
  private int timeoutSeconds;

  @Value("${kafkaAdmin.defaultMessageCount:" + KafkaAdminService.DEFAULT_MESSAGE_COUNT + "}")
  private int defaultMessageCount;

  // Consumer group IDs from environment - these define the known DataHub consumer groups
  @Value("${METADATA_CHANGE_PROPOSAL_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client}")
  private String mcpConsumerGroupId;

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String mclConsumerGroupId;

  @Value("${DATAHUB_UPGRADE_HISTORY_KAFKA_CONSUMER_GROUP_ID:generic-duhe-consumer-job-client}")
  private String upgradeHistoryConsumerGroupId;

  @Value("${PLATFORM_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-pe-consumer-job-client}")
  private String platformEventConsumerGroupId;

  @Value("${DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_ID:generic-usage-event-consumer-job-client}")
  private String usageEventConsumerGroupId;

  @Bean("kafkaAdminService")
  public KafkaAdminService kafkaAdminService(
      @Qualifier("traceAdminClient") AdminClient adminClient,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) TopicConvention topicConvention,
      @Qualifier("kafkaEventProducer") KafkaEventProducer kafkaEventProducer,
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {

    // Build the set of known consumer group IDs
    Set<String> consumerGroupIds = new HashSet<>();
    consumerGroupIds.add(mcpConsumerGroupId);
    consumerGroupIds.add(mclConsumerGroupId);
    consumerGroupIds.add(upgradeHistoryConsumerGroupId);
    consumerGroupIds.add(platformEventConsumerGroupId);
    consumerGroupIds.add(usageEventConsumerGroupId);

    return KafkaAdminService.builder()
        .adminClient(adminClient)
        .topicConvention(new DataHubTopicConvention(topicConvention, consumerGroupIds))
        .kafkaEventProducer(kafkaEventProducer)
        .consumerSupplier(
            () -> createConsumerWithUniqueId(kafkaConsumerFactory, "kafka-admin-service"))
        .timeoutSeconds(timeoutSeconds)
        .defaultMessageCount(defaultMessageCount)
        .build();
  }

  private Consumer<String, GenericRecord> createConsumerWithUniqueId(
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Nonnull String baseClientId) {
    Properties consumerProps = new Properties();
    consumerProps.putAll(ADMIN_CONSUMER_PROPERTIES);
    // Add a unique suffix to the client.id
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, baseClientId + "-" + UUID.randomUUID());

    return kafkaConsumerFactory.createConsumer(
        baseClientId, // groupId prefix
        null, // groupId suffix (using default)
        null, // assignor
        consumerProps);
  }
}
