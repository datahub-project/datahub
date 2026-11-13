package com.linkedin.gms.factory.auth;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.PolicyChangeTopic;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * Starts a {@link PolicyChangeListener} in each process with a policy cache (GMS and the standalone
 * MCE consumer) when POLICY_CACHE_BROADCAST_ENABLED is true.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "authorization.defaultAuthorizer.changeBroadcast.enabled",
    havingValue = "true")
public class PolicyChangeListenerFactory {

  @Value("${datahub.messaging.transport:kafka}")
  private String messagingTransport;

  @Bean(name = "policyChangeListener", destroyMethod = "close")
  @Nullable
  protected PolicyChangeListener policyChangeListener(
      @Qualifier("dataHubAuthorizer") final ObjectProvider<DataHubAuthorizer> authorizerProvider,
      @Qualifier("configurationProvider") final ConfigurationProvider config,
      @Qualifier("kafkaConsumerFactory")
          final ObjectProvider<DefaultKafkaConsumerFactory<String, GenericRecord>>
              consumerFactory) {
    // Processes without a policy cache (system-update) scan this package but have no authorizer.
    final DataHubAuthorizer authorizer = authorizerProvider.getIfAvailable();
    if (authorizer == null) {
      return null;
    }
    if (!"kafka".equalsIgnoreCase(messagingTransport)) {
      // DataHubAuthorizerFactory already warns about this at startup.
      return null;
    }
    if (!authorizer.isSqlSourceEnabled()) {
      log.warn("Policy change listener not started: the SQL policy source is off.");
      return null;
    }
    final Optional<String> topic = PolicyChangeTopic.enabledTopicName(config);
    final DefaultKafkaConsumerFactory<String, GenericRecord> factory =
        consumerFactory.getIfAvailable();
    if (topic.isEmpty() || factory == null) {
      log.warn(
          "Policy change listener not started: topic configured={}, Kafka consumer available={}.",
          topic.isPresent(),
          factory != null);
      return null;
    }

    final PolicyChangeListener listener =
        new PolicyChangeListener(
            new KafkaConsumer<>(listenerProperties(factory.getConfigurationProperties())),
            topic.get(),
            authorizer::requestLoad,
            authorizer.getSystemOpContext().getMetricUtils().orElse(null),
            PolicyChangeListener.Settings.defaults(),
            Clock.systemUTC());
    listener.start();
    return listener;
  }

  /**
   * The shared consumer settings (brokers, security, MSK IAM), changed for a reader that is not in
   * a consumer group, commits nothing and never decodes the message: any message means "reload". So
   * the listener does not need the schema registry.
   */
  @Nonnull
  public static Map<String, Object> listenerProperties(@Nonnull Map<String, Object> shared) {
    final Map<String, Object> props = new HashMap<>(shared);
    props.remove(ConsumerConfig.GROUP_ID_CONFIG);
    props.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // "none": a lost position raises an error, and the listener loads again and reads from the end.
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    // Only system-update creates the topic, with 1 partition and 1-day retention. Without this, a
    // listener that starts first could create it with the broker defaults.
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "datahub-policy-change-listener");
    return props;
  }
}
