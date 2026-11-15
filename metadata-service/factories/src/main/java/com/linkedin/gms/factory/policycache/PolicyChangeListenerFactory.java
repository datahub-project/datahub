package com.linkedin.gms.factory.policycache;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.PolicyChangeTopic;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import java.time.Clock;
import java.time.Duration;
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
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * Starts a {@link PolicyChangeListener} in GMS when POLICY_CACHE_BROADCAST_ENABLED is true.
 *
 * <p>GMS scans this package (CommonApplicationConfig); system-update scans it too but has no
 * authorizer, so it starts nothing. The standalone MCE consumer does not scan it: it sends the
 * changes it commits, and its own Rest.li endpoints keep the timer refresh.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "authorization.defaultAuthorizer.changeBroadcast.enabled",
    havingValue = "true")
// DataHubAuthorizerFactory warns at startup when the transport is not Kafka.
@Conditional(KafkaMessagingEnabledCondition.class)
public class PolicyChangeListenerFactory {

  @Bean(name = "policyChangeListener", destroyMethod = "close")
  @Nullable
  protected PolicyChangeListener policyChangeListener(
      @Qualifier("dataHubAuthorizer") final ObjectProvider<DataHubAuthorizer> authorizerProvider,
      @Qualifier("configurationProvider") final ConfigurationProvider config,
      @Qualifier("kafkaConsumerFactory")
          final ObjectProvider<DefaultKafkaConsumerFactory<String, GenericRecord>> consumerFactory,
      @Value("${authorization.defaultAuthorizer.immediateUpdate.recentChangeWindowSeconds:600}")
          final long recentChangeWindowSeconds) {
    // system-update scans all factory packages but has no authorizer.
    final DataHubAuthorizer authorizer = authorizerProvider.getIfAvailable();
    if (authorizer == null) {
      return null;
    }
    if (!authorizer.isImmediateUpdateEnabled()) {
      log.warn("Policy change listener not started: immediate policy cache updates are off.");
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
            authorizer::requestUpdate,
            authorizer.getSystemOpContext().getMetricUtils().orElse(null),
            // Replays as far back as the authorizer remembers recent changes.
            PolicyChangeListener.Settings.defaults(Duration.ofSeconds(recentChangeWindowSeconds)),
            Clock.systemUTC());
    listener.start();
    return listener;
  }

  /**
   * The shared consumer settings (brokers, security, MSK IAM), changed for a reader that is not in
   * a consumer group, commits nothing and only reads the message key (the policy URN, as text). So
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
    // partitionsFor() reads cached metadata; refresh it as often as the listener checks the topic,
    // so added partitions are found within about 60 s (metadata age plus check interval), not
    // after the 5 min default.
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 30_000);
    return props;
  }
}
