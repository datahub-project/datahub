package client;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.typesafe.config.Config;
import config.ConfigurationProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.api.inject.ApplicationLifecycle;
import utils.ConfigUtil;

@Singleton
public class KafkaTrackingProducer {
  private static final Logger logger =
      LoggerFactory.getLogger(KafkaTrackingProducer.class.getName());
  private static final List<String> KAFKA_SSL_PROTOCOLS =
      Collections.unmodifiableList(
          Arrays.asList(
              SecurityProtocol.SSL.name(),
              SecurityProtocol.SASL_SSL.name(),
              SecurityProtocol.SASL_PLAINTEXT.name()));

  private final Boolean isEnabled;
  private final KafkaProducer<String, String> producer;

  @Inject
  public KafkaTrackingProducer(
      @Nonnull Config config,
      ApplicationLifecycle lifecycle,
      final ConfigurationProvider configurationProvider) {
    isEnabled = !config.hasPath("analytics.enabled") || config.getBoolean("analytics.enabled");

    if (isEnabled) {
      logger.debug("Analytics tracking is enabled");
      producer = createKafkaProducer(config, configurationProvider.getKafka());

      lifecycle.addStopHook(
          () -> {
            producer.flush();
            producer.close();
            return CompletableFuture.completedFuture(null);
          });
    } else {
      logger.debug("Analytics tracking is disabled");
      producer = null;
    }
  }

  public Boolean isEnabled() {
    return isEnabled;
  }

  public void send(ProducerRecord<String, String> record) {
    producer.send(record);
  }

  private static KafkaProducer createKafkaProducer(
      Config config, KafkaConfiguration kafkaConfiguration) {
    final ProducerConfiguration producerConfiguration = kafkaConfiguration.getProducer();
    final Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "datahub-frontend");
    props.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        config.getString("analytics.kafka.delivery.timeout.ms"));
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.getString("analytics.kafka.bootstrap.server"));
    // key: Actor urn.
    // value: JSON object.
    props.putAll(kafkaConfiguration.getSerde().getUsageEvent().getProducerProperties(null));
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerConfiguration.getMaxRequestSize());
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfiguration.getCompressionType());

    final String securityProtocolConfig = "analytics.kafka.security.protocol";
    if (config.hasPath(securityProtocolConfig)
        && KAFKA_SSL_PROTOCOLS.contains(config.getString(securityProtocolConfig))) {
      props.put(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getString(securityProtocolConfig));
      setConfig(
          config, props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, "analytics.kafka.ssl.key.password");

      setConfig(
          config, props, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "analytics.kafka.ssl.keystore.type");
      setConfig(
          config,
          props,
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          "analytics.kafka.ssl.keystore.location");
      setConfig(
          config,
          props,
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          "analytics.kafka.ssl.keystore.password");

      setConfig(
          config,
          props,
          SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
          "analytics.kafka.ssl.truststore.type");
      setConfig(
          config,
          props,
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          "analytics.kafka.ssl.truststore.location");
      setConfig(
          config,
          props,
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          "analytics.kafka.ssl.truststore.password");

      setConfig(config, props, SslConfigs.SSL_PROTOCOL_CONFIG, "analytics.kafka.ssl.protocol");
      setConfig(
          config,
          props,
          SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          "analytics.kafka.ssl.endpoint.identification.algorithm");

      final String securityProtocol = config.getString(securityProtocolConfig);
      if (securityProtocol.equals(SecurityProtocol.SASL_SSL.name())
          || securityProtocol.equals(SecurityProtocol.SASL_PLAINTEXT.name())) {
        setConfig(config, props, SaslConfigs.SASL_MECHANISM, "analytics.kafka.sasl.mechanism");
        setConfig(config, props, SaslConfigs.SASL_JAAS_CONFIG, "analytics.kafka.sasl.jaas.config");
        setConfig(
            config,
            props,
            SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
            "analytics.kafka.sasl.kerberos.service.name");
        setConfig(
            config,
            props,
            SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
            "analytics.kafka.sasl.login.callback.handler.class");
        setConfig(
            config,
            props,
            SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
            "analytics.kafka.sasl.client.callback.handler.class");
      }
    }

    return new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
  }

  private static void setConfig(Config config, Properties props, String key, String configKey) {
    Optional.ofNullable(ConfigUtil.getString(config, configKey, null))
        .ifPresent(v -> props.put(key, v));
  }
}
