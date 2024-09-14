package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "kafka.schemaRegistry.type", havingValue = "KAFKA")
public class KafkaSchemaRegistryFactory {

  public static final String TYPE = "KAFKA";

  @Value("${kafka.schemaRegistry.url}")
  private String kafkaSchemaRegistryUrl;

  @Value("${kafka.schema.registry.ssl.truststore.location:}")
  private String sslTruststoreLocation;

  @Value("${kafka.schema.registry.ssl.truststore.password:}")
  private String sslTruststorePassword;

  @Value("${kafka.schema.registry.ssl.keystore.location:}")
  private String sslKeystoreLocation;

  @Value("${kafka.schema.registry.ssl.keystore.password:}")
  private String sslKeystorePassword;

  @Value("${kafka.schema.registry.security.protocol:}")
  private String securityProtocol;

  @Bean("schemaRegistryConfig")
  @Nonnull
  protected KafkaConfiguration.SerDeKeyValueConfig getInstance(
      ConfigurationProvider configurationProvider) {
    Map<String, String> props = new HashMap<>();
    // FIXME: Properties for this factory should come from ConfigurationProvider object,
    // specifically under the
    // KafkaConfiguration class. See InternalSchemaRegistryFactory as an example.
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
    props.put(withNamespace(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG), sslTruststoreLocation);
    props.put(withNamespace(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), sslTruststorePassword);
    props.put(withNamespace(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG), sslKeystoreLocation);
    props.put(withNamespace(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG), sslKeystorePassword);
    props.put(withNamespace(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), securityProtocol);

    if (sslKeystoreLocation.isEmpty()) {
      log.info("creating schema registry config using url: {}", kafkaSchemaRegistryUrl);
    } else {
      log.info(
          "creating schema registry config using url: {}, keystore location: {} and truststore location: {}",
          kafkaSchemaRegistryUrl,
          sslTruststoreLocation,
          sslKeystoreLocation);
    }

    return configurationProvider.getKafka().getSerde().getEvent().toBuilder()
        .properties(props)
        .build();
  }

  private String withNamespace(String configKey) {
    return SchemaRegistryClientConfig.CLIENT_NAMESPACE + configKey;
  }
}
