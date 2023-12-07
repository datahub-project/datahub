package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "kafka.schemaRegistry.type",
    havingValue = InternalSchemaRegistryFactory.TYPE)
public class InternalSchemaRegistryFactory {

  public static final String TYPE = "INTERNAL";

  /** Configure Kafka Producer/Consumer processes with a custom schema registry. */
  @Bean("schemaRegistryConfig")
  @Nonnull
  protected SchemaRegistryConfig getInstance(
      @Qualifier("configurationProvider") ConfigurationProvider provider) {
    Map<String, Object> props = new HashMap<>();
    KafkaConfiguration kafkaConfiguration = provider.getKafka();

    props.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        kafkaConfiguration.getSchemaRegistry().getUrl());

    log.info(
        "Creating internal registry configuration for url {}",
        kafkaConfiguration.getSchemaRegistry().getUrl());
    return new SchemaRegistryConfig(KafkaAvroSerializer.class, KafkaAvroDeserializer.class, props);
  }

  @Bean(name = "schemaRegistryService")
  @Nonnull
  @DependsOn({TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  protected SchemaRegistryService schemaRegistryService(TopicConvention convention) {
    return new SchemaRegistryServiceImpl(convention);
  }
}
