package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.KafkaConfiguration;
import com.linkedin.metadata.schema.registry.SchemaRegistryService;
import com.linkedin.metadata.schema.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@ConditionalOnProperty(name = "kafka.schemaRegistry.type", havingValue = InternalSchemaRegistryFactory.TYPE)
@Import({ConfigurationProvider.class})
public class InternalSchemaRegistryFactory {

  public static final String TYPE = "INTERNAL";

  /**
   * Configure Kafka Producer/Consumer processes with a custom schema registry.
   */
  @Bean
  @Nonnull
  protected SchemaRegistryConfig getInstance(KafkaConfiguration kafkaConfiguration) {
    Map<String, Object> props = new HashMap<>();

    // TODO: Fix this url to either come by config or from the source code directly. Particularly the last endpoint
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfiguration.getSchemaRegistry().getUrl());

    log.info("Creating internal registry");
    return new SchemaRegistryConfig(KafkaAvroSerializer.class, KafkaAvroDeserializer.class, props);
  }

  @Bean(name = "schemaRegistryService")
  @Nonnull
  @DependsOn({TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  protected SchemaRegistryService schemaRegistryService(TopicConvention convention) {
    return new SchemaRegistryServiceImpl(convention);
  }
}
