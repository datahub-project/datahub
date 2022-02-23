package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class KafkaSchemaRegistryFactory {

  public static final String TYPE = "KAFKA";

  @Value("${kafka.schemaRegistry.url}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "kafkaSchemaRegistry")
  @Nonnull
  protected SchemaRegistryConfig getInstance() {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    log.info("Creating kafka registry");
    return new SchemaRegistryConfig(KafkaAvroSerializer.class, KafkaAvroDeserializer.class, props);
  }
}
