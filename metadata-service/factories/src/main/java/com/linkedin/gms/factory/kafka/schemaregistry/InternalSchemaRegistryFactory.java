package com.linkedin.gms.factory.kafka.schemaregistry;

import com.datahub.kafka.avro.deserializer.KafkaAvroDeserializer;
import com.datahub.kafka.avro.serializer.KafkaAvroSerializer;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@ConditionalOnProperty(name = "kafka.schemaRegistry.type", havingValue = InternalSchemaRegistryFactory.TYPE)
public class InternalSchemaRegistryFactory {

  public static final String TYPE = "INTERNAL";

  @Bean
  @Nonnull
  protected SchemaRegistryConfig getInstance() {
    Map<String, Object> props = new HashMap<>();

    log.info("Creating internal registry");
    return new SchemaRegistryConfig(KafkaAvroSerializer.class, KafkaAvroDeserializer.class,
        props);
  }
}
