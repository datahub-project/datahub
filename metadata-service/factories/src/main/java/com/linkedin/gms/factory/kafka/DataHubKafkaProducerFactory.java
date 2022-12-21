package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.KafkaConfiguration;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties(KafkaProperties.class)
@Import({ConfigurationProvider.class, KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class, InternalSchemaRegistryFactory.class})
public class DataHubKafkaProducerFactory {

  @Autowired
  private SchemaRegistryConfig schemaRegistryConfig;

  @Bean(name = "kafkaProducer")
  protected Producer<String, IndexedRecord> createInstance(KafkaConfiguration kafkaConfiguration,
      KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null && kafkaConfiguration.getBootstrapServers().length() > 0) {
      producerProps.setBootstrapServers(Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildProducerProperties();

    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, schemaRegistryConfig.getSerializer());

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
        .forEach(entry -> props.put(entry.getKey(), entry.getValue()));

    return new KafkaProducer<>(props);
  }
}