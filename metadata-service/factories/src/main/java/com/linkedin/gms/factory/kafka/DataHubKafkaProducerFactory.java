package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties(KafkaProperties.class)
@Import({KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class})
public class DataHubKafkaProducerFactory {

  @Value("${kafka.bootstrapServers}")
  private String kafkaBootstrapServers;

  @Value("${kafka.schemaRegistry.type}")
  private String schemaRegistryType;

  @Autowired
  @Lazy
  @Qualifier("kafkaSchemaRegistry")
  private SchemaRegistryConfig kafkaSchemaRegistryConfig;

  @Autowired
  @Lazy
  @Qualifier("awsGlueSchemaRegistry")
  private SchemaRegistryConfig awsGlueSchemaRegistryConfig;

  @Bean(name = "kafkaProducer")
  protected Producer<String, IndexedRecord> createInstance(KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      producerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    SchemaRegistryConfig schemaRegistryConfig;
    if (schemaRegistryType.equals(KafkaSchemaRegistryFactory.TYPE)) {
      schemaRegistryConfig = kafkaSchemaRegistryConfig;
    } else {
      schemaRegistryConfig = awsGlueSchemaRegistryConfig;
    }

    Map<String, Object> props = properties.buildProducerProperties();

    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, schemaRegistryConfig.getSerializer().getName());

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
      .stream()
      .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
      .forEach(entry -> props.put(entry.getKey(), entry.getValue())); 

    return new KafkaProducer<>(props);
  }
}
