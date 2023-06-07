package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.kafka.MockDUHEDeserializer;
import com.linkedin.metadata.boot.kafka.MockDUHESerializer;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class DUHESchemaRegistryFactory {
    /**
     * Configure Kafka Producer/Consumer processes with a custom schema registry.
     */
    @Bean("duheSchemaRegistryConfig")
    protected SchemaRegistryConfig duheSchemaRegistryConfig(ConfigurationProvider provider) {
        Map<String, Object> props = new HashMap<>();
        KafkaConfiguration kafkaConfiguration = provider.getKafka();

        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaConfiguration
                .getSchemaRegistry().getUrl());

        log.info("DataHub System Update Registry");
        return new SchemaRegistryConfig(MockDUHESerializer.class, MockDUHEDeserializer.class, props);
    }
}
