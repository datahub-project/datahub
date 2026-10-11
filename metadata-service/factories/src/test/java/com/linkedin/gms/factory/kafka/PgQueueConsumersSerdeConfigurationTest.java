package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueueConsumersSerdeConfigurationTest {

  private final PgQueueConsumersSerdeConfiguration configuration =
      new PgQueueConsumersSerdeConfiguration();

  @Test
  public void pgQueueConsumerAvroDeserializer_buildsConfiguredDeserializer() {
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    KafkaConfiguration.SerDeConfig serDeConfig = new KafkaConfiguration.SerDeConfig();
    KafkaConfiguration.SerDeKeyValueConfig eventConfig =
        new KafkaConfiguration.SerDeKeyValueConfig();
    KafkaConfiguration.SerDeProperties serDeProperties = new KafkaConfiguration.SerDeProperties();
    serDeProperties.setSerializer("org.apache.kafka.common.serialization.StringSerializer");
    serDeProperties.setDeserializer("org.apache.kafka.common.serialization.StringSerializer");
    eventConfig.setKey(serDeProperties);
    eventConfig.setValue(serDeProperties);
    eventConfig.setProperties(Map.of("schema.registry.url", "mock://localhost"));
    serDeConfig.setEvent(eventConfig);
    kafkaConfiguration.setSerde(serDeConfig);
    when(configurationProvider.getKafka()).thenReturn(kafkaConfiguration);

    Deserializer<GenericRecord> deserializer =
        configuration.pgQueueConsumerAvroDeserializer(configurationProvider, null);

    assertNotNull(deserializer);
  }
}
