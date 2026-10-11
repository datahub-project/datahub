package com.linkedin.gms.factory.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import io.datahubproject.event.ExternalEventsPollHandler;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueueExternalEventsConfigurationTest {

  private final PgQueueExternalEventsConfiguration configuration =
      new PgQueueExternalEventsConfiguration();

  @Test
  public void pgQueueExternalEventsAvroDeserializer_buildsDeserializer() {
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
        configuration.pgQueueExternalEventsAvroDeserializer(configurationProvider, null);

    assertNotNull(deserializer);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pgQueueExternalEventsPollHandler_wrapsStore() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PostgresSqlSetupProperties postgresProperties = mock(PostgresSqlSetupProperties.class);
    when(postgresProperties.buildPgQueueOptions(any())).thenReturn(null);
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getKafka()).thenReturn(new KafkaConfiguration());

    ExternalEventsPollHandler handler =
        configuration.pgQueueExternalEventsPollHandler(
            store, postgresProperties, deserializer, new ObjectMapper(), configurationProvider);

    assertNotNull(handler);
    assertTrue(handler instanceof PgQueueExternalEventsPollHandler);
  }
}
