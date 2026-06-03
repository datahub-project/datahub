package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.event.PgQueueEventProducer;
import com.linkedin.metadata.event.PgQueueUsageEventPublisher;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.TopicConvention;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

public class PgQueueProducerFactoriesTest {

  @Test
  public void pgQueueEventProducerFactory_createsPgQueueEventProducer() {
    PostgresSqlSetupProperties pgProps = postgresPropertiesWithCompression();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    TopicConvention topicConvention = mock(TopicConvention.class);
    SchemaRegistryService schemaRegistry = mock(SchemaRegistryService.class);

    @SuppressWarnings("unchecked")
    ObjectProvider<PostgresSqlSetupProperties> pgPropsProvider = mock(ObjectProvider.class);
    when(pgPropsProvider.getIfAvailable()).thenReturn(pgProps);
    @SuppressWarnings("unchecked")
    ObjectProvider<ConfigurationProvider> configProvider = mock(ObjectProvider.class);
    when(configProvider.getIfAvailable()).thenReturn(null);

    PgQueueEventProducerFactory factory = new PgQueueEventProducerFactory();
    EventProducer producer =
        factory.kafkaEventProducer(
            store, topicConvention, schemaRegistry, pgPropsProvider, configProvider);

    assertNotNull(producer);
    assertTrue(producer instanceof PgQueueEventProducer);
  }

  @Test
  public void pgQueueUsageEventPublisher_withConfigurationProvider() {
    PostgresSqlSetupProperties pgProps = postgresPropertiesWithCompression();
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    DataHubConfiguration datahub = mock(DataHubConfiguration.class);
    when(configurationProvider.getDatahub()).thenReturn(datahub);
    when(datahub.isReadOnly()).thenReturn(true);

    @SuppressWarnings("unchecked")
    ObjectProvider<PostgresSqlSetupProperties> pgPropsProvider = mock(ObjectProvider.class);
    when(pgPropsProvider.getIfAvailable()).thenReturn(pgProps);
    @SuppressWarnings("unchecked")
    ObjectProvider<ConfigurationProvider> configProvider = mock(ObjectProvider.class);
    when(configProvider.getIfAvailable()).thenReturn(configurationProvider);

    PgQueueUsageEventPublisherConfiguration configuration =
        new PgQueueUsageEventPublisherConfiguration();
    UsageEventPublisher publisher =
        configuration.pgQueueUsageEventPublisher(
            mock(MetadataQueueStore.class), pgPropsProvider, configProvider);

    assertNotNull(publisher);
    assertTrue(publisher instanceof PgQueueUsageEventPublisher);
  }

  private static PostgresSqlSetupProperties postgresPropertiesWithCompression() {
    PostgresSqlSetupProperties properties = new PostgresSqlSetupProperties();
    properties.getPgQueue().setEnabled(true);
    properties.getPgQueue().setSchema("queue");
    properties.getPgQueue().setTablePrefix("metadata_queue");
    properties.getPgQueue().getRetention().setPartmanPartitionInterval("1 day");
    properties.getPgQueue().getProducer().setPayloadCompression("SNAPPY");
    return properties;
  }
}
