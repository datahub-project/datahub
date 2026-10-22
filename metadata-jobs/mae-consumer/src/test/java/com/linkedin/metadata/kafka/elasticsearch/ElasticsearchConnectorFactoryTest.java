package com.linkedin.metadata.kafka.elasticsearch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.testng.annotations.Test;

public class ElasticsearchConnectorFactoryTest {

  @Test
  public void testCreateInstanceUsesUsageClusterProcessor() {
    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    ESBulkProcessor usageProcessor = mock(ESBulkProcessor.class);
    ElasticSearchConfiguration usageConfig = mock(ElasticSearchConfiguration.class);
    BulkProcessorConfiguration bulkConfig = mock(BulkProcessorConfiguration.class);
    when(registry.bulkProcessorFor(SearchComponent.USAGE)).thenReturn(usageProcessor);
    when(registry.configFor(SearchComponent.USAGE)).thenReturn(usageConfig);
    when(usageConfig.getBulkProcessor()).thenReturn(bulkConfig);
    when(bulkConfig.getNumRetries()).thenReturn(5);

    ElasticsearchConnector connector = new ElasticsearchConnectorFactory().createInstance(registry);

    assertNotNull(connector);
    verify(registry).bulkProcessorFor(SearchComponent.USAGE);
    verify(registry).configFor(SearchComponent.USAGE);
  }
}
