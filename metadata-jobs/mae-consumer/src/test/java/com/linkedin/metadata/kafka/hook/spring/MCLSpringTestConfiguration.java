package com.linkedin.metadata.kafka.hook.spring;

import com.datahub.authentication.Authentication;
import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.kafka",
      "com.linkedin.gms.factory.entity.update.indices"
    })
public class MCLSpringTestConfiguration {

  @MockBean public EntityRegistry entityRegistry;

  @MockBean public ElasticSearchGraphService graphService;

  @MockBean public TimeseriesAspectService timeseriesAspectService;

  @MockBean public SystemMetadataService systemMetadataService;

  @MockBean public SearchDocumentTransformer searchDocumentTransformer;

  @MockBean public IngestionScheduler ingestionScheduler;

  @MockBean(name = "systemRestliEntityClient")
  public SystemRestliEntityClient entityClient;

  @MockBean public ElasticSearchService searchService;

  @MockBean public Authentication systemAuthentication;

  @MockBean(name = "dataHubUpgradeKafkaListener")
  public DataHubUpgradeKafkaListener dataHubUpgradeKafkaListener;

  @MockBean(name = "duheSchemaRegistryConfig")
  public SchemaRegistryConfig schemaRegistryConfig;

  @MockBean(name = "duheKafkaConsumerFactory")
  public DefaultKafkaConsumerFactory<String, GenericRecord> defaultKafkaConsumerFactory;

  @MockBean public SchemaRegistryService schemaRegistryService;

  @MockBean public EntityIndexBuilders entityIndexBuilders;
}
