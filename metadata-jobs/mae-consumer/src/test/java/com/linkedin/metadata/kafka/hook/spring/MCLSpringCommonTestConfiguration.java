package com.linkedin.metadata.kafka.hook.spring;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Authentication;
import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import io.datahubproject.metadata.context.ValidationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.kafka",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.gms.factory.entity.update.indices",
      "com.linkedin.gms.factory.timeline.eventgenerator",
      "com.linkedin.metadata.dao.producer",
      "com.linkedin.gms.factory.change",
      "com.datahub.event.hook",
      "com.linkedin.gms.factory.notifications",
      "com.linkedin.gms.factory.search.filter"
    })
public class MCLSpringCommonTestConfiguration {

  // TODO: We cannot move from MockBeans here because we are reliant on their behavior in
  // configuration classes
  @MockitoBean public EntityRegistry entityRegistry;

  @Bean
  @Primary
  public ElasticSearchGraphService graphService() {
    return Mockito.mock(ElasticSearchGraphService.class);
  }

  @Bean
  @Primary
  public TimeseriesAspectService timeseriesAspectService() {
    return Mockito.mock(TimeseriesAspectService.class);
  }

  @Bean
  @Primary
  public SystemMetadataService systemMetadataService() {
    return Mockito.mock(SystemMetadataService.class);
  }

  @Bean
  @Primary
  public SearchDocumentTransformer searchDocumentTransformer() {
    return Mockito.mock(SearchDocumentTransformer.class);
  }

  @Bean
  @Primary
  public IngestionScheduler ingestionScheduler() {
    return Mockito.mock(IngestionScheduler.class);
  }

  @Bean
  public EntityClientConfig entityClientConfig() {
    return EntityClientConfig.builder().build();
  }

  @Bean(name = "systemEntityClient")
  @Primary
  public SystemEntityClient systemEntityClient() {
    return Mockito.mock(SystemEntityClient.class);
  }

  @Bean(name = {"entitySearchService", "elasticSearchService"})
  @Primary
  public ElasticSearchService elasticSearchService() {
    return Mockito.mock(ElasticSearchService.class);
  }

  @Bean
  @Primary
  public FormService formService() {
    return Mockito.mock(FormService.class);
  }

  @Bean(name = "settingsBuilder")
  @Primary
  public SettingsBuilder settingsBuilder() {
    return Mockito.mock(SettingsBuilder.class);
  }

  // Use a real Authentication object to avoid Mockito state corruption during Spring context init
  @Bean(name = "systemAuthentication")
  @Primary
  public Authentication systemAuthentication() {
    return TestOperationContexts.TEST_SYSTEM_AUTH;
  }

  @Bean(name = "dataHubUpgradeKafkaListener")
  @Primary
  public DataHubUpgradeKafkaListener dataHubUpgradeKafkaListener() {
    return Mockito.mock(DataHubUpgradeKafkaListener.class);
  }

  @Bean(name = "duheKafkaConsumerFactory")
  @Primary
  @SuppressWarnings("unchecked")
  public DefaultKafkaConsumerFactory<String, GenericRecord> defaultKafkaConsumerFactory() {
    DefaultKafkaConsumerFactory<String, GenericRecord> factory =
        Mockito.mock(DefaultKafkaConsumerFactory.class);
    Consumer<String, GenericRecord> consumer = Mockito.mock(Consumer.class);
    Mockito.when(factory.createConsumer(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(consumer);
    return factory;
  }

  @Bean(name = "systemOperationContext")
  public OperationContext operationContext(
      final EntityRegistry entityRegistry,
      @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      final IndexConvention indexConvention) {
    return OperationContext.asSystem(
        OperationContextConfig.builder().build(),
        systemAuthentication,
        entityRegistry,
        mock(ServicesRegistryContext.class),
        SearchContext.EMPTY.toBuilder().indexConvention(indexConvention).build(),
        TestOperationContexts.emptyActiveUsersRetrieverContext(() -> entityRegistry),
        mock(ValidationContext.class),
        null,
        true);
  }

  @Bean
  @Primary
  public SpringStandardPluginConfiguration springStandardPluginConfiguration() {
    return Mockito.mock(SpringStandardPluginConfiguration.class);
  }

  @Bean(name = "kafkaThrottle")
  @Primary
  public ThrottleSensor kafkaThrottle() {
    return Mockito.mock(ThrottleSensor.class);
  }

  @Bean(name = "traceAdminClient")
  @Primary
  public AdminClient traceAdminClient() {
    return Mockito.mock(AdminClient.class);
  }

  @Bean
  @Primary
  public MetricUtils metricUtils() {
    return Mockito.mock(MetricUtils.class);
  }

  @Bean(name = "searchClientShim")
  @Primary
  @SuppressWarnings("unchecked")
  public SearchClientShim<?> searchClientShim() {
    SearchClientShim<?> mock = Mockito.mock(SearchClientShim.class);
    Mockito.when(mock.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    return mock;
  }

  // Provide mock directly to prevent ElasticSearchBulkProcessorFactory from calling
  // searchClient.generateAsyncBulkProcessor() (a void method) which corrupts Mockito stub state
  @Bean(name = "elasticSearchBulkProcessor")
  @Primary
  public ESBulkProcessor elasticSearchBulkProcessor() {
    return Mockito.mock(ESBulkProcessor.class);
  }
}
