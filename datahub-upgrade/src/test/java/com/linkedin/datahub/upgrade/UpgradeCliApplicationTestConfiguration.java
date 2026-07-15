package com.linkedin.datahub.upgrade;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.Nonnull;
import java.util.UUID;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@TestConfiguration
@Import(
    value = {
      UpgradeConfigurationSelector.class,
      SystemAuthenticationFactory.class,
    })
public class UpgradeCliApplicationTestConfiguration {

  // TODO: Convert to @Bean @Primary (like configEntityRegistry below) to avoid Spring Framework
  // issue #33934 where @MockitoBean still triggers real factory bean initialization.
  // See: https://github.com/spring-projects/spring-framework/issues/33934
  @MockitoBean public UpgradeCli upgradeCli;

  @MockitoBean public SearchService searchService;

  @MockitoBean public GraphService graphService;

  // Use @Bean instead of @MockitoBean to prevent ConfigEntityRegistryFactory from
  // attempting to load entity-registry.yml (Spring Framework 7.0 issue #33934)
  @Bean
  public ConfigEntityRegistry configEntityRegistry() {
    return Mockito.mock(ConfigEntityRegistry.class);
  }

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean public SemanticSearchServiceFactory semanticSearchServiceFactory;

  /**
   * Provide a pre-stubbed SearchClientShim so getEngineType() is non-null before any bean (e.g.
   * ElasticSearchGraphServiceFactory) uses it during context refresh.
   */
  @Bean
  @Primary
  public SearchClientShim<?> searchClientShim() {
    SearchClientShim<?> shim = Mockito.mock(SearchClientShim.class);
    Mockito.when(shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    return shim;
  }

  /** Use real EntityRegistry from TestOperationContexts for proper annotation-based validation. */
  @Primary
  @Bean
  public EntityRegistry entityRegistry() {
    return TestOperationContexts.defaultEntityRegistry();
  }

  @Primary
  @Bean
  public MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
  }

  @Bean(name = "eventSchemaData")
  @Nonnull
  protected EventSchemaData eventSchemaData(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    return new EventSchemaData(systemOpContext.getYamlMapper());
  }

  // @Lazy breaks a circular dependency that closes only in pgQueue mode: entityService ->
  // kafkaEventProducer (PgQueueEventProducerFactory needs schemaRegistryService) ->
  // schemaRegistryService -> eventSchemaData -> systemOperationContext -> entityService.
  // SchemaRegistryServiceImpl only stores eventSchemaData (buildTopicToSchemaNameMap uses just the
  // TopicConvention), so deferring its creation is safe and lets the in-process entity client work
  // for pgQueue tests too.
  @Bean
  public SchemaRegistryService schemaRegistryService(
      @Lazy @Qualifier("eventSchemaData") final EventSchemaData eventSchemaData) {
    return new SchemaRegistryServiceImpl(new TopicConventionImpl(), eventSchemaData);
  }

  @Bean
  @Primary
  public Database ebeanServer() {
    // Create a real H2 in-memory database for testing with a unique name to avoid conflicts
    String instanceId = "upgradecli_" + UUID.randomUUID().toString().replace("-", "");
    String serverName = "upgradecli_test_" + UUID.randomUUID().toString().replace("-", "");
    return EbeanTestUtils.createNamedTestServer(instanceId, serverName);
  }

  /**
   * Stub the dedicated pgQueue Ebean server so {@code MetadataQueueStoreFactory} can wire up a
   * {@link com.linkedin.metadata.queue.MetadataQueueStore} bean during pgQueue-mode tests without
   * the new {@code PgQueueEbeanConfigFactory} attempting a real Postgres connection.
   */
  @Bean(name = "pgQueueEbeanServer")
  public Database pgQueueEbeanServer() {
    String instanceId = "pgqueue_upgradecli_" + UUID.randomUUID().toString().replace("-", "");
    String serverName = "pgqueue_upgradecli_test_" + UUID.randomUUID().toString().replace("-", "");
    return EbeanTestUtils.createNamedTestServer(instanceId, serverName);
  }

  /**
   * Provide an in-memory HazelcastInstance for tests so that Hazelcast Spring integration
   * (HazelcastObjectExtractionConfiguration in hazelcast-spring 5.6+) can create its
   * hzInternalBeanExposer bean. Network is disabled so no cluster join is attempted.
   */
  @Bean(name = "hazelcastInstance")
  @Primary
  public HazelcastInstance hazelcastInstance() {
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getKubernetesConfig().setEnabled(false);
    config.setClusterName("datahub-upgrade-test-" + UUID.randomUUID());
    return Hazelcast.newHazelcastInstance(config);
  }

  /**
   * Replace the real Kafka producers with mocks.
   *
   * <p>{@code GeneralUpgradeConfiguration} component-scans {@code com.linkedin.gms.factory}, which
   * pulls in {@code DataHubKafkaProducerFactory}. Its {@code kafkaProducer} and {@code
   * dataHubUsageProducer} beans construct real {@link
   * org.apache.kafka.clients.producer.KafkaProducer} instances that open network connections to the
   * configured broker. No broker exists in this module's tests (UpgradeCli is mocked, so the topic
   * create/wait steps never run), so every Spring context boot spawns producer network threads that
   * spin in connection-retry loops -- thousands of WARN lines per run plus wasted time and CPU on
   * every context startup.
   *
   * <p>These overrides (enabled by {@code spring.main.allow-bean-definition-overriding=true} in
   * application-test.properties) keep the wiring intact -- {@code KafkaEventProducer} still wraps a
   * {@link org.apache.kafka.clients.producer.Producer} -- while never touching the network.
   */
  @Bean(name = "kafkaProducer")
  @Primary
  @SuppressWarnings("unchecked")
  public Producer<String, IndexedRecord> kafkaProducer() {
    return Mockito.mock(Producer.class);
  }

  @Bean(name = "dataHubUsageProducer")
  @Primary
  @SuppressWarnings("unchecked")
  public Producer<String, String> dataHubUsageProducer() {
    return Mockito.mock(Producer.class);
  }

  /**
   * Replace the DataHub-upgrade-history event producer with a mock.
   *
   * <p>{@code SystemUpdateKafkaMessagingConfig#duheKafkaEventProducer} builds its own {@link
   * org.apache.kafka.clients.producer.KafkaProducer} directly (not via the {@code kafkaProducer}
   * bean mocked above), so it opens its own broker connection. With the INTERNAL schema registry
   * (the default for these tests) it also becomes the {@code @Primary kafkaEventProducer} used by
   * EntityService, so mocking it here keeps the {@code kafkaEventProducer == duheKafkaEventProducer
   * == entityService.getProducer()} identity that DatahubUpgradeNoSchemaRegistryTest and
   * DatahubUpgradeNonBlockingTest assert, while removing the last real Kafka connection.
   */
  @Bean(name = "duheKafkaEventProducer")
  public KafkaEventProducer duheKafkaEventProducer() {
    return Mockito.mock(KafkaEventProducer.class);
  }
}
