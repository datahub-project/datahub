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
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@TestConfiguration
@Import(
    value = {
      UpgradeConfigurationSelector.class,
      SystemAuthenticationFactory.class,
    })
public class UpgradeCliApplicationTestConfiguration {

  // TODO: We cannot remove the MockBean annotation here because with MockitoBean it is still trying
  // to instantiate
  //       see: https://github.com/spring-projects/spring-framework/issues/33934
  @MockBean public UpgradeCli upgradeCli;

  @MockBean public SearchService searchService;

  @MockBean public GraphService graphService;

  @MockBean public ConfigEntityRegistry configEntityRegistry;

  // Mock semantic search factories to avoid needing full configuration
  @MockBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockBean public SemanticSearchServiceFactory semanticSearchServiceFactory;

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

  @Bean
  public SchemaRegistryService schemaRegistryService(
      @Qualifier("eventSchemaData") final EventSchemaData eventSchemaData) {
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
}
