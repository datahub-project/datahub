package com.linkedin.datahub.upgrade;

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
import jakarta.annotation.PostConstruct;
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

  @MockBean public SearchClientShim<?> searchClientShim;

  // Mock semantic search factories to avoid needing full configuration
  @MockBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockBean public SemanticSearchServiceFactory semanticSearchServiceFactory;

  @PostConstruct
  public void configureMocks() {
    // Configure SearchClientShim mock to return a valid engine type
    Mockito.when(searchClientShim.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
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
}
