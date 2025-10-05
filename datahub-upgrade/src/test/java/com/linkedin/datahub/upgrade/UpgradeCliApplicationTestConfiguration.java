package com.linkedin.datahub.upgrade;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.Nonnull;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class UpgradeCliApplicationTestConfiguration {

  @MockBean public UpgradeCli upgradeCli;

  @MockBean public SearchService searchService;

  @MockBean public GraphService graphService;

  @MockBean public EntityRegistry entityRegistry;

  @MockBean public ConfigEntityRegistry configEntityRegistry;

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
