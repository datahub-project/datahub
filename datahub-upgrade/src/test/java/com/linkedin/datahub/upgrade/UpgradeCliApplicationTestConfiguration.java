package com.linkedin.datahub.upgrade;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.mxe.TopicConventionImpl;
import io.ebean.Database;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class UpgradeCliApplicationTestConfiguration {

  @MockBean private UpgradeCli upgradeCli;

  @MockBean private Database ebeanServer;

  @MockBean private SearchService searchService;

  @MockBean private GraphService graphService;

  @MockBean private EntityRegistry entityRegistry;

  @MockBean ConfigEntityRegistry configEntityRegistry;

  @MockBean public EntityIndexBuilders entityIndexBuilders;

  @Bean
  public SchemaRegistryService schemaRegistryService() {
    return new SchemaRegistryServiceImpl(new TopicConventionImpl());
  }
}
