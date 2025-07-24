package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = {ElasticSearchServiceFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {"ELASTICSEARCH_QUERY_CUSTOM_CONFIG_ENABLED=false"})
@Import(DisabledCustomSearchTest.TestConfig.class)
public class DisabledCustomSearchTest extends AbstractTestNGSpringContextTests {

  @TestConfiguration
  static class TestConfig {
    @MockBean public MetricUtils metricUtils;

    @MockBean(name = "settingsBuilder")
    public SettingsBuilder settingsBuilder;

    @MockBean(name = "baseElasticSearchComponents")
    public BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
        baseElasticSearchComponents;

    @MockBean public QueryFilterRewriteChain queryFilterRewriteChain;

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }
  }

  @Autowired
  @Qualifier("elasticSearchService")
  private ElasticSearchService elasticSearchService;

  @Autowired private ConfigurationProvider configurationProvider;

  @Test
  public void testInit() {
    assertNotNull(elasticSearchService);
  }

  @Test
  public void testCustomSearchConfiguration() throws IOException {
    assertNotNull(configurationProvider);

    assertNotNull(configurationProvider.getElasticSearch());
    assertNotNull(configurationProvider.getElasticSearch().getSearch());
    assertNotNull(configurationProvider.getElasticSearch().getSearch().getCustom());
    assertFalse(configurationProvider.getElasticSearch().getSearch().getCustom().isEnabled());
    assertNull(
        configurationProvider
            .getElasticSearch()
            .getSearch()
            .getCustom()
            .resolve(ObjectMapperContext.DEFAULT.getYamlMapper()));
  }
}
