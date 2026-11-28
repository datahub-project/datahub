package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import java.util.Arrays;
import java.util.Map;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * The registry drains every cluster at shutdown, so Spring must destroy it before the primary bulk
 * processor bean. Spring destroys a bean's dependents first, so this checks the dependency the
 * registry declares.
 */
@TestPropertySource(locations = "classpath:/application.yaml")
@SpringBootTest(
    classes = {
      SearchClusterRegistryFactory.class,
      ElasticSearchBulkProcessorFactory.class,
      SearchClusterRegistryDestroyOrderTest.SupportingBeans.class
    })
@EnableConfigurationProperties(ConfigurationProvider.class)
public class SearchClusterRegistryDestroyOrderTest extends AbstractTestNGSpringContextTests {

  @Configuration
  static class SupportingBeans {
    @Bean("elasticSearchIndexSettingsOverrides")
    Map<String, Map<String, Object>> indexSettingOverrides() {
      return Map.of();
    }

    @Bean
    ObjectMapper objectMapper() {
      return new ObjectMapper();
    }
  }

  @Autowired ApplicationContext context;

  @MockitoBean MetricUtils metricUtils;
  @MockitoBean GitVersion gitVersion;

  @MockitoBean(name = IndexConventionFactory.INDEX_CONVENTION_BEAN)
  IndexConvention indexConvention;

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

  @Test
  void registryIsDestroyedBeforeThePrimaryBulkProcessor() {
    String[] dependencies =
        ((ConfigurableApplicationContext) context)
            .getBeanFactory()
            .getDependentBeans("elasticSearchBulkProcessor");
    assertTrue(
        Arrays.asList(dependencies).contains("searchClusterRegistry"),
        "Dependents of elasticSearchBulkProcessor should include searchClusterRegistry: "
            + Arrays.toString(dependencies));
  }
}
