package com.linkedin.gms.factory.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.RoutingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.Test;

/**
 * Thin Spring context check for {@link RoutingEntitySearchService}: verifies the routing bean shape
 * loads without Elasticsearch/Testcontainers. Full {@link EntitySearchServiceFactory} wiring is
 * covered by {@link EntitySearchServiceFactoryTest}.
 */
public class RoutingEntitySearchServiceApplicationContextTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner().withUserConfiguration(RoutingBeans.class);

  @Test
  public void registersRoutingImplementation_asEntitySearchService() {
    runner.run(
        ctx -> {
          assertThat(ctx.getBean("entitySearchService", EntitySearchService.class))
              .isInstanceOf(RoutingEntitySearchService.class);
          assertThat(ctx.getBean(RoutingEntitySearchService.class)).isNotNull();
        });
  }

  @Configuration
  static class RoutingBeans {

    @Bean
    SearchServiceConfiguration searchServiceConfiguration() {
      return SearchServiceConfiguration.builder()
          .limit(
              LimitConfig.builder()
                  .results(ResultsLimitConfig.builder().apiDefault(100).max(1000).build())
                  .build())
          .build();
    }

    @Bean
    ElasticSearchService elasticSearchService() {
      return mock(ElasticSearchService.class);
    }

    @Bean
    PostgresEntitySearchService postgresEntitySearchService() {
      return mock(PostgresEntitySearchService.class);
    }

    @Bean
    EntitySearchService entitySearchService(
        ElasticSearchService es, PostgresEntitySearchService pg, SearchServiceConfiguration cfg) {
      return new RoutingEntitySearchService(es, pg, cfg);
    }
  }
}
