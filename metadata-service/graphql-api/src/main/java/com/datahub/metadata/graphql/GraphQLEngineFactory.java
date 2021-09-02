package com.datahub.metadata.graphql;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Bean;

@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class})
public class GraphQLEngineFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient elasticClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;


  @Value("${ANALYTICS_ENABLED:true}")
  private Boolean isAnalyticsEnabled;

  @Bean(name = "graphQLEngine")
  @Nonnull
  protected GraphQLEngine getInstance() {
    if (isAnalyticsEnabled) {
      return new GmsGraphQLEngine(new AnalyticsService(elasticClient, indexConvention.getPrefix())).builder().build();
    }
    return new GmsGraphQLEngine().builder().build();
  }
}