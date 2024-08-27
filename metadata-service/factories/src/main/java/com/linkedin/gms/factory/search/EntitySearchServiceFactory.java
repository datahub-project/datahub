package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ElasticSearchServiceFactory.class})
public class EntitySearchServiceFactory {
  @Autowired
  @Qualifier("elasticSearchService")
  private ElasticSearchService elasticSearchService;

  @Bean(name = "entitySearchService")
  @Primary
  @Nonnull
  protected EntitySearchService getInstance() {
    return elasticSearchService;
  }
}
