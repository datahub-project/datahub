package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
<<<<<<< HEAD
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
=======
>>>>>>> master
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.MostPopularSource;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
<<<<<<< HEAD
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
=======
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
>>>>>>> master


@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class, EntityServiceFactory.class})
<<<<<<< HEAD
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
=======
>>>>>>> master
public class MostPopularCandidateSourceFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

<<<<<<< HEAD
  @Value("${recommendationService.mostPopular.offline}")
  private Boolean fetchOffline;

  @Bean(name = "mostPopularCandidateSource")
  @Nonnull
  protected MostPopularSource getInstance() {
    return new MostPopularSource(searchClient, indexConvention, entityService, fetchOffline);
  }
}
=======
  @Bean(name = "mostPopularCandidateSource")
  @Nonnull
  protected MostPopularSource getInstance() {
    return new MostPopularSource(searchClient, indexConvention, entityService);
  }
}
>>>>>>> master
