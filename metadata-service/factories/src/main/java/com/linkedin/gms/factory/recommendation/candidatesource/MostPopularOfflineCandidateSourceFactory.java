package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.candidatesource.MostPopularOfflineSource;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({EntityServiceFactory.class})
public class MostPopularOfflineCandidateSourceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Bean(name = "mostPopularOfflineCandidateSource")
  @Nonnull
  protected MostPopularOfflineSource getInstance() {
    return new MostPopularOfflineSource(entityService);
  }
}
