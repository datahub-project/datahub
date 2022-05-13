package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.propagate.PropagateTerms;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class PropagateTermsConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "propagateTerms")
  @DependsOn({"entityService", "entitySearchService"})
  @Nonnull
  public PropagateTerms createInstance() {
    final EntityService entityService = applicationContext.getBean(EntityService.class);
    final EntitySearchService entitySearchService = applicationContext.getBean(EntitySearchService.class);
    return new PropagateTerms(entityService, entitySearchService);
  }
}
