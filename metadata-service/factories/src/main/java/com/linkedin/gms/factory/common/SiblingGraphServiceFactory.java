package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({EntityServiceFactory.class})
public class SiblingGraphServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Bean(name = "siblingGraphService")
  @Primary
  @Nonnull
  protected SiblingGraphService getInstance(final GraphService graphService) {
    return new SiblingGraphService(_entityService, graphService);
  }
}
