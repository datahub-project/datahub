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
@Import({GraphServiceFactory.class, EntityServiceFactory.class})
public class SiblingGraphServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("graphService")
  private GraphService graphService;

  @Bean(name = "siblingGraphService")
  @Primary
  @Nonnull
  protected SiblingGraphService getInstance() {
    return new SiblingGraphService(_entityService, graphService);
  }
}
