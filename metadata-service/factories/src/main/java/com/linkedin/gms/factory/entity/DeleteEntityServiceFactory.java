package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntityServiceFactory.class})
public class DeleteEntityServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("graphService")
  private GraphService _graphService;

  @Bean(name = "deleteEntityService")
  @DependsOn({"entityService"})
  @Nonnull
  protected DeleteEntityService createDeleteEntityService() {
    return new DeleteEntityService(_entityService, _graphService);
  }
}
