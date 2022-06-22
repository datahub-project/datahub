package com.linkedin.gms.factory.test;

import com.google.common.collect.ImmutableList;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.QueryVersionedAspectEvaluator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({EntityRegistryFactory.class, EntityServiceFactory.class})
public class QueryEngineFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Bean(name = "queryEngine")
  @Nonnull
  protected QueryEngine getInstance() {
    QueryVersionedAspectEvaluator queryVersionedAspectEvaluator =
        new QueryVersionedAspectEvaluator(entityRegistry, entityService);
    return new QueryEngine(ImmutableList.of(queryVersionedAspectEvaluator));
  }
}
