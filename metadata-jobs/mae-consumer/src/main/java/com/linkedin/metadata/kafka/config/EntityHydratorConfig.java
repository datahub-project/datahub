package com.linkedin.metadata.kafka.config;

import com.google.common.collect.ImmutableSet;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityHydratorConfig {

  @Autowired private SystemEntityClient entityClient;

  @Autowired private EntityRegistry _entityRegistry;

  public static final ImmutableSet<String> EXCLUDED_ASPECTS =
      ImmutableSet.<String>builder()
          .add("datasetUpstreamLineage", "upstreamLineage")
          .add("dataJobInputOutput")
          .add(
              "dataProcessInstanceRelationships",
              "dataProcessInstanceInput",
              "dataProcessInstanceOutput")
          .add("inputFields")
          .build();

  @Bean
  public EntityHydrator getEntityHydrator() {
    return new EntityHydrator(_entityRegistry, entityClient);
  }
}
