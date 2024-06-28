package com.linkedin.metadata.kafka.config;

import com.google.common.collect.ImmutableSet;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityHydratorConfig {

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
  public EntityHydrator getEntityHydrator(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient) {
    return new EntityHydrator(systemOperationContext, entityClient);
  }
}
