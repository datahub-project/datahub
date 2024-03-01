package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.client.EntityClientAspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class AspectRetrieverFactory {

  @Bean(name = "cachingAspectRetriever")
  @Nonnull
  protected CachingAspectRetriever cachingAspectRetriever(
      final EntityRegistry entityRegistry,
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return EntityClientAspectRetriever.builder()
        .entityRegistry(entityRegistry)
        .entityClient(systemEntityClient)
        .build();
  }
}
