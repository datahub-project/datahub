package com.linkedin.gms.factory.ermodelrelation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ERModelRelationshipService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ERModelRelationshipServiceFactory {
  @Bean(name = "erModelRelationshipService")
  @Nonnull
  protected ERModelRelationshipService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new ERModelRelationshipService(entityClient, openApiClient, objectMapper);
  }
}
