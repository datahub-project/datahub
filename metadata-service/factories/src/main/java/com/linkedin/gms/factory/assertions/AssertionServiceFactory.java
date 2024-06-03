package com.linkedin.gms.factory.assertions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.AssertionService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AssertionServiceFactory {
  @Bean(name = "assertionService")
  @Nonnull
  protected AssertionService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("graphClient") final GraphClient graphClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new AssertionService(systemEntityClient, graphClient, openApiClient, objectMapper);
  }
}
