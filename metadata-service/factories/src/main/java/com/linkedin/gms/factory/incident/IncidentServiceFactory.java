package com.linkedin.gms.factory.incident;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.IncidentService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IncidentServiceFactory {
  @Bean(name = "incidentService")
  @Nonnull
  protected IncidentService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new IncidentService(entityClient, openApiClient, objectMapper);
  }
}
