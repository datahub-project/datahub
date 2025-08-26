package com.linkedin.gms.factory.glossary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.GlossaryTermService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class GlossaryTermServiceFactory {
  @Bean(name = "glossaryTermService")
  @Scope("singleton")
  @Nonnull
  protected GlossaryTermService getInstance(
      final SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new GlossaryTermService(entityClient, openApiClient, objectMapper);
  }
}
