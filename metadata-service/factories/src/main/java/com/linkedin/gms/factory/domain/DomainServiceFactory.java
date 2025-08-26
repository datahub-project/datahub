package com.linkedin.gms.factory.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.DomainService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DomainServiceFactory {
  @Bean(name = "domainService")
  @Scope("singleton")
  @Nonnull
  protected DomainService getInstance(
      final SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new DomainService(entityClient, openApiClient, objectMapper);
  }
}
