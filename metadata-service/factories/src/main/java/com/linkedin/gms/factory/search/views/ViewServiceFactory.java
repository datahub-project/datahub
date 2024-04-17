package com.linkedin.gms.factory.search.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ViewService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ViewServiceFactory {

  @Bean(name = "viewService")
  @Scope("singleton")
  @Nonnull
  protected ViewService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new ViewService(entityClient, openApiClient, objectMapper);
  }
}
