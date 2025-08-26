package com.linkedin.gms.factory.form;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.FormService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class FormServiceFactory {
  @Bean(name = "formService")
  @Scope("singleton")
  @Nonnull
  protected FormService getInstance(
      final SystemEntityClient entityClient,
      @Qualifier("openApiClient") final OpenApiClient openApiClient,
      final ObjectMapper objectMapper)
      throws Exception {
    return new FormService(entityClient, openApiClient, objectMapper);
  }
}
