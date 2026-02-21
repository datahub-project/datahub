package com.linkedin.gms.factory.assertions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AssertionAssignmentRuleServiceFactory {
  @Bean(name = "assertionAssignmentRuleService")
  @Nonnull
  protected AssertionAssignmentRuleService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      final ObjectMapper objectMapper) {
    return new AssertionAssignmentRuleService(systemEntityClient, openApiClient, objectMapper);
  }
}
