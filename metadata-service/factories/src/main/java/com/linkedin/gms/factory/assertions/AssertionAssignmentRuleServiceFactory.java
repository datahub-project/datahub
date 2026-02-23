package com.linkedin.gms.factory.assertions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.monitor.MonitorServiceFactory;
import com.linkedin.gms.factory.subscription.SubscriptionServiceFactory;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.service.SubscriptionService;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  AssertionServiceFactory.class,
  MonitorServiceFactory.class,
  SubscriptionServiceFactory.class
})
public class AssertionAssignmentRuleServiceFactory {
  @Bean(name = "assertionAssignmentRuleService")
  @Nonnull
  protected AssertionAssignmentRuleService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      final ObjectMapper objectMapper,
      @Qualifier("assertionService") final AssertionService assertionService,
      @Qualifier("monitorService") final MonitorService monitorService,
      @Qualifier("subscriptionService") final SubscriptionService subscriptionService) {
    return new AssertionAssignmentRuleService(
        systemEntityClient,
        openApiClient,
        objectMapper,
        assertionService,
        monitorService,
        subscriptionService);
  }
}
