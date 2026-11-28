package com.linkedin.gms.factory.entity;

import static org.mockito.Mockito.mock;

import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the registry and eligibility the way GMS does: from a configuration that is scanned
 * after the entity package. List it after the factory under test, so a {@code ConditionalOnBean} on
 * these beans is evaluated before they exist, as in production.
 */
@Configuration
class LateSemanticProfileBeansConfiguration {

  @Bean
  SemanticProfileRegistry semanticProfileRegistry() {
    return mock(SemanticProfileRegistry.class);
  }

  @Bean
  SemanticProfileEligibility semanticProfileEligibility() {
    return mock(SemanticProfileEligibility.class);
  }
}
