package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.semantic.profile.EntitySemanticContentStore;
import com.linkedin.metadata.search.semantic.profile.SemanticContentApplyService;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileEligibility;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * GMS-only. The MAE consumer scans the shared plugin configuration and has no entity service. This
 * factory lives with the entity service, which the consumer does not scan. Reads go through the
 * operation's aspect retriever; the entity service is used only to ingest the profile patch.
 *
 * <p>The condition names only the entity service. {@code SemanticProfileRegistry} comes from {@code
 * SpringStandardPluginConfiguration}, which GMS scans after this package, so a {@code
 * ConditionalOnBean} on the registry is evaluated before the bean exists and skips this factory
 * without a log line.
 */
@Configuration
@ConditionalOnBean(EntityService.class)
public class SemanticContentApplyServiceFactory {

  @Bean
  public SemanticContentApplyService semanticContentApplyService(
      EntityService<?> entityService,
      SemanticProfileEligibility eligibility,
      SemanticProfileRegistry registry) {
    return new SemanticContentApplyService(
        new EntitySemanticContentStore(entityService), eligibility, registry);
  }
}
