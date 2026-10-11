package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.globalsettings.IngestDefaultGlobalSettings;
import com.linkedin.datahub.upgrade.system.policies.IngestPolicies;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class IngestDefaultSystemDataConfig {

  @Bean
  public BlockingSystemUpgrade ingestDefaultGlobalSettings(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Value("${systemUpdate.ingestDefaultGlobalSettings.enabled}") final boolean enabled) {
    return new IngestDefaultGlobalSettings(entityService, enabled);
  }

  @Bean
  public BlockingSystemUpgrade ingestPolicies(
      @Qualifier("entityService") final EntityService<?> entityService,
      final EntitySearchService entitySearchService,
      final SearchDocumentTransformer searchDocumentTransformer,
      @Value("${bootstrap.policies.file:classpath:boot/policies.json}")
          final Resource policiesResource,
      @Value("${systemUpdate.ingestPolicies.enabled}") final boolean enabled) {
    return new IngestPolicies(
        entityService, entitySearchService, searchDocumentTransformer, policiesResource, enabled);
  }
}
