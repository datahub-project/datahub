package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.service.UpdateIndicesService;
import javax.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The one place that says whether this deployment can stream primary storage into a search index.
 * Both system-update phases key off the bean's presence: Phase 1 refuses to plan fills nothing
 * could run, and the non-blocking phase adds the fill step only when it is here.
 *
 * <p>The loader reads through the deployment's aspect DAO, so its primary-storage access is routed
 * exactly as every other read in the process rather than through a private connection.
 */
@Configuration
public class AspectIndexLoaderConfig {

  @Bean(name = "aspectIndexLoader")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public AspectIndexLoader aspectIndexLoader(
      final AspectDao aspectDao, final UpdateIndicesService updateIndicesService) {
    return new AspectIndexLoader(aspectDao, updateIndicesService);
  }
}
