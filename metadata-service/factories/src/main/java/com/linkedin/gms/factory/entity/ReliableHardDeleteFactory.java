package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the reliable hard delete. In this package because every app that hosts the Rest.li
 * {@code EntityResource} (GMS and the standalone MCE consumer) scans it, and the resource requires
 * the bean.
 */
@Configuration
public class ReliableHardDeleteFactory {

  /** The kill switch shared by every hard-delete entry point. */
  @Bean(name = "reliableHardDelete")
  @Nonnull
  protected ReliableHardDelete reliableHardDelete(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider) {
    return new ReliableHardDelete(
        entityService, configurationProvider.getFeatureFlags().isReliableHardDelete());
  }
}
