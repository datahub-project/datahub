package com.linkedin.gms.factory.async;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.time.Clock;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the reliable hard delete. Scanned with its package, by GMS through {@code
 * CommonApplicationConfig}.
 */
@Configuration
public class DeleteEntityOperationFactory {

  /** The kill switch shared by every hard-delete entry point. */
  @Bean(name = "reliableHardDelete")
  @Nonnull
  protected ReliableHardDelete reliableHardDelete(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("deleteEntityService") final DeleteEntityService deleteEntityService,
      @Qualifier("timeseriesAspectService") final TimeseriesAspectService timeseriesAspectService,
      @Qualifier("graphService") final GraphService graphService,
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider) {
    return new ReliableHardDelete(
        entityService,
        deleteEntityService,
        timeseriesAspectService,
        graphService,
        Clock.systemUTC(),
        configurationProvider.getFeatureFlags().isReliableHardDelete());
  }
}
