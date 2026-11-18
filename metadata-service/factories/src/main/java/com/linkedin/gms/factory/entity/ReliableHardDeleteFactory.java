package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.HardDeleteService;
import com.linkedin.metadata.service.HardDeleteDispatcher;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the reliable hard delete. In this package because every app that hosts the Rest.li
 * {@code EntityResource} (GMS and the standalone MCE consumer) scans it, and the resource requires
 * the bean.
 */
@Slf4j
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

  /**
   * The hard deletes every entry point runs. Where they run is decided only by whether a {@link
   * HardDeleteDispatcher} bean exists; {@code reliableHardDelete} only picks bounded or unbounded,
   * wherever they run.
   */
  @Bean(name = "hardDeleteService")
  @Nonnull
  protected HardDeleteService hardDeleteService(
      @Qualifier("entityService") final EntityService<?> entityService,
      @Qualifier("deleteEntityService") final DeleteEntityService deleteEntityService,
      @Qualifier("timeseriesAspectService") final TimeseriesAspectService timeseriesAspectService,
      @Qualifier("reliableHardDelete") final ReliableHardDelete reliableHardDelete,
      final ObjectProvider<HardDeleteDispatcher> dispatcher) {
    final HardDeleteDispatcher resolvedDispatcher = dispatcher.getIfAvailable();
    if (resolvedDispatcher != null) {
      log.info(
          "Hard deletes are offered to {} first, else run in-process; reliableHardDelete={}",
          resolvedDispatcher.getClass().getSimpleName(),
          reliableHardDelete.isEnabled());
    } else {
      log.info(
          "No HardDeleteDispatcher bean available; hard deletes run in-process;"
              + " reliableHardDelete={}",
          reliableHardDelete.isEnabled());
    }
    return new HardDeleteService(
        entityService,
        deleteEntityService,
        timeseriesAspectService,
        reliableHardDelete,
        resolvedDispatcher);
  }
}
