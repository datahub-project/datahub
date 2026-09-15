package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.SystemAspectValidator;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.AspectTableResolver;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.ScopedTransactionFactory;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class EntityAspectDaoFactory {

  @Autowired(required = false)
  private List<SystemAspectValidator> systemAspectValidators;

  /**
   * OSS default table resolver: always the unqualified {@code metadata_aspect_v2} table. An
   * extension module may override with a {@code @Primary} bean to qualify the table per-request
   * (e.g. against a different underlying database).
   */
  @Bean
  @ConditionalOnMissingBean(AspectTableResolver.class)
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectTableResolver aspectTableResolver() {
    return new PlainAspectTableResolver();
  }

  /**
   * OSS default transaction factory: pass-through to the primary Ebean {@code Database}. An
   * extension module may override with a {@code @Primary} bean to route transactions/queries to a
   * different underlying database.
   */
  @Bean
  @ConditionalOnMissingBean(ScopedTransactionFactory.class)
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected ScopedTransactionFactory scopedTransactionFactory(
      final PrimaryStorageResolver primaryStorageResolver) {
    return new PassThroughScopedTransactionFactory(primaryStorageResolver.resolveEbeanPrimary());
  }

  @Bean(name = "entityAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(
      final PrimaryStorageResolver primaryStorageResolver,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils,
      final AspectTableResolver aspectTableResolver,
      final ScopedTransactionFactory scopedTransactionFactory) {
    List<SystemAspectValidator> validators =
        Objects.requireNonNullElse(systemAspectValidators, List.of());
    log.debug(
        "Creating EntityAspectDao with {} SystemAspectValidators: {}",
        validators.size(),
        validators.stream().map(v -> v.getClass().getSimpleName()).toList());
    EbeanAspectDao ebeanAspectDao =
        new EbeanAspectDao(
            primaryStorageResolver,
            configurationProvider.getEbean(),
            metricUtils,
            validators,
            configurationProvider.getDatahub().getValidation() != null
                ? configurationProvider.getDatahub().getValidation().getAspectSize()
                : null,
            aspectTableResolver,
            scopedTransactionFactory,
            configurationProvider.getEbean().isOptimisticLockingEnabled());
    if (configurationProvider.getDatahub().isReadOnly()) {
      ebeanAspectDao.setWritable(false);
    }
    return ebeanAspectDao;
  }

  @Bean(name = "entityAspectDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectDao createCassandraInstance(
      PrimaryStorageResolver primaryStorageResolver,
      final ConfigurationProvider configurationProvider) {
    if (configurationProvider.getEbean().isOptimisticLockingEnabled()) {
      log.warn(
          "OPTIMISTIC_LOCKING_ENABLED is true, but entityService.impl=cassandra does not implement "
              + "optimistic locking; the flag is ignored.");
    }
    List<SystemAspectValidator> validators =
        Objects.requireNonNullElse(systemAspectValidators, List.of());
    CassandraAspectDao cassandraAspectDao =
        new CassandraAspectDao(
            primaryStorageResolver,
            validators,
            configurationProvider.getDatahub().getValidation() != null
                ? configurationProvider.getDatahub().getValidation().getAspectSize()
                : null);
    if (configurationProvider.getDatahub().isReadOnly()) {
      cassandraAspectDao.setWritable(false);
    }
    return cassandraAspectDao;
  }
}
