package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.SystemAspectValidator;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class EntityAspectDaoFactory {

  @Autowired(required = false)
  private List<SystemAspectValidator> payloadValidators;

  @Bean(name = "entityAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    List<SystemAspectValidator> validators =
        Objects.requireNonNullElse(payloadValidators, List.of());
    log.debug(
        "Creating EntityAspectDao with {} SystemAspectValidators: {}",
        validators.size(),
        validators.stream().map(v -> v.getClass().getSimpleName()).toList());
    EbeanAspectDao ebeanAspectDao =
        new EbeanAspectDao(
            server,
            configurationProvider.getEbean(),
            metricUtils,
            validators,
            configurationProvider.getDatahub().getValidation() != null
                ? configurationProvider.getDatahub().getValidation().getAspectSize()
                : null);
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
      CqlSession session, final ConfigurationProvider configurationProvider) {
    List<SystemAspectValidator> validators =
        Objects.requireNonNullElse(payloadValidators, List.of());
    CassandraAspectDao cassandraAspectDao =
        new CassandraAspectDao(
            session,
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
