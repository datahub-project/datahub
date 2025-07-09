package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityAspectDaoFactory {

  @Bean(name = "entityAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    return new EbeanAspectDao(server, configurationProvider.getEbean(), metricUtils);
  }

  @Bean(name = "entityAspectDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectDao createCassandraInstance(CqlSession session) {
    return new CassandraAspectDao(session);
  }
}
