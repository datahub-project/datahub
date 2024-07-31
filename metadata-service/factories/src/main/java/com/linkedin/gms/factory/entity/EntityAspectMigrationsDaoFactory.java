package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityAspectMigrationsDaoFactory {

  @Bean(name = "entityAspectMigrationsDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectMigrationsDao createEbeanInstance(
      @Qualifier("ebeanServer") final Database server,
      final ConfigurationProvider configurationProvider) {
    return new EbeanAspectDao(server, configurationProvider.getEbean());
  }

  @Bean(name = "entityAspectMigrationsDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectMigrationsDao createCassandraInstance(CqlSession session) {
    return new CassandraAspectDao(session);
  }
}
