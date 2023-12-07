package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.Database;
import javax.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityAspectMigrationsDaoFactory {

  @Bean(name = "entityAspectMigrationsDao")
  @DependsOn({"gmsEbeanServiceConfig"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectMigrationsDao createEbeanInstance(Database server) {
    return new EbeanAspectDao(server);
  }

  @Bean(name = "entityAspectMigrationsDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectMigrationsDao createCassandraInstance(CqlSession session) {
    return new CassandraAspectDao(session);
  }
}
