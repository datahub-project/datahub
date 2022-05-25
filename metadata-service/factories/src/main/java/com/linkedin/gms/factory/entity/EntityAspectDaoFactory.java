package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.EbeanServer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class EntityAspectDaoFactory {

  @Bean(name = "entityAspectDao")
  @DependsOn({"gmsEbeanServiceConfig"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected AspectDao createEbeanInstance(EbeanServer server) {
    return new EbeanAspectDao(server);
  }

  @Bean(name = "entityAspectDao")
  @DependsOn({"cassandraSession"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected AspectDao createCassandraInstance(CqlSession session) {
    return new CassandraAspectDao(session);
  }
}
