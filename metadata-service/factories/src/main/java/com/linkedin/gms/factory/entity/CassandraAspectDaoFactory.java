package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class CassandraAspectDaoFactory {


  @Bean(name = "cassandraAspectDao")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @DependsOn({"cassandraSession"})
  @Nonnull
  protected CassandraAspectDao createInstance(CqlSession session) {
    return new CassandraAspectDao(session);
  }
}
