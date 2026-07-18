package com.linkedin.gms.factory.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.health.PrimaryDatastoreHealthProbe;
import io.ebean.Database;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PrimaryDatastoreHealthConfiguration {

  @Bean
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  PrimaryDatastoreHealthProbe ebeanPrimaryDatastoreHealthProbe(
      @Qualifier("ebeanServer") Database database) {
    return new EbeanPrimaryDatastoreHealthProbe(database);
  }

  @Bean
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  PrimaryDatastoreHealthProbe cassandraPrimaryDatastoreHealthProbe(
      @Qualifier("cassandraSession") CqlSession cassandraSession) {
    return new CassandraPrimaryDatastoreHealthProbe(cassandraSession);
  }
}
