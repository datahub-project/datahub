package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.cassandra.CassandraRetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.Nonnull;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RetentionServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Value("${RETENTION_APPLICATION_BATCH_SIZE:1000}")
  private Integer _batchSize;


  @Bean(name = "retentionService")
  @DependsOn({"cassandraSession", "entityService"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected RetentionService createCassandraInstance(CqlSession session, EntityRegistry entityRegistry) {
    RetentionService retentionService = new CassandraRetentionService(_entityService, entityRegistry, session, _batchSize);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }


  @Bean(name = "retentionService")
  @DependsOn({"ebeanPrimaryServer", "entityService"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected RetentionService createEbeanInstance(Database server, EntityRegistry entityRegistry) {
    RetentionService retentionService = new EbeanRetentionService(_entityService, entityRegistry, server, _batchSize);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }
}
