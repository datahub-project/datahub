package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.datastax.DatastaxRetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import io.ebean.EbeanServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.Nonnull;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RetentionServiceFactory {

  @Autowired
  ApplicationContext applicationContext;

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Value("${RETENTION_APPLICATION_BATCH_SIZE:1000}")
  private Integer _batchSize;


  @Bean(name = "retentionService")
  @DependsOn({"datastaxSession", "entityService"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "datastax")
  @Nonnull
  protected RetentionService createDatastaxInstance() {
    CqlSession cqlSession = applicationContext.getBean(CqlSession.class);
    RetentionService retentionService = new DatastaxRetentionService(_entityService, cqlSession, _batchSize);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }


  @Bean(name = "retentionService")
  @DependsOn({"ebeanServer", "entityService"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected RetentionService createEbeanInstance() {
    EbeanServer server = applicationContext.getBean("ebeanServer", EbeanServer.class);
    RetentionService retentionService = new EbeanRetentionService(_entityService, server, _batchSize);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }
}
