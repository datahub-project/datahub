package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import io.ebean.EbeanServer;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RetentionServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("ebeanServer")
  private EbeanServer _server;

  @Value("${RETENTION_APPLICATION_BATCH_SIZE:1000}")
  private Integer _batchSize;


  @Bean(name = "retentionService")
  @DependsOn({"ebeanServer", "entityService"})
  @Nonnull
  protected RetentionService createInstance() {
    RetentionService retentionService = new EbeanRetentionService(_entityService, _server, _batchSize);
    _entityService.setRetentionService(retentionService);
    return retentionService;
  }
}
