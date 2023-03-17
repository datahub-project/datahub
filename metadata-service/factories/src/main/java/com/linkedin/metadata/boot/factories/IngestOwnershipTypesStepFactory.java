package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.boot.steps.IngestOwnershipTypesStep;
import com.linkedin.metadata.entity.EntityService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@Import({EntityServiceFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IngestOwnershipTypesStepFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Value("${bootstrap.ingestDefaultOwnershipTypes.enabled}")
  private Boolean _enableOwnershipTypeBootstrap;

  @Bean(name = "ingestMetadataTestsStep")
  @Scope("singleton")
  @Nonnull
  protected IngestOwnershipTypesStep createInstance() {
    return new IngestOwnershipTypesStep(_entityService, _entityService.getEntityRegistry(), _enableOwnershipTypeBootstrap);
  }
}
