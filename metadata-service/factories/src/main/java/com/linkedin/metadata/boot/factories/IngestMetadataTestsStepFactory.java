package com.linkedin.metadata.boot.factories;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.boot.steps.IngestMetadataTestsStep;
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
public class IngestMetadataTestsStepFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Value("${metadataTests.bootstrap.enabled}")
  private Boolean _enableMetadataTestsBoostrap;

  @Bean(name = "ingestMetadataTestsStep")
  @Scope("singleton")
  @Nonnull
  protected IngestMetadataTestsStep createInstance() {
    return new IngestMetadataTestsStep(_entityService, _enableMetadataTestsBoostrap);
  }
}