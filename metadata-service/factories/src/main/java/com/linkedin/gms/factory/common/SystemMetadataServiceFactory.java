package com.linkedin.gms.factory.common;

import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ElasticSearchSystemMetadataServiceFactory.class})
public class SystemMetadataServiceFactory {
  @Autowired
  @Qualifier("elasticSearchSystemMetadataService")
  private ElasticSearchSystemMetadataService _elasticSearchSystemMetadataService;

  @Nonnull
  @DependsOn({"elasticSearchSystemMetadataService"})
  @Bean(name = "systemMetadataService")
  @Primary
  protected SystemMetadataService createInstance() {
    return _elasticSearchSystemMetadataService;
  }
}
