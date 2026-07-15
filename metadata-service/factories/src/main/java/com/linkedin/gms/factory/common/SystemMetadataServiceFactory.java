package com.linkedin.gms.factory.common;

import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({
  ElasticSearchSystemMetadataServiceFactory.class,
  PostgresSystemMetadataServiceFactory.class
})
public class SystemMetadataServiceFactory {

  @Nonnull
  @Bean(name = "systemMetadataService")
  @Primary
  protected SystemMetadataService createSystemMetadataService(
      @Autowired(required = false) @Qualifier("elasticSearchSystemMetadataService")
          ElasticSearchSystemMetadataService elasticSearchSystemMetadataService,
      @Autowired(required = false) @Qualifier("postgresSystemMetadataService")
          PostgresSystemMetadataService postgresSystemMetadataService) {

    if (elasticSearchSystemMetadataService != null) {
      return elasticSearchSystemMetadataService;
    }
    if (postgresSystemMetadataService != null) {
      return postgresSystemMetadataService;
    }
    throw new IllegalStateException(
        "No SystemMetadataService bean is available; check elasticsearch.enabled and "
            + "PostgreSQL system metadata wiring.");
  }
}
