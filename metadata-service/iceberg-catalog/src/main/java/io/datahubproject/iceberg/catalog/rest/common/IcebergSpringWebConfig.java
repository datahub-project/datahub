package io.datahubproject.iceberg.catalog.rest.common;

import io.datahubproject.iceberg.catalog.credentials.CachingCredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.iceberg.catalog.credentials.S3CredentialProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.*;

@Configuration
public class IcebergSpringWebConfig {

  @Bean
  public CredentialProvider credentialProvider() {
    return new S3CredentialProvider();
  }

  @Bean
  public CredentialProvider cachingCredentialProvider(CredentialProvider credentialProvider) {
    return new CachingCredentialProvider(credentialProvider);
  }
}
