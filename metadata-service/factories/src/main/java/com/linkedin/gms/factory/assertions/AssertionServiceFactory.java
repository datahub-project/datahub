package com.linkedin.gms.factory.assertions;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class AssertionServiceFactory {
  @Bean(name = "assertionService")
  @Scope("singleton")
  @Nonnull
  protected AssertionService getInstance(
      final SystemEntityClient systemEntityClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient)
      throws Exception {
    return new AssertionService(
        systemEntityClient, systemEntityClient.getSystemAuthentication(), openApiClient);
  }
}
