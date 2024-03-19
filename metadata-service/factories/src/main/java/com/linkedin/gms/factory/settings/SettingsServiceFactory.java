package com.linkedin.gms.factory.settings;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.SettingsService;
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
public class SettingsServiceFactory {
  @Bean(name = "settingsService")
  @Scope("singleton")
  @Nonnull
  protected SettingsService getInstance(final SystemEntityClient entityClient, @Qualifier("openApiClient") final OpenApiClient openApiClient) throws Exception {
    return new SettingsService(entityClient, entityClient.getSystemAuthentication(), openApiClient);
  }
}
