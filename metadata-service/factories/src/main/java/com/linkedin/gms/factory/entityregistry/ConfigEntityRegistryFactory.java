package com.linkedin.gms.factory.entityregistry;

import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.Resource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ConfigEntityRegistryFactory {

  @Value("${configEntityRegistry.path}")
  private String entityRegistryConfigPath;

  @Value("${configEntityRegistry.resource}")
  Resource entityRegistryResource;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance() throws IOException, EntityRegistryException {
    if (entityRegistryConfigPath != null) {
      return new ConfigEntityRegistry(entityRegistryConfigPath);
    } else {
      return new ConfigEntityRegistry(entityRegistryResource.getInputStream());
    }
  }
}
