package com.linkedin.gms.factory.entityregistry;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.io.FileNotFoundException;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ConfigEntityRegistryFactory {

  @Value("${configEntityRegistry.path}")
  private String entityRegistryConfigPath;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance() throws FileNotFoundException {
    return new ConfigEntityRegistry(entityRegistryConfigPath);
  }
}
