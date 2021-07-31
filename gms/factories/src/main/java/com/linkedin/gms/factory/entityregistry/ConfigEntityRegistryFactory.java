package com.linkedin.gms.factory.entityregistry;

import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.io.FileNotFoundException;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ConfigEntityRegistryFactory {

  @Value("${ENTITY_REGISTRY_CONFIG_PATH:../../metadata-models/src/main/resources/entity-registry.yml}")
  private String entityRegistryConfigPath;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance() throws FileNotFoundException {
    return new ConfigEntityRegistry(entityRegistryConfigPath);
  }
}
