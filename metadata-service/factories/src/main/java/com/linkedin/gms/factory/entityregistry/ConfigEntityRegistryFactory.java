package com.linkedin.gms.factory.entityregistry;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import java.io.IOException;
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

//  @Value("${configEntityRegistry.classpath}")
//  @Value("${ENTITY_REGISTRY_CLASS_PATH:../../metadata-custom-models/build/libs/}")
//  private String entityRegistryClassPath;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance() throws IOException, EntityRegistryException {
    return new ConfigEntityRegistry(entityRegistryConfigPath);
  }
}
