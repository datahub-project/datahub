package com.linkedin.gms.factory.entityregistry;

import com.datahub.plugins.metadata.aspect.SpringPluginFactory;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
public class ConfigEntityRegistryFactory {

  @Value("${configEntityRegistry.path}")
  private String entityRegistryConfigPath;

  @Value("${configEntityRegistry.resource}")
  Resource entityRegistryResource;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance(final ApplicationContext springContext)
      throws IOException, EntityRegistryException {
    BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactory =
        (config, loaders) -> new SpringPluginFactory(springContext, config, loaders);
    if (entityRegistryConfigPath != null) {
      return new ConfigEntityRegistry(entityRegistryConfigPath, pluginFactory);
    } else {
      return new ConfigEntityRegistry(entityRegistryResource.getInputStream(), pluginFactory);
    }
  }
}
