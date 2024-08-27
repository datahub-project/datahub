package com.linkedin.gms.factory.entityregistry;

import com.datahub.plugins.metadata.aspect.SpringPluginFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.config.EntityRegistryPluginConfiguration;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PluginEntityRegistryFactory {

  @Value("${datahub.plugin.entityRegistry.path}")
  private String pluginRegistryPath;

  @Bean(name = "pluginEntityRegistry")
  @Nonnull
  protected PluginEntityRegistryLoader getInstance(
      @Nonnull final ConfigurationProvider configurationProvider)
      throws FileNotFoundException, MalformedURLException {
    EntityRegistryPluginConfiguration pluginConfiguration =
        configurationProvider.getDatahub().getPlugin().getEntityRegistry();
    BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider =
        (config, loaders) -> new SpringPluginFactory(null, config, loaders);
    return new PluginEntityRegistryLoader(
        pluginConfiguration.getPath(),
        pluginConfiguration.getLoadDelaySeconds(),
        pluginFactoryProvider);
  }
}
