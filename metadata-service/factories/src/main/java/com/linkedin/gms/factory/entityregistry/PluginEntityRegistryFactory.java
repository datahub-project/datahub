package com.linkedin.gms.factory.entityregistry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.EntityRegistryPluginConfiguration;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
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
  protected PluginEntityRegistryLoader getInstance(ConfigurationProvider configurationProvider)
      throws FileNotFoundException, MalformedURLException {
    EntityRegistryPluginConfiguration pluginConfiguration =
        configurationProvider.getDatahub().getPlugin().getEntityRegistry();
    return new PluginEntityRegistryLoader(
        pluginConfiguration.getPath(), pluginConfiguration.getLoadDelaySeconds());
  }
}
