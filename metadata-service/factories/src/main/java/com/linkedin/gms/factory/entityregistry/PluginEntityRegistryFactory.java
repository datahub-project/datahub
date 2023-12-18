package com.linkedin.gms.factory.entityregistry;

import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class PluginEntityRegistryFactory {

  @Value("${datahub.plugin.entityRegistry.path}")
  private String pluginRegistryPath;

  @Bean(name = "pluginEntityRegistry")
  @Nonnull
  protected PluginEntityRegistryLoader getInstance()
      throws FileNotFoundException, MalformedURLException {
    return new PluginEntityRegistryLoader(pluginRegistryPath);
  }
}
