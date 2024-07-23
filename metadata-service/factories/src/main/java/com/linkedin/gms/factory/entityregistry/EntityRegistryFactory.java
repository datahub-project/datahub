package com.linkedin.gms.factory.entityregistry;

import com.datahub.plugins.metadata.aspect.SpringPluginFactory;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import java.util.List;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ConfigEntityRegistryFactory.class, PluginEntityRegistryFactory.class})
public class EntityRegistryFactory {

  @Autowired
  @Qualifier("configEntityRegistry")
  private ConfigEntityRegistry configEntityRegistry;

  @Autowired
  @Qualifier("pluginEntityRegistry")
  private PluginEntityRegistryLoader pluginEntityRegistryLoader;

  @Autowired private ApplicationContext applicationContext;

  @SneakyThrows
  @Bean("entityRegistry")
  @Primary
  @Nonnull
  protected EntityRegistry getInstance(
      SpringStandardPluginConfiguration springStandardPluginConfiguration)
      throws EntityRegistryException {
    BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider =
        (config, loaders) -> new SpringPluginFactory(applicationContext, config, loaders);
    MergedEntityRegistry baseEntityRegistry =
        new MergedEntityRegistry(new SnapshotEntityRegistry(pluginFactoryProvider))
            .apply(configEntityRegistry);
    pluginEntityRegistryLoader.withBaseRegistry(baseEntityRegistry).start(true);
    return baseEntityRegistry;
  }
}
