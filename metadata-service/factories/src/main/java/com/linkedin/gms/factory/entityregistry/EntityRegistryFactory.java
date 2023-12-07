package com.linkedin.gms.factory.entityregistry;

import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

  @SneakyThrows
  @Bean("entityRegistry")
  @Primary
  @Nonnull
  protected EntityRegistry getInstance() throws EntityRegistryException {
    MergedEntityRegistry baseEntityRegistry =
        new MergedEntityRegistry(SnapshotEntityRegistry.getInstance()).apply(configEntityRegistry);
    pluginEntityRegistryLoader.withBaseRegistry(baseEntityRegistry).start(true);
    return baseEntityRegistry;
  }
}
