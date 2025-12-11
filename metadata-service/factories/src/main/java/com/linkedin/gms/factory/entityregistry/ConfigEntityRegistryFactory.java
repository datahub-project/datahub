/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
public class ConfigEntityRegistryFactory {

  @Autowired private ApplicationContext applicationContext;

  @Value("${configEntityRegistry.path}")
  private String entityRegistryConfigPath;

  @Value("${configEntityRegistry.resource}")
  Resource entityRegistryResource;

  @Bean(name = "configEntityRegistry")
  @Nonnull
  protected ConfigEntityRegistry getInstance() throws IOException, EntityRegistryException {
    BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider =
        (config, loaders) -> new SpringPluginFactory(applicationContext, config, loaders);
    if (entityRegistryConfigPath != null) {
      return new ConfigEntityRegistry(entityRegistryConfigPath, pluginFactoryProvider);
    } else {
      return new ConfigEntityRegistry(
          entityRegistryResource.getInputStream(), pluginFactoryProvider);
    }
  }
}
